
#include "pgmap.h"
#include "db.h"
#include "util.h"
#include "cppo5m/OsmData.h"
#include <algorithm>
using namespace std;

PgMapError::PgMapError()
{

}

PgMapError::PgMapError(const string &connection)
{
	this->errStr = connection;
}

PgMapError::PgMapError(const PgMapError &obj)
{
	this->errStr = obj.errStr;
}

PgMapError::~PgMapError()
{

}

// **********************************************

PgMapQuery::PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn)
{
	mapQueryActive = false;
	mapQueryEnc = NULL;
	dbconn = NULL;
	mapQueryWork = NULL;

	tableStaticPrefix = tableStaticPrefixIn;
	tableActivePrefix = tableActivePrefixIn;
}

PgMapQuery::~PgMapQuery()
{
	this->Reset();
}

PgMapQuery& PgMapQuery::operator=(const PgMapQuery&)
{
	return *this;
}

void PgMapQuery::SetDbConn(shared_ptr<pqxx::connection> db)
{
	dbconn = db;
}

int PgMapQuery::Start(const vector<double> &bbox, std::shared_ptr<IDataStreamHandler> &enc)
{
	if(mapQueryActive)
		throw runtime_error("Query already active");
	if(dbconn.get() == NULL)
		throw runtime_error("DB pointer not set for PgMapQuery");
	mapQueryActive = true;
	this->mapQueryPhase = 0;
	this->mapQueryBbox = bbox;
	this->mapQueryWork.reset(new pqxx::work(*dbconn));

	this->mapQueryEnc = enc;
	this->retainNodeIds.reset(new class DataStreamRetainIds(*enc.get()));
	this->retainWayIds.reset(new class DataStreamRetainIds(this->nullEncoder));
	this->retainWayMemIds.reset(new class DataStreamRetainMemIds(*this->retainWayIds));

	//Lock database for reading (this must always be done in a set order)
	this->mapQueryWork->exec("LOCK TABLE "+this->tableStaticPrefix+ "nodes IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableStaticPrefix+ "ways IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableStaticPrefix+ "relations IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableStaticPrefix+ "way_mems IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableStaticPrefix+ "relation_mems_n IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableStaticPrefix+ "relation_mems_w IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableStaticPrefix+ "relation_mems_r IN ACCESS SHARE MODE;");

	this->mapQueryWork->exec("LOCK TABLE "+this->tableActivePrefix+ "nodes IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableActivePrefix+ "ways IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableActivePrefix+ "relations IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableActivePrefix+ "way_mems IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableActivePrefix+ "relation_mems_n IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableActivePrefix+ "relation_mems_w IN ACCESS SHARE MODE;");
	this->mapQueryWork->exec("LOCK TABLE "+this->tableActivePrefix+ "relation_mems_r IN ACCESS SHARE MODE;");
	return 0;
}

int PgMapQuery::Continue()
{
	if(!mapQueryActive)
		throw runtime_error("Query not active");

	if(this->mapQueryPhase == 0)
	{
		this->mapQueryEnc->StoreIsDiff(false);
		this->mapQueryEnc->StoreBounds(this->mapQueryBbox[0], this->mapQueryBbox[1], this->mapQueryBbox[2], this->mapQueryBbox[3]);
		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 1)
	{
		//Get nodes in bbox
		cursor = LiveNodesInBboxStart(*this->mapQueryWork, this->tableStaticPrefix, this->mapQueryBbox, 0);

		this->mapQueryPhase ++;
		return 0;

	}

	if(this->mapQueryPhase == 2)
	{
		int ret = LiveNodesInBboxContinue(cursor, *retainNodeIds);
		if(ret > 0)
			return 0;
		if(ret < 0)
			return -1; 

		cursor.reset();
		cout << "Found " << retainNodeIds->nodeIds.size() << " nodes in bbox" << endl;

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 3)
	{
		//Get way objects that reference these nodes
		//Keep the way objects in memory until we have finished encoding nodes
		GetLiveWaysThatContainNodes(*this->mapQueryWork, this->tableStaticPrefix, retainNodeIds->nodeIds, *retainWayMemIds);
		cout << "Ways depend on " << retainWayMemIds->nodeIds.size() << " nodes" << endl;

		//Identify extra node IDs to complete ways
		this->extraNodes.clear();
		std::set_difference(retainWayMemIds->nodeIds.begin(), retainWayMemIds->nodeIds.end(), 
			retainNodeIds->nodeIds.begin(), retainNodeIds->nodeIds.end(),
			std::inserter(this->extraNodes, this->extraNodes.end()));
		cout << "num extraNodes " << this->extraNodes.size() << endl;

		//Get node objects to complete these ways
		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 4)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveNodesById(*this->mapQueryWork, this->tableStaticPrefix, this->extraNodes, 
				this->setIterator, 1000, *this->mapQueryEnc);
			return 0;
		}

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 5)
	{
		this->setIterator = this->retainWayIds->wayIds.begin();

		//Write ways to output
		this->mapQueryEnc->Reset();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 6)
	{		
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveWaysById(*this->mapQueryWork, this->tableStaticPrefix, 
				this->retainWayIds->wayIds, this->setIterator, 1000, *this->mapQueryEnc);
			return 0;
		}

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 7)
	{

		//Get relations that reference any of the above nodes
		this->mapQueryEnc->Reset();
		set<int64_t> empty;
		DataStreamRetainIds retainRelationIds(*this->mapQueryEnc);
		GetLiveRelationsForObjects(*this->mapQueryWork, this->tableStaticPrefix, 
			'n', retainNodeIds->nodeIds, empty, retainRelationIds);
		GetLiveRelationsForObjects(*this->mapQueryWork, this->tableStaticPrefix, 
			'n', this->extraNodes, retainRelationIds.relationIds, retainRelationIds);
		this->extraNodes.clear();

		//Get relations that reference any of the above ways
		GetLiveRelationsForObjects(*this->mapQueryWork, this->tableStaticPrefix, 
			'w', this->retainWayIds->wayIds, retainRelationIds.relationIds, retainRelationIds);
		cout << "found " << retainRelationIds.relationIds.size() << " relations" << endl;

		//Release database lock by finishing the transaction
		this->mapQueryWork->commit();

		this->mapQueryEnc->Finish();

		this->Reset();
		return 1; // All done!
	}

	return -1;
}

void PgMapQuery::Reset()
{
	this->mapQueryPhase = 0;
	this->mapQueryActive = false;
	this->mapQueryEnc.reset();
	this->mapQueryBbox.clear();
	this->retainNodeIds.reset();
	this->mapQueryWork.reset();
	this->retainWayIds.reset();
	this->retainWayMemIds.reset();
}

// **********************************************

PgMap::PgMap(const string &connection, const string &tableStaticPrefixIn, 
	const string &tableActivePrefixIn)
{
	dbconn.reset(new pqxx::connection(connection));
	connectionString = connection;
	this->tableStaticPrefix = tableStaticPrefixIn;
	this->tableActivePrefix = tableActivePrefixIn;
}

PgMap::~PgMap()
{
	dbconn->disconnect();
	dbconn.reset();
}

bool PgMap::Ready()
{
	return dbconn->is_open();
}

void PgMap::Dump(bool onlyLiveData, std::shared_ptr<IDataStreamHandler> &enc)
{
	enc->StoreIsDiff(false);

	//Lock database for reading (this must always be done in a set order)
	pqxx::work work(*dbconn);
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relations IN ACCESS SHARE MODE;");

	work.exec("LOCK TABLE "+this->tableActivePrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "relations IN ACCESS SHARE MODE;");

	DumpNodes(work, this->tableStaticPrefix, onlyLiveData, *enc.get());

	enc->Reset();

	DumpWays(work, this->tableStaticPrefix, onlyLiveData, *enc.get());

	enc->Reset();
		
	DumpRelations(work, this->tableStaticPrefix, onlyLiveData, *enc.get());

	//Release locks
	work.commit();

	enc->Finish();

}

shared_ptr<class PgMapQuery> PgMap::GetQueryMgr()
{
	shared_ptr<class PgMapQuery> out(new class PgMapQuery(tableStaticPrefix, tableActivePrefix));
	out->SetDbConn(this->dbconn);
	return out;
}

void PgMap::GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, std::shared_ptr<IDataStreamHandler> &out)
{
	pqxx::work work(*dbconn);
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relations IN ACCESS SHARE MODE;");

	work.exec("LOCK TABLE "+this->tableActivePrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "relations IN ACCESS SHARE MODE;");

	if(type == "node")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveNodesById(work, this->tableStaticPrefix, objectIds, 
				it, 1000, *out.get());
	}
	else if(type == "way")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveWaysById(work, this->tableStaticPrefix, objectIds, 
				it, 1000, *out.get());
	}
	else if(type == "relation")
		GetLiveRelationsById(work, this->tableStaticPrefix, 
			objectIds, *out.get());
	else
		throw invalid_argument("Known object type");

	//Release locks
	work.commit();
}

bool PgMap::StoreObjects(class OsmData &data, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	class PgMapError &errStr)
{
	std::string nativeErrStr;
	pqxx::work work(*dbconn);

	work.exec("LOCK TABLE "+this->tableActivePrefix+ "nodes IN ACCESS EXCLUSIVE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "ways IN ACCESS EXCLUSIVE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "relations IN ACCESS EXCLUSIVE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "nextids IN ACCESS EXCLUSIVE MODE;");

	bool ok = ::StoreObjects(*dbconn, work, this->tableActivePrefix, data, createdNodeIds, createdWayIds, createdRelationIds, nativeErrStr);
	errStr.errStr = nativeErrStr;

	if(ok)
		work.commit();
	return ok;
}

