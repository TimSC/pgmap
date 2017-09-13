
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
	retainNodeIds = NULL;
	mapQueryEnc = NULL;
	dbconn = NULL;
	tableStaticPrefix = tableStaticPrefixIn;
	tableActivePrefix = tableActivePrefixIn;
}

PgMapQuery::~PgMapQuery()
{
	this->Abort();
}

void PgMapQuery::SetDbConn(pqxx::connection &db)
{
	dbconn = &db;
}

int PgMapQuery::Start(const vector<double> &bbox, IDataStreamHandler &enc)
{
	if(mapQueryActive)
		throw runtime_error("Query already active");
	if(dbconn == NULL)
		throw runtime_error("DB pointer not set for PgMapQuery");
	mapQueryActive = true;
	this->mapQueryPhase = 0;
	this->mapQueryBbox = bbox;
	this->mapQueryWork = new pqxx::work(*dbconn);

	assert(this->retainNodeIds == NULL);
	this->mapQueryEnc = &enc;
	this->retainNodeIds = new class DataStreamRetainIds(enc);

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
		GetLiveNodesInBbox(*this->mapQueryWork, this->tableStaticPrefix, this->mapQueryBbox, 0, *retainNodeIds); 
		cout << "Found " << retainNodeIds->nodeIds.size() << " nodes in bbox" << endl;

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 2)
	{
		//Get way objects that reference these nodes
		class OsmData wayObjects;
		DataStreamRetainIds retainWayIds(wayObjects);
		DataStreamRetainMemIds retainWayMemIds(retainWayIds);
		GetLiveWaysThatContainNodes(*this->mapQueryWork, this->tableStaticPrefix, retainNodeIds->nodeIds, retainWayMemIds);
		cout << "Ways depend on " << retainWayMemIds.nodeIds.size() << " nodes" << endl;

		//Identify extra node IDs to complete ways
		set<int64_t> extraNodes;
		std::set_difference(retainWayMemIds.nodeIds.begin(), retainWayMemIds.nodeIds.end(), 
			retainNodeIds->nodeIds.begin(), retainNodeIds->nodeIds.end(),
			std::inserter(extraNodes, extraNodes.end()));
		cout << "num extraNodes " << extraNodes.size() << endl;

		//Get node objects to complete these ways
		GetLiveNodesById(*this->mapQueryWork, this->tableStaticPrefix, extraNodes, *this->mapQueryEnc);

		//Write ways to output
		this->mapQueryEnc->Reset();
		wayObjects.StreamTo(*this->mapQueryEnc, false);

		//Get relations that reference any of the above nodes
		this->mapQueryEnc->Reset();
		set<int64_t> empty;
		DataStreamRetainIds retainRelationIds(*this->mapQueryEnc);
		GetLiveRelationsForObjects(*this->mapQueryWork, this->tableStaticPrefix, 
			'n', retainNodeIds->nodeIds, empty, retainRelationIds);
		GetLiveRelationsForObjects(*this->mapQueryWork, this->tableStaticPrefix, 
			'n', extraNodes, retainRelationIds.relationIds, retainRelationIds);

		//Get relations that reference any of the above ways
		GetLiveRelationsForObjects(*this->mapQueryWork, this->tableStaticPrefix, 
			'w', retainWayIds.wayIds, retainRelationIds.relationIds, retainRelationIds);
		cout << "found " << retainRelationIds.relationIds.size() << " relations" << endl;

		//Release database lock by finishing the transaction
		this->mapQueryWork->commit();

		this->mapQueryEnc->Finish();

		this->mapQueryPhase = 0;
		this->mapQueryActive = false;
		this->mapQueryEnc = NULL;
		this->mapQueryBbox.clear();
		if(this->retainNodeIds != NULL)
		{
			delete this->retainNodeIds;
			this->retainNodeIds = NULL;
		}
		if(this->mapQueryWork != NULL)
		{
			delete this->mapQueryWork;
			this->mapQueryWork = NULL;
		}
		return 1; // All done!
	}

	return -1;
}

void PgMapQuery::Abort()
{
	this->mapQueryPhase = 0;
	this->mapQueryActive = false;
	this->mapQueryEnc = NULL;
	this->mapQueryBbox.clear();
	if(this->retainNodeIds != NULL)
	{
		delete this->retainNodeIds;
		this->retainNodeIds = NULL;
	}
	if(this->mapQueryWork != NULL)
	{
		delete this->mapQueryWork;
		this->mapQueryWork = NULL;
	}
}

// **********************************************

PgMap::PgMap(const string &connection, const string &tableStaticPrefixIn, 
	const string &tableActivePrefixIn) : dbconn(connection),
	pgMapQuery(tableStaticPrefixIn, tableActivePrefixIn)
{
	connectionString = connection;
	this->tableStaticPrefix = tableStaticPrefixIn;
	this->tableActivePrefix = tableActivePrefixIn;
	
	this->pgMapQuery.SetDbConn(this->dbconn);
}

PgMap::~PgMap()
{
	dbconn.disconnect();
}

bool PgMap::Ready()
{
	return dbconn.is_open();
}

void PgMap::Dump(bool onlyLiveData, IDataStreamHandler &enc)
{
	enc.StoreIsDiff(false);

	//Lock database for reading (this must always be done in a set order)
	pqxx::work work(dbconn);
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relations IN ACCESS SHARE MODE;");

	work.exec("LOCK TABLE "+this->tableActivePrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "relations IN ACCESS SHARE MODE;");

	DumpNodes(work, this->tableStaticPrefix, onlyLiveData, enc);

	enc.Reset();

	DumpWays(work, this->tableStaticPrefix, onlyLiveData, enc);

	enc.Reset();
		
	DumpRelations(work, this->tableStaticPrefix, onlyLiveData, enc);

	//Release locks
	work.commit();

	enc.Finish();

}

void PgMap::GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, class IDataStreamHandler &out)
{
	pqxx::work work(dbconn);
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relations IN ACCESS SHARE MODE;");

	work.exec("LOCK TABLE "+this->tableActivePrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "relations IN ACCESS SHARE MODE;");

	if(type == "node")
		GetLiveNodesById(work, this->tableStaticPrefix, 
			objectIds, out);
	else if(type == "way")
		GetLiveWaysById(work, this->tableStaticPrefix, 
			objectIds, out);
	else if(type == "relation")
		GetLiveRelationsById(work, this->tableStaticPrefix, 
			objectIds, out);
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
	pqxx::work work(dbconn);

	work.exec("LOCK TABLE "+this->tableActivePrefix+ "nodes IN ACCESS EXCLUSIVE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "ways IN ACCESS EXCLUSIVE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "relations IN ACCESS EXCLUSIVE MODE;");
	work.exec("LOCK TABLE "+this->tableActivePrefix+ "nextids IN ACCESS EXCLUSIVE MODE;");

	bool ok = ::StoreObjects(dbconn, work, this->tableActivePrefix, data, createdNodeIds, createdWayIds, createdRelationIds, nativeErrStr);
	errStr.errStr = nativeErrStr;

	if(ok)
		work.commit();
	return ok;
}

