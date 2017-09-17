
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

void LockMap(std::shared_ptr<pqxx::work> work, const std::string &prefix, const std::string &accessMode)
{
	//It is important resources are locked in a consistent order to avoid deadlock
	work->exec("LOCK TABLE "+prefix+ "oldnodes IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "oldways IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "oldrelations IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "livenodes IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "liveways IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "liverelations IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "way_mems IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "relation_mems_n IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "relation_mems_w IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "relation_mems_r IN ACCESS "+accessMode+";");
	work->exec("LOCK TABLE "+prefix+ "nextids IN ACCESS "+accessMode+";");
}

// **********************************************

PgMapQuery::PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn,
		shared_ptr<pqxx::connection> &db)
{
	mapQueryActive = false;
	dbconn = db;

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
	this->retainRelationIds.reset(new class DataStreamRetainIds(*this->mapQueryEnc));

	//Lock database for reading (this must always be done in a set order)
	LockMap(this->mapQueryWork, this->tableStaticPrefix, "SHARE MODE");
	LockMap(this->mapQueryWork, this->tableActivePrefix, "SHARE MODE");

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
		//Get nodes in bbox (static db)
		cursor = LiveNodesInBboxStart(this->mapQueryWork.get(), this->tableStaticPrefix, this->mapQueryBbox, this->tableActivePrefix);

		this->mapQueryPhase ++;
		return 0;

	}

	if(this->mapQueryPhase == 2)
	{
		int ret = LiveNodesInBboxContinue(cursor, retainNodeIds);
		if(ret > 0)
			return 0;
		if(ret < 0)
			return -1; 

		cursor.reset();
		cout << "Found " << retainNodeIds->nodeIds.size() << " static nodes in bbox" << endl;

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 3)
	{
		//Get nodes in bbox (active db)
		cursor = LiveNodesInBboxStart(this->mapQueryWork.get(), this->tableActivePrefix, this->mapQueryBbox, "");

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 4)
	{
		int ret = LiveNodesInBboxContinue(cursor, retainNodeIds);
		if(ret > 0)
			return 0;
		if(ret < 0)
			return -1; 

		cursor.reset();
		cout << "Found " << retainNodeIds->nodeIds.size() << " active nodes in bbox" << endl;

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 5)
	{
		//Get way objects that reference these nodes
		//Keep the way objects in memory until we have finished encoding nodes
		GetLiveWaysThatContainNodes(this->mapQueryWork.get(), this->tableStaticPrefix, this->tableActivePrefix, retainNodeIds->nodeIds, retainWayMemIds);
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

	if(this->mapQueryPhase == 6)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveNodesById(this->mapQueryWork.get(), this->tableStaticPrefix, this->extraNodes, 
				this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 7)
	{
		this->setIterator = this->retainWayIds->wayIds.begin();

		//Write ways to output
		this->mapQueryEnc->Reset();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 8)
	{		
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveWaysById(this->mapQueryWork.get(), this->tableStaticPrefix, 
				this->retainWayIds->wayIds, this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 9)
	{

		//Get relations that reference any of the above nodes
		this->mapQueryEnc->Reset();
		this->setIterator = this->retainNodeIds->nodeIds.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 10)
	{
		if(this->setIterator != retainNodeIds->nodeIds.end())
		{
			set<int64_t> empty;
			GetLiveRelationsForObjects(this->mapQueryWork.get(), this->tableStaticPrefix, 
				'n', retainNodeIds->nodeIds, this->setIterator, 1000, empty, retainRelationIds);
			return 0;
		}

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 11)
	{
		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 12)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveRelationsForObjects(this->mapQueryWork.get(), this->tableStaticPrefix, 
				'n', this->extraNodes, this->setIterator, 1000, retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		this->extraNodes.clear();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 13)
	{
		//Get relations that reference any of the above ways
		this->setIterator = this->retainWayIds->wayIds.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 14)
	{
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveRelationsForObjects(this->mapQueryWork.get(), this->tableStaticPrefix, 
				'w', this->retainWayIds->wayIds, this->setIterator, 1000, 
				retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}
		cout << "found " << retainRelationIds->relationIds.size() << " relations" << endl;

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
	this->retainRelationIds.reset();
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
	std::shared_ptr<pqxx::work> work(new pqxx::work(*dbconn));
	LockMap(work, this->tableStaticPrefix, "SHARED MODE");
	LockMap(work, this->tableActivePrefix, "SHARED MODE");

	DumpNodes(work.get(), this->tableStaticPrefix, onlyLiveData, enc);

	enc->Reset();

	DumpWays(work.get(), this->tableStaticPrefix, onlyLiveData, enc);

	enc->Reset();
		
	DumpRelations(work.get(), this->tableStaticPrefix, onlyLiveData, enc);

	//Release locks
	work->commit();

	enc->Finish();

}

shared_ptr<class PgMapQuery> PgMap::GetQueryMgr()
{
	shared_ptr<class PgMapQuery> out(new class PgMapQuery(tableStaticPrefix, tableActivePrefix, this->dbconn));
	return out;
}

void PgMap::GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, std::shared_ptr<IDataStreamHandler> &out)
{
	std::shared_ptr<pqxx::work> work(new pqxx::work(*dbconn));
	LockMap(work, this->tableStaticPrefix, "SHARED MODE");
	LockMap(work, this->tableActivePrefix, "SHARED MODE");

	if(type == "node")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveNodesById(work.get(), this->tableStaticPrefix, objectIds, 
				it, 1000, out);
	}
	else if(type == "way")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveWaysById(work.get(), this->tableStaticPrefix, objectIds, 
				it, 1000, out);
	}
	else if(type == "relation")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveRelationsById(work.get(), this->tableStaticPrefix, 
				objectIds, 
				it, 1000, out);
	}
	else
		throw invalid_argument("Known object type");

	//Release locks
	work->commit();
}

bool PgMap::StoreObjects(class OsmData &data, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::work> work(new pqxx::work(*dbconn));
	LockMap(work, this->tableStaticPrefix, "EXCLUSIVE MODE");
	LockMap(work, this->tableActivePrefix, "EXCLUSIVE MODE");

	bool ok = ::StoreObjects(*dbconn, work.get(), this->tableActivePrefix, data, createdNodeIds, createdWayIds, createdRelationIds, nativeErrStr);
	errStr.errStr = nativeErrStr;

	if(ok)
		work->commit();
	return ok;
}

bool PgMap::ResetActiveTables(class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::work> work(new pqxx::work(*dbconn));
	LockMap(work, this->tableStaticPrefix, "EXCLUSIVE MODE");
	LockMap(work, this->tableActivePrefix, "EXCLUSIVE MODE");

	bool ok = ::ResetActiveTables(*dbconn, work.get(), this->tableActivePrefix, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	if(ok)
		work->commit();
	return ok;
}

