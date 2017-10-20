
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

bool LockMap(std::shared_ptr<pqxx::work> work, const std::string &prefix, const std::string &accessMode, std::string &errStr)
{
	try
	{
		//It is important resources are locked in a consistent order to avoid deadlock
		work->exec("LOCK TABLE "+prefix+ "oldnodes IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "oldways IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "oldrelations IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "livenodes IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "liveways IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "liverelations IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "way_mems IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "relation_mems_n IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "relation_mems_w IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "relation_mems_r IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "nextids IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "changesets IN "+accessMode+" MODE;");
		work->exec("LOCK TABLE "+prefix+ "meta IN "+accessMode+" MODE;");
	}
	catch (const pqxx::sql_error &e)
	{
		stringstream ss;
		ss << e.what() << " (" << e.query() << ")";
		errStr = ss.str();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}
	return true;
}

// **********************************************

PgMapQuery::PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn,
		shared_ptr<pqxx::connection> &db,
		std::shared_ptr<pqxx::work> mapQueryWork)
{
	mapQueryActive = false;
	dbconn = db;
	this->mapQueryWork = mapQueryWork;

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

	this->mapQueryEnc = enc;
	this->retainNodeIds.reset(new class DataStreamRetainIds(*enc.get()));
	this->retainWayIds.reset(new class DataStreamRetainIds(this->nullEncoder));
	this->retainWayMemIds.reset(new class DataStreamRetainMemIds(*this->retainWayIds));
	this->retainRelationIds.reset(new class DataStreamRetainIds(*this->mapQueryEnc));

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
		cout << "Found " << retainNodeIds->nodeIds.size() << " static+active nodes in bbox" << endl;

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 5)
	{
		//Get way objects that reference these nodes
		//Keep the way objects in memory until we have finished encoding nodes
		GetLiveWaysThatContainNodes(this->mapQueryWork.get(), this->tableStaticPrefix, this->tableActivePrefix, retainNodeIds->nodeIds, retainWayMemIds);

		GetLiveWaysThatContainNodes(this->mapQueryWork.get(), this->tableActivePrefix, "", retainNodeIds->nodeIds, retainWayMemIds);
		cout << "Found " << this->retainWayIds->wayIds.size() << " ways depend on " << retainWayMemIds->nodeIds.size() << " nodes" << endl;

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
			GetLiveNodesById(this->mapQueryWork.get(), this->tableStaticPrefix, this->tableActivePrefix, this->extraNodes, 
				this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 7)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveNodesById(this->mapQueryWork.get(), this->tableActivePrefix, "", this->extraNodes, 
				this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

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
			GetLiveWaysById(this->mapQueryWork.get(), this->tableStaticPrefix, this->tableActivePrefix, 
				this->retainWayIds->wayIds, this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->setIterator = this->retainWayIds->wayIds.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 9)
	{		
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveWaysById(this->mapQueryWork.get(), this->tableActivePrefix, "",
				this->retainWayIds->wayIds, this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

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
				this->tableActivePrefix, 
				'n', retainNodeIds->nodeIds, this->setIterator, 1000, empty, retainRelationIds);
			return 0;
		}
		this->setIterator = this->retainNodeIds->nodeIds.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 11)
	{
		if(this->setIterator != retainNodeIds->nodeIds.end())
		{
			set<int64_t> empty;
			GetLiveRelationsForObjects(this->mapQueryWork.get(),
				this->tableActivePrefix, "",
				'n', retainNodeIds->nodeIds, this->setIterator, 1000, empty, retainRelationIds);
			return 0;
		}

		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 12)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveRelationsForObjects(this->mapQueryWork.get(), this->tableStaticPrefix, 
				this->tableActivePrefix, 
				'n', this->extraNodes, this->setIterator, 1000, retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 13)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveRelationsForObjects(this->mapQueryWork.get(),
				this->tableActivePrefix, "",
				'n', this->extraNodes, this->setIterator, 1000, retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		this->extraNodes.clear();

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
				this->tableActivePrefix, 
				'w', this->retainWayIds->wayIds, this->setIterator, 1000, 
				retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		this->setIterator = this->retainWayIds->wayIds.begin();

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 15)
	{
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveRelationsForObjects(this->mapQueryWork.get(),
				this->tableActivePrefix, "",
				'w', this->retainWayIds->wayIds, this->setIterator, 1000, 
				retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		cout << "found " << retainRelationIds->relationIds.size() << " relations" << endl;

		this->mapQueryPhase ++;
		return 0;
	}

	if(this->mapQueryPhase == 16)
	{
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
	this->retainWayIds.reset();
	this->retainWayMemIds.reset();
	this->retainRelationIds.reset();
}

// **********************************************

PgTransaction::PgTransaction(shared_ptr<pqxx::connection> dbconnIn,
	const string &tableStaticPrefixIn, 
	const string &tableActivePrefixIn,
	std::shared_ptr<pqxx::work> workIn,
	const std::string &shareMode):
	work(workIn)
{
	dbconn = dbconnIn;
	tableStaticPrefix = tableStaticPrefixIn;
	tableActivePrefix = tableActivePrefixIn;
	this->shareMode = shareMode;
	string errStr;
	bool ok = LockMap(work, this->tableStaticPrefix, this->shareMode, errStr);
	if(!ok)
		throw runtime_error(errStr);
	ok = LockMap(work, this->tableActivePrefix, this->shareMode, errStr);
	if(!ok)
		throw runtime_error(errStr);
}

PgTransaction::~PgTransaction()
{
	work.reset();
}

shared_ptr<class PgMapQuery> PgTransaction::GetQueryMgr()
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	shared_ptr<class PgMapQuery> out(new class PgMapQuery(tableStaticPrefix, tableActivePrefix, this->dbconn, this->work));
	return out;
}

void PgTransaction::GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, std::shared_ptr<IDataStreamHandler> &out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(type == "node")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveNodesById(work.get(), this->tableStaticPrefix, this->tableActivePrefix, objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveNodesById(work.get(), this->tableActivePrefix, "", objectIds, 
				it, 1000, out);
	}
	else if(type == "way")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveWaysById(work.get(), this->tableStaticPrefix, this->tableActivePrefix, objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveWaysById(work.get(), this->tableActivePrefix, "", objectIds, 
				it, 1000, out);
	}
	else if(type == "relation")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveRelationsById(work.get(), this->tableStaticPrefix, this->tableActivePrefix,
				objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveRelationsById(work.get(), this->tableActivePrefix, "",
				objectIds, 
				it, 1000, out);
	}
	else
		throw invalid_argument("Known object type");

}

bool PgTransaction::StoreObjects(class OsmData &data, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	bool saveToStaticTables,
	class PgMapError &errStr)
{
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");

	string tablePrefix = this->tableActivePrefix;
	if(saveToStaticTables)
		tablePrefix = this->tableStaticPrefix;

	bool ok = ::StoreObjects(*dbconn, work.get(), tablePrefix, data, createdNodeIds, createdWayIds, createdRelationIds, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

void PgTransaction::GetWaysForNodes(const std::set<int64_t> &objectIds, 
	std::shared_ptr<IDataStreamHandler> &out)
{
	GetLiveWaysThatContainNodes(work.get(), this->tableStaticPrefix, this->tableActivePrefix, objectIds, out);

	GetLiveWaysThatContainNodes(work.get(), this->tableActivePrefix, "", objectIds, out);
}

void PgTransaction::GetRelationsForObjs(const std::string &type, const std::set<int64_t> &objectIds, 
	std::shared_ptr<IDataStreamHandler> &out)
{
	if(type.size() == 0)
		throw invalid_argument("Type string cannot be zero length");

	set<int64_t> empty;
	std::set<int64_t>::const_iterator it = objectIds.begin();
	while(it != objectIds.end())
	{
		GetLiveRelationsForObjects(work.get(), this->tableStaticPrefix, 
			this->tableActivePrefix, 
			type[0], objectIds, it, 1000, empty, out);
	}
	it = objectIds.begin();
	while(it != objectIds.end())
	{
		GetLiveRelationsForObjects(work.get(), this->tableActivePrefix, "", 
			type[0], objectIds, it, 1000, empty, out);
	}
}

bool PgTransaction::ResetActiveTables(class PgMapError &errStr)
{
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");

	bool ok = ::ResetActiveTables(*dbconn, work.get(), this->tableActivePrefix, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgTransaction::UpdateNextIds(class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;

	ClearNextIdValues(work.get(), this->tableStaticPrefix);
	ClearNextIdValues(work.get(), this->tableActivePrefix);

	//Update next node IDs
	bool ok = UpdateNextIdsOfType(*dbconn, work.get(), 
		"node", this->tableActivePrefix, this->tableStaticPrefix,
		errStrNative);
	errStr.errStr = errStrNative; if(!ok) return false;

	//Update next way IDs
	ok = UpdateNextIdsOfType(*dbconn, work.get(), 
		"way", this->tableActivePrefix, this->tableStaticPrefix,
		errStrNative);
	errStr.errStr = errStrNative; if(!ok) return false;

	//Update next relation IDs
	ok = UpdateNextIdsOfType(*dbconn, work.get(), 
		"relation", this->tableActivePrefix, this->tableStaticPrefix,
		errStrNative);
	errStr.errStr = errStrNative; if(!ok) return false;

	//Update next changeset and UIDs
	ok = ResetChangesetUidCounts(work.get(), 
		"", this->tableStaticPrefix,
		errStrNative);
	errStr.errStr = errStrNative; if(!ok) return false;

	ok = ResetChangesetUidCounts(work.get(), 
		this->tableStaticPrefix, this->tableActivePrefix, 
		errStrNative);
	errStr.errStr = errStrNative; if(!ok) return false;

	return true;
}

void PgTransaction::GetReplicateDiff(int64_t timestampStart, int64_t timestampEnd, std::shared_ptr<IDataStreamHandler> &enc)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");


	std::shared_ptr<class OsmData> osmData(new class OsmData);

	GetReplicateDiffNodes(work.get(), this->tableStaticPrefix, false, timestampStart, timestampEnd, osmData);

	GetReplicateDiffNodes(work.get(), this->tableStaticPrefix, true, timestampStart, timestampEnd, osmData);

	GetReplicateDiffNodes(work.get(), this->tableActivePrefix, false, timestampStart, timestampEnd, osmData);

	GetReplicateDiffNodes(work.get(), this->tableActivePrefix, true, timestampStart, timestampEnd, osmData);

	GetReplicateDiffWays(work.get(), this->tableStaticPrefix, false, timestampStart, timestampEnd, osmData);

	GetReplicateDiffWays(work.get(), this->tableStaticPrefix, true, timestampStart, timestampEnd, osmData);

	GetReplicateDiffWays(work.get(), this->tableActivePrefix, false, timestampStart, timestampEnd, osmData);

	GetReplicateDiffWays(work.get(), this->tableActivePrefix, true, timestampStart, timestampEnd, osmData);

	GetReplicateDiffRelations(work.get(), this->tableStaticPrefix, false, timestampStart, timestampEnd, osmData);

	GetReplicateDiffRelations(work.get(), this->tableStaticPrefix, true, timestampStart, timestampEnd, osmData);

	GetReplicateDiffRelations(work.get(), this->tableActivePrefix, false, timestampStart, timestampEnd, osmData);

	GetReplicateDiffRelations(work.get(), this->tableActivePrefix, true, timestampStart, timestampEnd, osmData);

	enc->StoreIsDiff(false);

	osmData->StreamTo(*enc.get());
	
	enc->Reset();

	enc->Finish();
}

void PgTransaction::Dump(bool order, std::shared_ptr<IDataStreamHandler> &enc)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	enc->StoreIsDiff(false);

	DumpNodes(work.get(), this->tableStaticPrefix, this->tableActivePrefix, order, enc);

	DumpNodes(work.get(), this->tableActivePrefix, "", order, enc);

	enc->Reset();

	DumpWays(work.get(), this->tableStaticPrefix, this->tableActivePrefix, order, enc);

	DumpWays(work.get(), this->tableActivePrefix, "", order, enc);

	enc->Reset();
		
	DumpRelations(work.get(), this->tableStaticPrefix, this->tableActivePrefix, order, enc);

	DumpRelations(work.get(), this->tableActivePrefix, "", order, enc);

	enc->Finish();
}

int64_t PgTransaction::GetAllocatedId(const string &type)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");

	string errStr;
	int64_t val;
	bool ok = GetAllocatedIdFromDb(*dbconn, work.get(),
		this->tableActivePrefix,
		type, errStr, val);
	if(!ok)
		throw runtime_error(errStr);
	return val;
}

void PgTransaction::Commit()
{
	//Release locks
	work->commit();
}

void PgTransaction::Abort()
{
	work->abort();
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

std::shared_ptr<class PgTransaction> PgMap::GetTransaction(const std::string &shareMode)
{
	dbconn->cancel_query();
	std::shared_ptr<pqxx::work> work(new pqxx::work(*dbconn));
	shared_ptr<class PgTransaction> out(new class PgTransaction(dbconn, tableStaticPrefix, tableActivePrefix, work, shareMode));
	return out;
}

