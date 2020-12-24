#include "pgmap.h"
#include "dbquery.h"
#include "dbids.h"
#include "dbadmin.h"
#include "dbdecode.h"
#include "dbreplicate.h"
#include "dbstore.h"
#include "dbdump.h"
#include "dbfilters.h"
#include "dbchangeset.h"
#include "dbmeta.h"
#include "dbcommon.h"
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

bool LockMap(std::shared_ptr<pqxx::transaction_base> work, const std::string &prefix, const std::string &accessMode, std::string &errStr)
{
	try
	{
		//It is important resources are locked in a consistent order to avoid deadlock
		//Also, lock everything in one command to get a consistent view of the data.
		string sql = "LOCK TABLE "+prefix+ "oldnodes";
		sql += ","+prefix+ "oldways";
		sql += ","+prefix+ "oldrelations";
		sql += ","+prefix+ "livenodes";
		sql += ","+prefix+ "liveways";
		sql += ","+prefix+ "liverelations";

		sql += ","+prefix+ "nodeids";
		sql += ","+prefix+ "wayids";
		sql += ","+prefix+ "relationids";

		sql += ","+prefix+ "way_mems";
		sql += ","+prefix+ "relation_mems_n";
		sql += ","+prefix+ "relation_mems_w";
		sql += ","+prefix+ "relation_mems_r";
		sql += ","+prefix+ "nextids";
		sql += ","+prefix+ "changesets";
		sql += ","+prefix+ "meta";
		sql += ","+prefix+ "usernames";
		sql += " IN "+accessMode+" MODE;";

		work->exec(sql);

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

PgChangeset::PgChangeset()
{
	objId = 0;
	uid = 0;
	open_timestamp = 0;
	close_timestamp = 0;
	is_open = true; bbox_set = false;
	x1 = 0.0; y1 = 0.0; x2 = 0.0; y2 = 0.0;
}

PgChangeset::PgChangeset(const PgChangeset &obj)
{
	*this = obj;
}

PgChangeset::~PgChangeset()
{

}

PgChangeset& PgChangeset::operator=(const PgChangeset &obj)
{
	objId = obj.objId;
	uid = obj.uid;
	open_timestamp = obj.open_timestamp;
	close_timestamp = obj.close_timestamp;
	username = obj.username;
	tags = obj.tags;
	is_open = obj.is_open;
	bbox_set = obj.bbox_set;
	x1 = obj.x1; y1 = obj.y1; x2 = obj.x2; y2 = obj.y2;
	return *this;
}

// **********************************************

PgMapQuery::PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn,
		shared_ptr<pqxx::connection> &db,
		std::shared_ptr<class PgWork> sharedWorkIn,
		class DbUsernameLookup &dbUsernameLookupIn):
	sharedWork(sharedWorkIn),
	dbUsernameLookup(dbUsernameLookupIn)
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
	if(bbox.size() != 4)
		throw invalid_argument("bbox must have four values");

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

int PgMapQuery::Start(const std::string &wkt, std::shared_ptr<IDataStreamHandler> &enc)
{
	if(mapQueryActive)
		throw runtime_error("Query already active");
	if(dbconn.get() == NULL)
		throw runtime_error("DB pointer not set for PgMapQuery");
	mapQueryActive = true;
	this->mapQueryPhase = 0;
	this->mapQueryWkt = wkt;

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
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	int verbose = 1;

	if(this->mapQueryPhase == 0)
	{
		this->mapQueryEnc->StoreIsDiff(false);
		if(this->mapQueryBbox.size() == 4)
			this->mapQueryEnc->StoreBounds(this->mapQueryBbox[0], this->mapQueryBbox[1], this->mapQueryBbox[2], this->mapQueryBbox[3]);
		this->mapQueryPhase = 3;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	//Phases 1 and 2 have been simplifed out of the system

	if(this->mapQueryPhase == 3)
	{
		if(this->mapQueryBbox.size() == 4)
		{
			//Get nodes in bbox (active db)
			cursor = VisibleNodesInBboxStart(*dbconn, work.get(), 
				this->tableActivePrefix, this->mapQueryBbox, 0, "");
		}
		else
			cursor = VisibleNodesInWktStart(*dbconn, work.get(), 
				this->tableActivePrefix, this->mapQueryWkt, 4326, "");

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 4)
	{
		int ret = LiveNodesInBboxContinue(cursor, this->dbUsernameLookup, retainNodeIds);
		if(ret > 0)
			return 0;
		if(ret < 0)
			return -1; 

		cursor.reset();
		cout << "Found " << retainNodeIds->nodeIds.size() << " static+active nodes in bbox" << endl;

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 5)
	{
		//Get way objects that reference these nodes
		//Keep the way objects in memory until we have finished encoding nodes
		GetLiveWaysThatContainNodes(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableStaticPrefix, this->tableActivePrefix, retainNodeIds->nodeIds, retainWayMemIds);

		GetLiveWaysThatContainNodes(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, "", retainNodeIds->nodeIds, retainWayMemIds);
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
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 6)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveNodesById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, this->tableActivePrefix, this->extraNodes, 
				this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 7)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveNodesById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "", this->extraNodes, 
				this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->setIterator = this->retainWayIds->wayIds.begin();

		//Write ways to output
		this->mapQueryEnc->Reset();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 8)
	{		
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveWaysById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, this->tableActivePrefix, 
				this->retainWayIds->wayIds, this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->setIterator = this->retainWayIds->wayIds.begin();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 9)
	{		
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveWaysById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "",
				this->retainWayIds->wayIds, this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		//Get relations that reference any of the above nodes
		this->mapQueryEnc->Reset();
		this->setIterator = this->retainNodeIds->nodeIds.begin();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 10)
	{
		if(this->setIterator != retainNodeIds->nodeIds.end())
		{
			GetLiveRelationsForObjects(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, 
				this->tableActivePrefix, 
				'n', retainNodeIds->nodeIds, this->setIterator, 1000, retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}
		this->setIterator = this->retainNodeIds->nodeIds.begin();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 11)
	{
		if(this->setIterator != retainNodeIds->nodeIds.end())
		{
			GetLiveRelationsForObjects(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "",
				'n', retainNodeIds->nodeIds, this->setIterator, 1000, retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 12)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveRelationsForObjects(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, 
				this->tableActivePrefix, 
				'n', this->extraNodes, this->setIterator, 1000, retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 13)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetLiveRelationsForObjects(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "",
				'n', this->extraNodes, this->setIterator, 1000, retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		this->extraNodes.clear();

		//Get relations that reference any of the above ways
		this->setIterator = this->retainWayIds->wayIds.begin();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 14)
	{
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveRelationsForObjects(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, 
				this->tableActivePrefix, 
				'w', this->retainWayIds->wayIds, this->setIterator, 1000, 
				retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		this->setIterator = this->retainWayIds->wayIds.begin();

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(this->mapQueryPhase == 15)
	{
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetLiveRelationsForObjects(*dbconn, work.get(),
				this->dbUsernameLookup,
				this->tableActivePrefix, "",
				'w', this->retainWayIds->wayIds, this->setIterator, 1000, 
				retainRelationIds->relationIds, retainRelationIds);
			return 0;
		}

		cout << "found " << retainRelationIds->relationIds.size() << " relations" << endl;

		this->mapQueryPhase ++;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
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

// *********************************************


PgTransaction::PgTransaction(shared_ptr<pqxx::connection> dbconnIn,
	const string &tableStaticPrefixIn, 
	const string &tableActivePrefixIn,
	std::shared_ptr<class PgWork> sharedWorkIn,
	const std::string &shareMode):

	PgCommon(dbconnIn, tableStaticPrefixIn, tableActivePrefixIn, sharedWorkIn, shareMode)
{
	string errStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = LockMap(work, this->tableStaticPrefix, this->shareMode, errStr);
	if(!ok)
		throw runtime_error(errStr);
	ok = LockMap(work, this->tableActivePrefix, this->shareMode, errStr);
	if(!ok)
		throw runtime_error(errStr);
}

PgTransaction::~PgTransaction()
{
	this->sharedWork->work.reset();
	this->sharedWork.reset();
}

shared_ptr<class PgMapQuery> PgTransaction::GetQueryMgr()
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	shared_ptr<class PgMapQuery> out(new class PgMapQuery(tableStaticPrefix, tableActivePrefix, 
		this->dbconn, this->sharedWork, this->dbUsernameLookup));
	return out;
}

void PgTransaction::GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, 
	std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(objectIds.size()==0)
		return;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	if(type == "node")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveNodesById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, this->tableActivePrefix, objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveNodesById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "", objectIds, 
				it, 1000, out);
	}
	else if(type == "way")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveWaysById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, this->tableActivePrefix, objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveWaysById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "", objectIds, 
				it, 1000, out);
	}
	else if(type == "relation")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveRelationsById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, this->tableActivePrefix,
				objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveRelationsById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "",
				objectIds, 
				it, 1000, out);
	}
	else
		throw invalid_argument("Known object type");

}

void PgTransaction::GetFullObjectById(const std::string &type, int64_t objectId, 
	std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(type == "node")
		throw invalid_argument("Cannot get full object for nodes");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Get main object
	std::shared_ptr<class OsmData> outData(new class OsmData());
	std::set<int64_t> objectIds;
	objectIds.insert(objectId);
	this->GetObjectsById(type, objectIds, outData);

	//Get members of main object
	if(type == "way")
	{
		if (outData->ways.size() != 1)
			return; //Unexpected number of objects in intermediate result
		class OsmWay &mainWay = outData->ways[0];

		std::set<int64_t> memberNodes;
		for(int64_t i=0; i<mainWay.refs.size(); i++)
			memberNodes.insert(mainWay.refs[i]);
		this->GetObjectsById("node", memberNodes, outData);
 	}
	else if(type == "relation")
	{
		if (outData->relations.size() != 1)
			return; //Unexpected number of objects in intermediate result
		class OsmRelation &mainRelation = outData->relations[0];

		std::set<int64_t> memberNodes, memberWays, memberRelations;
		for(int64_t i=0; i<mainRelation.refIds.size(); i++)
		{
			if(mainRelation.refTypeStrs[i]=="node")
				memberNodes.insert(mainRelation.refIds[i]);
			else if(mainRelation.refTypeStrs[i]=="way")
				memberWays.insert(mainRelation.refIds[i]);
			else if(mainRelation.refTypeStrs[i]=="relation")
				memberRelations.insert(mainRelation.refIds[i]);
		}

		std::shared_ptr<class OsmData> memberWayObjs(new class OsmData());
		this->GetObjectsById("node", memberNodes, outData);
		this->GetObjectsById("way", memberWays, memberWayObjs);
		this->GetObjectsById("relation", memberRelations, outData);
		memberWayObjs->StreamTo(*outData.get());

		std::set<int64_t> memberNodes2;
		for(int64_t i=0; i<memberWayObjs->ways.size(); i++)
			for(int64_t j=0; j<memberWayObjs->ways[i].refs.size(); j++)
				memberNodes2.insert(memberWayObjs->ways[i].refs[j]);
		this->GetObjectsById("node", memberNodes2, outData);
	}
	else
		throw invalid_argument("Known object type");
	
	outData->StreamTo(*out.get());
}

void PgTransaction::GetObjectsByIdVer(const std::string &type, const std::set<std::pair<int64_t, int64_t> > &objectIdVers, 
		std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(objectIdVers.size()==0)
		return;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Query all tables that might contain object of this id and version
	std::set<std::pair<int64_t, int64_t> >::const_iterator it = objectIdVers.begin();
	while(it != objectIdVers.end())
		DbGetObjectsByIdVer(*dbconn, work.get(), this->dbUsernameLookup, 
			this->tableStaticPrefix, type, "old", objectIdVers, 
			it, 1000, out);
	it = objectIdVers.begin();
	while(it != objectIdVers.end())
		DbGetObjectsByIdVer(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableStaticPrefix, type, "live", objectIdVers, 
			it, 1000, out);
	it = objectIdVers.begin();
	while(it != objectIdVers.end())
		DbGetObjectsByIdVer(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, "old", objectIdVers, 
			it, 1000, out);
	it = objectIdVers.begin();
	while(it != objectIdVers.end())
		DbGetObjectsByIdVer(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, "live", objectIdVers, 
			it, 1000, out);
}

void PgTransaction::GetObjectsHistoryById(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(objectIds.size()==0)
		return;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Query all tables that might contain object of this id and version
	std::set<int64_t>::const_iterator it = objectIds.begin();
	while(it != objectIds.end())
		DbGetObjectsHistoryById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableStaticPrefix, type, "old", objectIds, 
			it, 1000, out);
	it = objectIds.begin();
	while(it != objectIds.end())
		DbGetObjectsHistoryById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableStaticPrefix, type, "live", objectIds, 
			it, 1000, out);
	it = objectIds.begin();
	while(it != objectIds.end())
		DbGetObjectsHistoryById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, "old", objectIds, 
			it, 1000, out);
	it = objectIds.begin();
	while(it != objectIds.end())
		DbGetObjectsHistoryById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, "live", objectIds, 
			it, 1000, out);
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
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	string tablePrefix = this->tableActivePrefix;
	if(saveToStaticTables)
		tablePrefix = this->tableStaticPrefix;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = ::StoreObjects(*dbconn, work.get(), tablePrefix, data, createdNodeIds, createdWayIds, createdRelationIds, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

int PgTransaction::UpdateObjectBboxesById(
	const std::string &objType,
	const std::set<int64_t> &objectIds, int verbose, 
	bool saveToStaticTables,
	class PgMapError &errStr)
{
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	string tablePrefix = this->tableActivePrefix;
	if(saveToStaticTables)
		tablePrefix = this->tableStaticPrefix;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	int ok = 1;
	if(objType == "way")
		ok = ::UpdateWayBboxesById(*dbconn, work.get(),
			objectIds,
			verbose,
			tablePrefix, 
			nativeErrStr);
	else if (objType == "relation")
	{
		ok = ::UpdateRelationBboxesById(*dbconn, work.get(),
			objectIds,
			verbose,
			tablePrefix, 
			nativeErrStr);
	}

	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgTransaction::ResetActiveTables(class PgMapError &errStr)
{
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = ::ResetActiveTables(*dbconn, work.get(), this->tableActivePrefix, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

void PgTransaction::GetReplicateDiff(int64_t timestampStart, int64_t timestampEnd, class OsmChange &out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	std::shared_ptr<class OsmData> osmData(new class OsmData);

	GetReplicateDiffNodes(*dbconn, work.get(), this->dbUsernameLookup, 
		this->tableStaticPrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffNodes(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffNodes(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffNodes(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffWays(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffWays(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffWays(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffWays(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffRelations(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffRelations(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffRelations(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffRelations(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, true, timestampStart, timestampEnd, out);
}

/**
* Dump live objects. Only current nodes are dumped, not old (non-visible) nodes.
*/
void PgTransaction::Dump(bool order, bool nodes, bool ways, bool relations, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	enc->StoreIsDiff(false);

	if(nodes)
	{
		DumpNodes(*dbconn, work.get(), this->dbUsernameLookup, this->tableActivePrefix, order, enc);

		enc->Reset();
	}

	if(ways)
	{
		DumpWays(*dbconn, work.get(), this->dbUsernameLookup, this->tableActivePrefix, order, enc);

		enc->Reset();
	}

	if(relations)
	{	
		DumpRelations(*dbconn, work.get(), this->dbUsernameLookup, this->tableActivePrefix, order, enc);
	}

	enc->Finish();
}

int64_t PgTransaction::GetAllocatedId(const string &type)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	class PgMapError perrStr;
	if(atoi(this->GetMetaValue("readonly", perrStr).c_str()) == 1)
	{
		perrStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	string errStr;
	int64_t val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = GetAllocatedIdFromDb(*dbconn, work.get(),
		this->tableActivePrefix,
		type, true, errStr, val);
	if(!ok)
		throw runtime_error(errStr);
	return val;
}

int64_t PgTransaction::PeekNextAllocatedId(const string &type)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	string errStr;
	int64_t val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = GetAllocatedIdFromDb(*dbconn, work.get(),
		this->tableActivePrefix,
		type, false, errStr, val);
	if(!ok)
		throw runtime_error(errStr);
	return val;
}

int PgTransaction::GetChangeset(int64_t objId,
	class PgChangeset &changesetOut,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	string errStrNative;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	int ret = GetChangesetFromDb(*dbconn, work.get(),
		this->tableActivePrefix,
		this->dbUsernameLookup, 
		objId,
		changesetOut,
		errStrNative);

	if(ret == -1)
	{
		ret = GetChangesetFromDb(*dbconn, work.get(),
			this->tableStaticPrefix,
			this->dbUsernameLookup, 
			objId,
			changesetOut,
			errStrNative);
	}

	errStr.errStr = errStrNative;
	return ret;
}

int PgTransaction::GetChangesetOsmChange(int64_t changesetId,
	std::shared_ptr<class IOsmChangeBlock> output,
	class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	std::shared_ptr<class OsmData> data(new class OsmData());
	GetAllNodesByChangeset(*dbconn, work.get(), this->dbUsernameLookup, 
		this->tableStaticPrefix,
		"", changesetId,
		data);
	GetAllNodesByChangeset(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix,
		"", changesetId,
		data);

	GetAllWaysByChangeset(*dbconn, work.get(), this->dbUsernameLookup,	
		this->tableStaticPrefix,
		"", changesetId,
		data);
	GetAllWaysByChangeset(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix,
		"", changesetId,
		data);

	GetAllRelationsByChangeset(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix,
		"", changesetId,
		data);
	GetAllRelationsByChangeset(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix,
		"", changesetId,
		data);

	class OsmData created, modified, deleted;
	FilterObjectsInOsmChange(1, *data, created);
	FilterObjectsInOsmChange(2, *data, modified);
	FilterObjectsInOsmChange(3, *data, deleted);

	if(!created.IsEmpty())
		output->StoreOsmData("create", created, false);
	if(!modified.IsEmpty())
		output->StoreOsmData("modify", modified, false);
	if(!deleted.IsEmpty())	
		output->StoreOsmData("delete", deleted, false);
	return 1;
}

bool PgTransaction::GetChangesets(std::vector<class PgChangeset> &changesetsOut,
	int64_t user_uid, //0 means don't filter
	int64_t openedBeforeTimestamp, //-1 means don't filter
	int64_t closedAfterTimestamp, //-1 means don't filter
	bool is_open_only,
	bool is_closed_only,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	string errStrNative;
	size_t targetNum = 100;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = GetChangesetsFromDb(*dbconn, work.get(),
		this->tableActivePrefix, "",
		this->dbUsernameLookup, 
		targetNum,
		user_uid, 
		openedBeforeTimestamp, 
		closedAfterTimestamp,
		is_open_only,
		is_closed_only,
		changesetsOut,
		errStrNative);
	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	if(changesetsOut.size() < targetNum)
	{
		std::vector<class PgChangeset> changesetsStatic;
		ok = GetChangesetsFromDb(*dbconn, work.get(),
			this->tableStaticPrefix,
			this->tableActivePrefix,
			this->dbUsernameLookup, 
			targetNum - changesetsOut.size(),
			user_uid,
			openedBeforeTimestamp, 
			closedAfterTimestamp,
			is_open_only,
			is_closed_only,
			changesetsStatic,
			errStrNative);
		if(!ok)
		{
			errStr.errStr = errStrNative;
			return false;
		}
		changesetsOut.insert(changesetsOut.end(), changesetsStatic.begin(), changesetsStatic.end());
	}

	return true;
}

int64_t PgTransaction::CreateChangeset(const class PgChangeset &changeset,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;	
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	class PgChangeset changesetMod(changeset);
	if(changesetMod.objId != 0)
		throw invalid_argument("Changeset ID should be zero since it is allocated by pgmap");

	int64_t cid = this->GetAllocatedId("changeset");
	changesetMod.objId = cid;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = InsertChangesetInDb(*dbconn, work.get(),
		this->tableActivePrefix,
		changesetMod,
		errStrNative);

	errStr.errStr = errStrNative;
	if(!ok)
		return 0;
	return cid;
}

bool PgTransaction::UpdateChangeset(const class PgChangeset &changeset,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;	
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Attempt to update in active table
	int rowsAffected = UpdateChangesetInDb(*dbconn, work.get(),
		this->tableActivePrefix,
		changeset,
		errStrNative);

	if(rowsAffected < 0)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	if(rowsAffected > 0)
		return true; //Success

	//Update a changeset in the static tables by copying to active table
	//and updating its values there
	size_t rowsAffected2 = 0;
	bool ok = CopyChangesetToActiveInDb(*dbconn, work.get(),
		this->tableStaticPrefix,
		this->tableActivePrefix,
		this->dbUsernameLookup, 
		changeset.objId,
		rowsAffected2,
		errStrNative);

	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}
	if(rowsAffected2 == 0)
	{
		errStr.errStr = "No changeset found in active or static table";
		return false;
	}

	return rowsAffected2 > 0;
}

bool PgTransaction::CloseChangeset(int64_t changesetId,
	int64_t closedTimestamp,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	size_t rowsAffected = 0;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = CloseChangesetInDb(*dbconn, work.get(),
		this->tableActivePrefix,
		changesetId,
		closedTimestamp,
		rowsAffected,
		errStrNative);

	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	if(rowsAffected == 0)
	{
		//Close a changeset in the static tables by copying to active table
		//and setting the is_open flag to false.
		ok = CopyChangesetToActiveInDb(*dbconn, work.get(),
			this->tableStaticPrefix,
			this->tableActivePrefix,
			this->dbUsernameLookup, 
			changesetId,
			rowsAffected,
			errStrNative);

		if(!ok)
		{
			errStr.errStr = errStrNative;
			return false;
		}
		if(rowsAffected == 0)
		{
			errStr.errStr = "No changeset found in active or static table";
			return false;
		}

		ok = CloseChangesetInDb(*dbconn, work.get(),
			this->tableActivePrefix,
			changesetId,
			closedTimestamp,
			rowsAffected,
			errStrNative);
	}

	errStr.errStr = errStrNative;
	return ok;
}

bool PgTransaction::CloseChangesetsOlderThan(int64_t whereBeforeTimestamp,
	int64_t closedTimestamp,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	size_t rowsAffected = 0;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Find changesets in static that have not been closed in active tables
	std::vector<class PgChangeset> openStaticChangesets;
	bool ok = GetChangesetsFromDb(*dbconn, work.get(),
		this->tableStaticPrefix, this->tableActivePrefix,
		this->dbUsernameLookup, 
		0, //Get all
		0, //Any user 
		whereBeforeTimestamp, 
		-1,
		true, //That are open
		false,
		openStaticChangesets,
		errStrNative);
	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	//Migrate open static changesets to active table
	for(size_t i=0; i<openStaticChangesets.size(); i++)
	{
		size_t rowsAffected = 0;
		ok = CopyChangesetToActiveInDb(*dbconn, work.get(),
			this->tableStaticPrefix,
			this->tableActivePrefix,
			this->dbUsernameLookup, 
			openStaticChangesets[i].objId,
			rowsAffected,
			errStrNative);

		if(!ok)
		{
			errStr.errStr = errStrNative;
			return false;
		}
	}

	//Close changesets in active table
	ok = CloseChangesetsOlderThanInDb(*dbconn, work.get(),
		this->tableActivePrefix,
		whereBeforeTimestamp,
		closedTimestamp,
		rowsAffected,
		errStrNative);

	errStr.errStr = errStrNative;
	return ok;
}

std::string PgTransaction::GetMetaValue(const std::string &key, 
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	string errStrNative;
	std::string val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	try
	{
		val = DbGetMetaValue(*dbconn, work.get(),
			key, 
			this->tableActivePrefix,
			errStrNative);
	}
	catch(runtime_error &err)
	{
		//Hard coded defaults
		if(key == "readonly")
			return "0";

		throw err;
	}
	return val;
}

bool PgTransaction::SetMetaValue(const std::string &key, 
	const std::string &value, 
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ret = DbSetMetaValue(*dbconn, work.get(),
		key, 
		value, 
		this->tableActivePrefix, 
		errStrNative);
	errStr.errStr = errStrNative;
	return ret;
}

bool PgTransaction::UpdateUsername(int uid, const std::string &username,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbUpsertUsernamePrepare(*dbconn, work.get(), this->tableActivePrefix);

	DbUpsertUsername(*dbconn, work.get(), this->tableActivePrefix, 
		uid, username);

	return true;
}

void PgTransaction::Commit()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	//Release locks
	work->commit();
}

void PgTransaction::Abort()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	work->abort();
}

// **********************************************

PgAdmin::PgAdmin(shared_ptr<pqxx::connection> dbconnIn,
		const string &tableStaticPrefixIn, 
		const string &tableModPrefixIn,
		const string &tableTestPrefixIn,
		std::shared_ptr<class PgWork> sharedWorkIn,
		const string &shareModeIn):

	PgCommon(dbconnIn, tableStaticPrefixIn, tableModPrefixIn, sharedWorkIn, shareMode),
	tableModPrefix(tableModPrefixIn),
	tableTestPrefix(tableTestPrefixIn)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	if(shareMode.size() > 0)
	{
		string errStr;
		bool ok = LockMap(work, this->tableStaticPrefix, this->shareMode, errStr);
		if(!ok)
			throw runtime_error(errStr);
		ok = LockMap(work, this->tableModPrefix, this->shareMode, errStr);
		if(!ok)
			throw runtime_error(errStr);
		ok = LockMap(work, this->tableTestPrefix, this->shareMode, errStr);
		if(!ok)
			throw runtime_error(errStr);
	}
}

PgAdmin::~PgAdmin()
{
	this->sharedWork->work.reset();
	this->sharedWork.reset();
}

bool PgAdmin::IsAdminMode()
{
	return true;
}

bool PgAdmin::CreateMapTables(int verbose, int targetVer, bool latest, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, "", this->tableStaticPrefix, targetVer, latest, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, this->tableStaticPrefix, this->tableModPrefix, targetVer, latest, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, this->tableStaticPrefix, this->tableTestPrefix, targetVer, latest, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::DropMapTables(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, "", this->tableStaticPrefix, 0, false, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, this->tableStaticPrefix, this->tableModPrefix, 0, false, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, this->tableStaticPrefix, this->tableTestPrefix, 0, false, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::CopyMapData(int verbose, const std::string &filePrefix, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbCopyData(*dbconn, work.get(), verbose, filePrefix, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::CreateMapIndices(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbCreateIndices(*dbconn, work.get(), verbose, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateIndices(*dbconn, work.get(), verbose, this->tableModPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateIndices(*dbconn, work.get(), verbose, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::ApplyDiffs(const std::string &diffPath, int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbApplyDiffs(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, diffPath, this, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

bool PgAdmin::RefreshMapIds(int verbose, class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");	

	ClearNextIdValuesById(*dbconn, work.get(), this->tableStaticPrefix, "node");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableStaticPrefix, "way");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableStaticPrefix, "relation");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableModPrefix, "node");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableModPrefix, "way");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableModPrefix, "relation");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableTestPrefix, "node");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableTestPrefix, "way");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableTestPrefix, "relation");
	
	std::string nativeErrStr;

	bool ok = DbRefreshMaxIds(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

bool PgAdmin::ImportChangesetMetadata(const std::string &fina, int verbose, class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	class OsmChangesetsDecodeString osmChangesetsDecodeString;

	std::string content;
	try
	{
		int ret = ReadFileContents(fina.c_str(), 0, content);
		if(ret < 1)
		{
			errStr.errStr = "Error reading file";
			return false;
		}
	}
	catch(const std::bad_alloc &err)
	{
		errStr.errStr = "Failed to allocate buffer: ";
		errStr.errStr += err.what();
		return false;
	}
	cout << fina << "," << content.length() << endl;

	osmChangesetsDecodeString.DecodeSubString(content.c_str(), content.length(), 1);

	if(!osmChangesetsDecodeString.parseCompletedOk)
	{
		errStr = osmChangesetsDecodeString.errString;
		return false;
	}

	bool ok = true;
	for(size_t i=0; i<osmChangesetsDecodeString.outChangesets.size(); i++)
	{
		int rowsAffected = UpdateChangesetInDb(*dbconn, work.get(), 
			this->tableStaticPrefix,
			osmChangesetsDecodeString.outChangesets[i],
			errStr.errStr);

		if(rowsAffected==0)
			ok = InsertChangesetInDb(*dbconn, work.get(), 
				this->tableStaticPrefix,
				osmChangesetsDecodeString.outChangesets[i],
				errStr.errStr);
		if(!ok)
			break;
	}

	return ok;
}

bool PgAdmin::RefreshMaxChangesetUid(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbRefreshMaxChangesetUid(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

bool PgAdmin::GenerateUsernameTable(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbGenerateUsernameTable(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

bool PgAdmin::UpdateBboxes(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	/*bool ok = DbUpdateWayBboxes(*dbconn, work.get(), verbose, 
		this->tableStaticPrefix, 
		this,
		nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;*/

	/*ok = DbUpdateWayBboxes(*dbconn, work.get(), verbose,
		this->tableModPrefix, 
		this,
		nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;*/

	bool ok = DbUpdateRelationBboxes(*dbconn, work.get(), verbose, 
		this->tableStaticPrefix, 
		this,
		nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	/*ok = DbUpdateRelationBboxes(*dbconn, work.get(), verbose,
		this->tableModPrefix, 
		this,
		nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;*/

	work->commit();

	return true;
}

bool PgAdmin::CheckNodesExistForWays(class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbCheckNodesExistForAllWays(*dbconn, work.get(), this->tableStaticPrefix, this->tableModPrefix,
		this->tableStaticPrefix, this->tableModPrefix);

	DbCheckNodesExistForAllWays(*dbconn, work.get(), this->tableModPrefix, "",
		this->tableStaticPrefix, this->tableModPrefix);

	return true;
}

bool PgAdmin::CheckObjectIdTables(class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	vector<string> objTypes = {"node", "way", "relation"};

	for(size_t i=0; i<objTypes.size(); i++)
	{
		DbCheckObjectIdTables(*dbconn, work.get(),
			this->tableModPrefix, "live", objTypes[i]);

		DbCheckObjectIdTables(*dbconn, work.get(),
			this->tableModPrefix, "old", objTypes[i]);

		DbCheckObjectIdTables(*dbconn, work.get(),
			this->tableStaticPrefix, "live", objTypes[i]);

		DbCheckObjectIdTables(*dbconn, work.get(),
			this->tableStaticPrefix, "old", objTypes[i]);
	}
	
	return true;
}

void PgAdmin::Commit()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	//Release locks
	work->commit();
}

void PgAdmin::Abort()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	work->abort();
}

// **********************************************

PgMap::PgMap(const string &connection, const string &tableStaticPrefixIn, 
	const string &tableActivePrefixIn,
	const string &tableModPrefixIn,
	const string &tableTestPrefixIn)
{
	dbconn.reset(new pqxx::connection(connection));
	connectionString = connection;
	tableStaticPrefix = tableStaticPrefixIn;
	tableActivePrefix = tableActivePrefixIn;
	tableModPrefix = tableModPrefixIn;
	tableTestPrefix = tableTestPrefixIn;
}

PgMap::~PgMap()
{
	if(this->sharedWork)
		this->sharedWork->work.reset();
	this->sharedWork.reset();

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
	if(this->sharedWork)
		this->sharedWork->work.reset();
	this->sharedWork.reset(new class PgWork(new pqxx::transaction<pqxx::repeatable_read>(*dbconn)));
	shared_ptr<class PgTransaction> out(new class PgTransaction(dbconn, tableStaticPrefix, tableActivePrefix, this->sharedWork, shareMode));
	return out;
}

std::shared_ptr<class PgAdmin> PgMap::GetAdmin()
{
	dbconn->cancel_query();
	if(this->sharedWork)
		this->sharedWork->work.reset();
	this->sharedWork.reset(new class PgWork(new pqxx::nontransaction(*dbconn)));
	shared_ptr<class PgAdmin> out(new class PgAdmin(dbconn, tableStaticPrefix, tableModPrefix, tableTestPrefix, this->sharedWork, ""));
	return out;
}

std::shared_ptr<class PgAdmin> PgMap::GetAdmin(const std::string &shareMode)
{
	dbconn->cancel_query();
	if(this->sharedWork)
		this->sharedWork->work.reset();
	this->sharedWork.reset(new class PgWork(new pqxx::transaction<pqxx::repeatable_read>(*dbconn)));
	shared_ptr<class PgAdmin> out(new class PgAdmin(dbconn, tableStaticPrefix, tableModPrefix, tableTestPrefix, this->sharedWork, shareMode));
	return out;
}

