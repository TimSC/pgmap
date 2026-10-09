#include "pgmap.h"
#include "dbquery.h"
#include "dbextract.h"
#include "dbprepared.h"
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
#include "dboverpass.h"
#include "util.h"
#include "cppo5m/model.h"
#include <algorithm>
#include <cmath>
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
		sql += ","+prefix+ "query_activity";
		sql += ","+prefix+ "edit_activity";
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
	created_count = 0; modified_count = 0; deleted_count = 0;
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
	created_count = obj.created_count;
	modified_count = obj.modified_count;
	deleted_count = obj.deleted_count;
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
	useBboxInQuery = 0;
}

PgMapQuery::~PgMapQuery()
{
	this->Reset();
}

PgMapQuery& PgMapQuery::operator=(const PgMapQuery&)
{
	return *this;
}

int PgMapQuery::StartCommon(const vector<double> &bbox, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc)
{
	/**
	\param area being queried
	\param timestamp the timestamp right now (0 if we don't care about logging accurate query times)
	\param enc output object to receive the query result
	*/
	this->mapQueryEnc = enc;
	this->retainNodeIds.reset(new class DataStreamRetainIds(*enc.get()));
	this->retainWayIds.reset(new class DataStreamRetainIds(this->nullEncoder));
	this->retainWayMemIds.reset(new class DataStreamRetainMemIds(*this->retainWayIds));
	this->retainRelationIds.reset(new class DataStreamRetainIds(*this->mapQueryEnc));

	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Check if bbox data has been enabled for ways and relations
	string errStrNative;
	string useBboxInQueryStr;
	try
	{
		useBboxInQueryStr = DbGetMetaValue(*dbconn, work.get(),
			"useBboxInQuery", 
			this->tableActivePrefix,
			errStrNative);
	}
	catch(runtime_error &err)
	{		
	}
	this->useBboxInQuery = atoi(useBboxInQueryStr.c_str()) == 1;

	string errStr;
	bool ok = DbInsertQueryActivity(*dbconn, work.get(), this->tableActivePrefix,
		timestamp,
		bbox,
		errStr,
		0);
	if (!ok)
		cout << errStr << endl;

	return 0;
}

int PgMapQuery::Start(const vector<double> &bbox, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc)
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

	return this->StartCommon(bbox, timestamp, enc);
}

int PgMapQuery::Start(const std::string &wkt, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc)
{
	if(mapQueryActive)
		throw runtime_error("Query already active");
	if(dbconn.get() == NULL)
		throw runtime_error("DB pointer not set for PgMapQuery");
	mapQueryActive = true;
	this->mapQueryPhase = 0;
	this->mapQueryWkt = wkt;

	std::vector<double> emptyBbox;
	return this->StartCommon(emptyBbox, timestamp, enc);
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
			this->mapQueryEnc->StoreBounds(Bounds(this->mapQueryBbox[0], this->mapQueryBbox[1], this->mapQueryBbox[2], this->mapQueryBbox[3]));
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
		//Keep the way object IDs in memory until we have finished encoding nodes
		if(!useBboxInQuery)
		{
			GetLiveWaysThatContainNodes(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableStaticPrefix, this->tableActivePrefix, retainNodeIds->nodeIds, retainWayMemIds);

			GetLiveWaysThatContainNodes(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "", retainNodeIds->nodeIds, retainWayMemIds);
		}
		else
		{
			DbXapiQueryObjVisible(*dbconn, work.get(), 
				this->dbUsernameLookup, 
				this->tableActivePrefix, 
				"way",
				"",
				"",
				this->mapQueryBbox, 
				retainWayMemIds);
		}

		cout << "Found " << this->retainWayIds->wayIds.size() << " ways depend on " << retainWayMemIds->nodeIds.size() << " nodes" << endl;

		//Identify extra node IDs to complete ways
		this->extraNodes.clear();
		std::set_difference(retainWayMemIds->nodeIds.begin(), retainWayMemIds->nodeIds.end(), 
			retainNodeIds->nodeIds.begin(), retainNodeIds->nodeIds.end(),
			std::inserter(this->extraNodes, this->extraNodes.end()));
		cout << "num extraNodes " << this->extraNodes.size() << endl;

		//Get node objects to complete these ways
		this->setIterator = this->extraNodes.begin();

		this->mapQueryPhase = 7;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	//Step 6 simplified out

	if(this->mapQueryPhase == 7)
	{
		if(this->setIterator != this->extraNodes.end())
		{
			GetVisibleObjectsById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "node", this->extraNodes, 
				this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->setIterator = this->retainWayIds->wayIds.begin();

		//Write ways to output
		this->mapQueryEnc->Reset();

		this->mapQueryPhase = 9;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	//Step 8 simplified out

	if(this->mapQueryPhase == 9)
	{		
		if(this->setIterator != this->retainWayIds->wayIds.end())
		{
			GetVisibleObjectsById(*dbconn, work.get(), this->dbUsernameLookup,
				this->tableActivePrefix, "way",
				this->retainWayIds->wayIds, this->setIterator, 1000, this->mapQueryEnc);
			return 0;
		}

		this->mapQueryEnc->Reset();
		this->setIterator = this->retainNodeIds->nodeIds.begin();

		this->mapQueryPhase = 10;
		if(verbose >= 1)
			cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
		return 0;
	}

	if(!useBboxInQuery)
	{
		if(this->mapQueryPhase == 10)
		{
			//Get relations that reference any of the above nodes within bbox
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
			//Get relations that reference any of the "extra nodes"
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
			//Get relations that reference any of the above ways
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
	}
	else
	{
		if(this->mapQueryPhase == 10)
		{
			//Get relations that overlap bbox
			DbXapiQueryObjVisible(*dbconn, work.get(), 
				this->dbUsernameLookup, 
				this->tableActivePrefix, 
				"relation",
				"",
				"",
				this->mapQueryBbox, 
				retainRelationIds);

			cout << "found " << retainRelationIds->relationIds.size() << " relations" << endl;

			this->mapQueryPhase = 16;
			if(verbose >= 1)
				cout << "mapQueryPhase increased to " << this->mapQueryPhase << endl;
			return 0;
		}
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

	std::set<int64_t>::const_iterator it = objectIds.begin();
	while(it != objectIds.end())
		GetVisibleObjectsById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, objectIds, 
			it, 1000, out);

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
		for(size_t i=0; i<mainWay.refs.size(); i++)
			memberNodes.insert(mainWay.refs[i]);
		this->GetObjectsById("node", memberNodes, outData);
 	}
	else if(type == "relation")
	{
		if (outData->relations.size() != 1)
			return; //Unexpected number of objects in intermediate result
		class OsmRelation &mainRelation = outData->relations[0];

		std::set<int64_t> memberNodes, memberWays, memberRelations;
		for(const RelationMember &member : mainRelation.members)
		{
			if(member.type == ObjectType::Node)
				memberNodes.insert(member.ref);
			else if(member.type == ObjectType::Way)
				memberWays.insert(member.ref);
			else if(member.type == ObjectType::Relation)
				memberRelations.insert(member.ref);
		}

		std::shared_ptr<class OsmData> memberWayObjs(new class OsmData());
		this->GetObjectsById("node", memberNodes, outData);
		this->GetObjectsById("way", memberWays, memberWayObjs);
		this->GetObjectsById("relation", memberRelations, outData);
		memberWayObjs->StreamTo(*outData.get());

		std::set<int64_t> memberNodes2;
		for(size_t i=0; i<memberWayObjs->ways.size(); i++)
			for(size_t j=0; j<memberWayObjs->ways[i].refs.size(); j++)
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

// A streaming database sink: object contents remain in PostgreSQL, not OsmData.
// The query and destination writes use the same repeatable-read transaction.
class ExtractDatabaseWriter : public IDataStreamHandler
{
	pqxx::connection &connection;
	pqxx::transaction_base &work;
	string prefix;
	int64_t extractId;

	void CopyObject(const string &kind, int64_t id)
	{
		string columns = "id, changeset, changeset_index, username, uid, timestamp, version, tags";
		if(kind == "node") columns += ", geom";
		else if(kind == "way") columns += ", members, bbox";
		else columns += ", members, memberroles, bbox";
		string destination = connection.quote_name(prefix + "extract_live" + kind + "s");
		string source = connection.quote_name(prefix + "visible" + kind + "s");
		work.exec("INSERT INTO " + destination + " (extract_id, " + columns + ") SELECT " +
			to_string(extractId) + ", " + columns + " FROM " + source + " WHERE id=" +
			to_string(id) + " ON CONFLICT (extract_id, id) DO NOTHING");
	}

	// types, when given, selects the members whose type starts with the
	// letter ending the table suffix.
	void Membership(const string &suffix, int64_t id, int64_t version,
		const vector<int64_t> &refs, const vector<ObjectType> *types = nullptr)
	{
		// Bound each SQL batch even for unusually large ways or relations.
		for(size_t start = 0; start < refs.size(); start += 1000)
		{
			string sql = "INSERT INTO " + connection.quote_name(prefix + "extract_" + suffix) +
				" (extract_id, id, version, index, member) VALUES ";
			bool any = false;
			for(size_t i = start; i < refs.size() && i < start + 1000; ++i)
			{
				if(types && suffix.back() != ObjectTypeName((*types)[i])[0]) continue;
				if(any) sql += ",";
				any = true;
				sql += "(" + to_string(extractId) + "," + to_string(id) + "," +
					to_string(version) + "," + to_string(i) + "," + to_string(refs[i]) + ")";
			}
			if(any) work.exec(sql + " ON CONFLICT (extract_id, id, index) DO NOTHING");
		}
	}
public:
	ExtractDatabaseWriter(pqxx::connection &c, pqxx::transaction_base &w,
		const string &p, int64_t id): connection(c), work(w), prefix(p), extractId(id) {}
	void StoreNode(const OsmNode &node) override
	{
		CopyObject("node", node.objId);
	}
	void StoreWay(const OsmWay &way) override
	{
		CopyObject("way", way.objId);
		Membership("way_mems", way.objId, way.metaData.version, way.refs);
	}
	void StoreRelation(const OsmRelation &relation) override
	{
		vector<int64_t> refs;
		vector<ObjectType> types;
		for(const RelationMember &member : relation.members)
		{
			refs.push_back(member.ref);
			types.push_back(member.type);
		}
		CopyObject("relation", relation.objId);
		for(const char *suffix : {"relation_mems_n", "relation_mems_w", "relation_mems_r"})
			Membership(suffix, relation.objId, relation.metaData.version, refs, &types);
	}
};

void PgTransaction::LockExtractTables(const string &accessMode)
{
	if(accessMode != "ACCESS SHARE" && accessMode != "EXCLUSIVE")
		throw invalid_argument("Unsupported extract lock mode");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	// The constructor acquires static then active main-map locks first.
	// Every extract operation must use this same table order, in one statement,
	// before accessing any extract metadata, objects or membership rows.
	string sql = "LOCK TABLE ";
	bool first = true;
	for(const char *name : {"extracts", "extract_livenodes", "extract_liveways",
		"extract_liverelations", "extract_way_mems", "extract_relation_mems_n",
		"extract_relation_mems_w", "extract_relation_mems_r"})
	{
		if(!first) sql += ",";
		first = false;
		sql += dbconn->quote_name(tableActivePrefix + name);
	}
	work->exec(sql + " IN " + accessMode + " MODE;");
	// Locks are owned by the transaction and released at commit or abort.
}

int64_t PgTransaction::SaveExtract(const vector<double> &bbox, const string &name)
{
	if(bbox.size() != 4) throw invalid_argument("Bbox must have four coordinates");
	for(double value : bbox)
		if(!std::isfinite(value)) throw invalid_argument("Bbox coordinates must be finite");
	if(bbox[0] < -180 || bbox[2] > 180 || bbox[1] < -90 || bbox[3] > 90 ||
		bbox[0] >= bbox[2] || bbox[1] >= bbox[3])
		throw invalid_argument("Bbox must be a nonempty longitude/latitude rectangle");
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while creating an extract");
	LockExtractTables("EXCLUSIVE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	// Capture both cursors from the same snapshot as the map query. No later
	// commits can advance these checkpoints beyond the contents being extracted.
	string activity = dbconn->quote_name(tableActivePrefix + "edit_activity");
	auto checkpoint = work->exec("SELECT COALESCE(max(id),0), COALESCE(max(atomic_edit_id),0) FROM " + activity);
	auto mode = work->exec("SELECT value FROM " + dbconn->quote_name(tableActivePrefix + "meta") +
		" WHERE key='useBboxInQuery'");
	bool bboxMode = !mode.empty() && atoi(mode[0][0].as<string>().c_str()) == 1;
	string envelope = "ST_MakeEnvelope(";
	for(size_t i=0; i<4; ++i) envelope += work->quote(bbox[i]) + ",";
	envelope += "4326)";
	string sql = "INSERT INTO " + dbconn->quote_name(tableActivePrefix + "extracts") +
		" (name, bbox, use_bbox_in_query, performed_at, edit_activity_id, atomic_edit_id) VALUES (" +
		work->quote(name) + "," + envelope + "," + (bboxMode ? "true" : "false") +
		",CURRENT_TIMESTAMP," + checkpoint[0][0].as<string>() + "," +
		checkpoint[0][1].as<string>() + ") RETURNING id";
	int64_t id = work->exec(sql)[0][0].as<int64_t>();
	shared_ptr<IDataStreamHandler> sink = make_shared<ExtractDatabaseWriter>(*dbconn, *work, tableActivePrefix, id);
	auto query = GetQueryMgr();
	int status = query->Start(bbox, time(nullptr), sink);
	while(status == 0) status = query->Continue();
	if(status < 0) throw runtime_error("Extract map query failed");
	return id;
}

int64_t PgTransaction::UpdateExtract(int64_t extractId, const string &name)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Main map must be locked before updating an extract");
	LockExtractTables("EXCLUSIVE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	return DbUpdateExtract(*dbconn, *work, tableStaticPrefix, tableActivePrefix, extractId, name);
}

std::shared_ptr<PgExtractExport> PgTransaction::StartExportExtract(int64_t extractId,
    const string &name, shared_ptr<IDataStreamHandler> output)
{
    if(extractId < 0 || (!extractId && name.empty()) || !output)
        throw invalid_argument("Select an extract by positive ID or nonempty name");
    if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
        throw runtime_error("Map must be locked while exporting an extract");
    LockExtractTables("ACCESS SHARE");
    return shared_ptr<PgExtractExport>(new PgExtractExport(dbconn, sharedWork,
        tableActivePrefix, extractId, name, output));
}

int64_t PgTransaction::ExportExtract(int64_t extractId, const string &name,
    shared_ptr<IDataStreamHandler> output)
{
    auto exporter = StartExportExtract(extractId, name, output);
    while(exporter->Continue() != 1) {}
    return exporter->GetId();
}

std::shared_ptr<PgExtractImport> PgTransaction::StartImportExtract(const string &name,
	const vector<double> &bbox, int64_t editActivityId, int64_t atomicEditId)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while importing an extract");
	LockExtractTables("EXCLUSIVE");
	return shared_ptr<PgExtractImport>(new PgExtractImport(dbconn, sharedWork,
		tableActivePrefix, name, bbox, editActivityId, atomicEditId));
}

int64_t PgTransaction::DeleteExtract(int64_t extractId, const string &name)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while deleting an extract");
	LockExtractTables("EXCLUSIVE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	return DbDeleteExtract(*dbconn, *work, tableActivePrefix, extractId, name);
}

void PgTransaction::ListExtracts(vector<ExtractInfo> &out)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while listing extracts");
	LockExtractTables("ACCESS SHARE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	DbListExtracts(*dbconn, *work, tableActivePrefix, 0, false, out);
}

bool PgTransaction::GetExtract(int64_t extractId, ExtractInfo &out)
{
	if(extractId <= 0) throw invalid_argument("Extract ID must be positive");
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while reading an extract");
	LockExtractTables("ACCESS SHARE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	vector<ExtractInfo> found;
	DbListExtracts(*dbconn, *work, tableActivePrefix, extractId, true, found);
	if(found.empty()) return false;
	out = found[0];
	return true;
}

void PgTransaction::CompareExtract(int64_t extractId, const string &name,
	ExtractComparison &out)
{
	DbCompareExtract(*this, extractId, name, out);
}

void PgTransaction::CompareAllExtracts(vector<ExtractComparison> &out)
{
	out.clear();
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while comparing extracts");
	LockExtractTables("ACCESS SHARE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	for(int64_t id : DbListExtractIds(*dbconn, *work, tableActivePrefix))
	{
		out.emplace_back();
		DbCompareExtract(*this, id, "", out.back());
	}
}

bool PgTransaction::InsertEditActivity(const class EditActivity &activity,
		class PgMapError &errStr)
{
	/*
	 * Retaining context for rectangular extract synchronization
	 * -------------------------------------------------------
	 * An extract is a map-query result, not simply nodes inside its rectangle.
	 * In membership-based query mode, an inside node selects its parent ways,
	 * and all nodes of those ways are returned, including outside nodes. Direct
	 * relations of those returned nodes/ways are also selected. In bbox mode,
	 * stored way/relation envelopes determine selection instead. Neither mode
	 * implies unlimited expansion of the graph or completion of all relations.
	 * Consequently, an edit to an outside node can affect an extract, and moving
	 * a node inside can require adding an unchanged way and its other nodes.
	 * Recording only modified objects or their new positions is insufficient.
	 *
	 * The caller captures original object state before applying a block, then
	 * constructs this activity after applying it, while the map remains locked.
	 * existing/updated record object type, ID and version; affectedparents and
	 * related retain additional versioned references. Full object contents stay
	 * in the map's static, live and history tables rather than being duplicated
	 * in each activity row. Type is required because IDs are per object type;
	 * version is required because fetching today's object by ID would lose the
	 * state associated with this edit. Referenced history must remain available
	 * for as long as consumers are allowed to synchronize from these records.
	 * All lookups must respect the static baseline and active overlay, including
	 * suppression of overridden/deleted static objects.
	 *
	 * sync_before/sync_after are intended to retain the relevant versioned
	 * context on each side of the edit. bbox_before/bbox_after retain matching
	 * spatial extents as SRID-4326 GeometryCollections. JSON array entry i matches
	 * geometry component i+1: counts AND ordering must agree. Separate extents
	 * matter because a node can enter or leave a rectangle, and an unchanged
	 * parent object's version can acquire a different extent when a node moves.
	 * An object version alone also does not identify its members' versions at
	 * this revision; retaining footprints does not reconstruct the whole graph.
	 *
	 * Current coverage is deliberately limited: the upload caller records old
	 * versions and point geometries of accepted node modifications/deletions.
	 * Creation has an empty before array and empty collection. Deletions skipped
	 * by if-unused are excluded. After-state and complete parent/dependency sync
	 * context are not populated yet, so these rows alone do not constitute a
	 * complete extract-update algorithm. Unrecorded context is SQL NULL; known
	 * empty context is [] paired with GEOMETRYCOLLECTION EMPTY.
	 *
	 * A future consumer uses retained spatial/dependency context to find candidate
	 * edits, retrieves the referenced historical objects, and determines which
	 * objects enter, leave, or change in its query result. Spatial overlap is a
	 * candidate filter, not proof of membership. Before removing a completion
	 * node or relation, other retained selection reasons must be checked. Leaving
	 * an extract is distinct from deletion from the global map. The correctness
	 * target is the result of a fresh query at the consumer's target revision.
	 *
	 * One upload may contain multiple action blocks. This method gives all rows
	 * in this PgTransaction one atomic_edit_id and ordered block_index values.
	 * Activity inserts and map changes share the same database transaction: a
	 * later validation/storage failure rolls them all back. Consumers must apply
	 * complete groups before advancing their checkpoint, not individual rows or
	 * timestamp windows. The exclusive locks serialize ID allocation and remain
	 * held until commit/abort; sequence gaps after rollback are harmless. Legacy
	 * rows with NULL grouping cannot be assumed to share transaction boundaries.
	 */
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	// Allocate only after the exclusive map locks have been acquired. The next
	// writer cannot allocate until this transaction commits or aborts.
	if(atomicEditId == 0)
	{
		string sequence = dbconn->quote_name(this->tableActivePrefix + "atomic_edit_id_seq");
		atomicEditId = work->exec("SELECT nextval(" + work->quote(sequence) + "::regclass)")[0][0].as<int64_t>();
	}
	EditActivity groupedActivity(activity);
	groupedActivity.atomicEditId = atomicEditId;
	groupedActivity.blockIndex = activityBlockIndex;
	bool ok = DbInsertEditActivity(*dbconn, work.get(), this->tableActivePrefix,
		groupedActivity,
		nativeErrStr,
		0);
	if(ok) ++activityBlockIndex;

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
	std::shared_ptr<class IOsmChangeHandler> output,
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

	const std::pair<const char *, const OsmData *> groups[] = {
		{"create", &created}, {"modify", &modified}, {"delete", &deleted}};
	for(const auto &group : groups)
	{
		if(group.second->IsEmpty())
			continue;
		OsmChangeBlock block(group.first);
		block.data = *group.second;
		output->StoreChangeBlock(block);
	}
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
	class PgChangesetQuery query;
	query.user_uid = user_uid;
	query.openedBeforeTimestamp = openedBeforeTimestamp;
	query.closedAfterTimestamp = closedAfterTimestamp;
	query.is_open_only = is_open_only;
	query.is_closed_only = is_closed_only;
	return this->GetChangesets(changesetsOut, query, errStr);
}

bool PgTransaction::GetChangesets(std::vector<class PgChangeset> &changesetsOut,
	const class PgChangesetQuery &query,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	string errStrNative;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = GetChangesetsFromDb(*dbconn, work.get(),
		this->tableActivePrefix, "",
		this->dbUsernameLookup, 
		query,
		changesetsOut,
		errStrNative);
	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	//Either set of tables could hold all of the changesets wanted, so each is
	//asked for the full number before the two are merged
	std::vector<class PgChangeset> changesetsStatic;
	ok = GetChangesetsFromDb(*dbconn, work.get(),
		this->tableStaticPrefix,
		this->tableActivePrefix,
		this->dbUsernameLookup, 
		query,
		changesetsStatic,
		errStrNative);
	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}
	changesetsOut.insert(changesetsOut.end(), changesetsStatic.begin(), changesetsStatic.end());

	bool oldestFirst = query.oldestFirst;
	std::stable_sort(changesetsOut.begin(), changesetsOut.end(),
		[oldestFirst](const PgChangeset &a, const PgChangeset &b) {
			if(a.open_timestamp != b.open_timestamp)
				return oldestFirst ? a.open_timestamp < b.open_timestamp : a.open_timestamp > b.open_timestamp;
			return oldestFirst ? a.objId < b.objId : a.objId > b.objId;
		});
	if(query.limit > 0 && changesetsOut.size() > query.limit)
		changesetsOut.resize(query.limit);
	return true;
}

void PgTransaction::GetChangesetChangeCounts(std::vector<class PgChangeset> &changesets)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	DbGetChangesetChangeCounts(*dbconn, work.get(), this->tableActivePrefix, changesets);
}

void PgTransaction::GetChangesetChangeCounts(class PgChangeset &changeset)
{
	std::vector<class PgChangeset> changesets = {changeset};
	this->GetChangesetChangeCounts(changesets);
	changeset = changesets[0];
}

int64_t PgTransaction::GetChangesetCount(int64_t user_uid)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	return DbCountChangesets(*dbconn, work.get(), this->tableActivePrefix, "", user_uid) +
		DbCountChangesets(*dbconn, work.get(), this->tableStaticPrefix, this->tableActivePrefix, user_uid);
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

bool PgTransaction::ExpandChangesetBbox(int64_t cid,
	const std::vector<double> &bbox,
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
	int rowsAffected = DbExpandChangesetBbox(*dbconn, work.get(),
		this->tableActivePrefix,
		cid,
		bbox,
		errStrNative);

	if(rowsAffected < 0)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	return rowsAffected > 0;
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
	class PgChangesetQuery openQuery;
	openQuery.limit = 0; //Get all
	openQuery.openedBeforeTimestamp = whereBeforeTimestamp;
	openQuery.is_open_only = true;
	bool ok = GetChangesetsFromDb(*dbconn, work.get(),
		this->tableStaticPrefix, this->tableActivePrefix,
		this->dbUsernameLookup, 
		openQuery,
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

bool PgTransaction::GetEditActivityById(int64_t editActivityId,
	class EditActivity &editActivity,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	string errStrNative;
	std::string val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool found = DbGetEditActivityById(*dbconn, work.get(),
		this->tableActivePrefix,
		editActivityId,
		editActivity,
		errStrNative);

	errStr.errStr = errStrNative;
	return found;
}

std::pair<int64_t, int64_t> PgTransaction::GetLatestEditIds()
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work) throw runtime_error("Transaction has been deleted");
	std::pair<int64_t, int64_t> ids(0, 0);
	DbGetLatestEditIds(*dbconn, work.get(), this->tableActivePrefix, ids.first, ids.second);
	return ids;
}

std::map<std::string, std::string> PgTransaction::GetLatestEditIdAttribs()
{
	auto ids = this->GetLatestEditIds();
	return {{"edit_activity_id", to_string(ids.first)}, {"atomic_edit_id", to_string(ids.second)}};
}

void PgTransaction::QueryEditActivityByIds(int64_t firstId, int64_t lastId,
	int64_t atomicEditId, std::vector<std::shared_ptr<class EditActivity> > &editActivity,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	if(firstId < 0 || lastId < 0 || atomicEditId < 0 || (lastId > 0 && lastId < firstId))
		throw invalid_argument("Invalid activity ID range");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work) throw runtime_error("Transaction has been deleted");
	DbQueryEditActivityByIds(*dbconn, work.get(), this->tableActivePrefix,
		firstId, lastId, atomicEditId, editActivity, errStr.errStr);
}

void PgTransaction::QueryEditActivityByTimestamp(int64_t sinceTimestamp,
	int64_t untilTimestamp,
	std::vector<std::shared_ptr<class EditActivity> > &editActivity,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	string errStrNative;
	std::string val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbQueryEditActivityByTimestamp(*dbconn, work.get(),
		this->tableActivePrefix,
		sinceTimestamp, untilTimestamp,
		editActivity,
		errStrNative);

	errStr.errStr = errStrNative;
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

void PgTransaction::OverpassQuery(const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	std::shared_ptr<IDataStreamHandler> enc)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbOverpassQueryObjVisible(*dbconn, work.get(),
		this->dbUsernameLookup,
		this->tableActivePrefix,
		objType, filters, bbox, ids, limit, enc);
}

void PgTransaction::OverpassQueryIds(const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	std::vector<int64_t> &idsOut)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbOverpassQueryIdsVisible(*dbconn, work.get(),
		this->tableActivePrefix,
		objType, filters, bbox, ids, limit, idsOut);
}

void PgTransaction::SetStatementTimeout(int64_t milliseconds)
{
	if(milliseconds < 0)
		throw invalid_argument("Timeout must not be negative");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	// SET LOCAL lasts until the transaction ends, so nothing leaks to its next user
	work->exec("SET LOCAL statement_timeout = " + to_string(milliseconds));
}

void PgTransaction::XapiQuery(const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbXapiQueryVisible(*dbconn, work.get(), 
		this->dbUsernameLookup, 
		this->tableActivePrefix, 
		objType,
		tagKey,
		tagValue,
		bbox, 
		enc);
}

void PgTransaction::GetMostActiveUsers(int64_t startTimestamp,
	std::vector<int64_t> &uidOut,
	std::vector<std::vector<int64_t> > &objectCountOut)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbGetMostActiveUsers(*dbconn, work.get(), 
		this->tableActivePrefix, 
		startTimestamp,
		uidOut,
		objectCountOut);
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

	// Dropping everything does not need intermediate schema transformations.
	// Process dependent active views before the static tables they reference.
	for(const string &prefix : {this->tableTestPrefix, this->tableModPrefix, this->tableStaticPrefix})
	{
		if(!DbDropMapTables(*dbconn, work.get(), verbose, prefix, nativeErrStr))
		{
			errStr.errStr = nativeErrStr;
			return false;
		}
	}
	errStr.errStr.clear();
	bool ok = true;
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

bool PgAdmin::UpdateBboxes(int verbose, bool updateStatic, bool updateActive, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = true;

    if(updateStatic)
    {
		ok = DbUpdateWayBboxes(*dbconn, work.get(), verbose, 
			this->tableStaticPrefix, 
			this,
			nativeErrStr);
		errStr.errStr = nativeErrStr;
		if(!ok) return ok;
	}

	if (updateActive)
	{
		//Update way bboxes
		ok = DbUpdateWayBboxes(*dbconn, work.get(), verbose,
			this->tableModPrefix, 
			this,
			nativeErrStr);
		errStr.errStr = nativeErrStr;
		if(!ok) return ok;
	}

    if(updateStatic)
	{
		ok = DbUpdateRelationBboxes(*dbconn, work.get(), verbose, 
			this->tableStaticPrefix, 
			this,
			nativeErrStr);
		errStr.errStr = nativeErrStr;
		if(!ok) return ok;
	}

	if (updateActive)
	{
		//Update relation bboxes
		ok = DbUpdateRelationBboxes(*dbconn, work.get(), verbose,
			this->tableModPrefix, 
			this,
			nativeErrStr);
		errStr.errStr = nativeErrStr;
		if(!ok) return ok;
	}

	work->commit();

	return true;
}

bool PgAdmin::CreateBboxIndices(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbCreateBboxIndices(*dbconn, work.get(), verbose, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateBboxIndices(*dbconn, work.get(), verbose, this->tableModPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateBboxIndices(*dbconn, work.get(), verbose, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::DropBboxIndices(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbDropBboxIndices(*dbconn, work.get(), verbose, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbDropBboxIndices(*dbconn, work.get(), verbose, this->tableModPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbDropBboxIndices(*dbconn, work.get(), verbose, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
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
	// Transactions share ownership of the connection, so tidy the prepared
	// statement record when the last owner lets go, not when PgMap does. A
	// later connection may be allocated at the same address.
	dbconn.reset(new pqxx::connection(connection), [](pqxx::connection *c) {
		forget_prepared(*c);
		delete c;
	});
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

#if PQXX_VERSION_MAJOR < 7
	dbconn->disconnect();
#endif
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
