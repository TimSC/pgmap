
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

void PgTransaction::GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(objectIds.size()==0)
		return;

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
	std::shared_ptr<IDataStreamHandler> out)
{
	GetLiveWaysThatContainNodes(work.get(), this->tableStaticPrefix, this->tableActivePrefix, objectIds, out);

	GetLiveWaysThatContainNodes(work.get(), this->tableActivePrefix, "", objectIds, out);
}

void PgTransaction::GetRelationsForObjs(const std::string &type, const std::set<int64_t> &objectIds, 
	std::shared_ptr<IDataStreamHandler> out)
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

void PgTransaction::GetAffectedObjects(std::shared_ptr<class OsmData> inputObjects,
	std::shared_ptr<class OsmData> outAffectedObjects)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	//Modified nodes are considered to be affected
	outAffectedObjects->nodes = inputObjects->nodes;
	outAffectedObjects->ways = inputObjects->ways;
	outAffectedObjects->relations = inputObjects->relations;

	//Get IDs of objects already known
	std::set<int64_t> affectedNodeIds;
	outAffectedObjects->nodes = inputObjects->nodes;
	for(size_t i=0; i<outAffectedObjects->nodes.size(); i++)
		affectedNodeIds.insert(outAffectedObjects->nodes[i].objId);

	std::set<int64_t> parentWayIds;
	for(size_t i=0; i<inputObjects->ways.size(); i++)
		parentWayIds.insert(inputObjects->ways[i].objId);

	std::set<int64_t> parentRelationIds;
	for(size_t i=0; i<inputObjects->relations.size(); i++)
		parentRelationIds.insert(inputObjects->relations[i].objId);

	//For all nodes affected, find all parent ways and relations affected
	std::shared_ptr<class OsmData> parentObjs(new class OsmData());
	GetWaysForNodes(affectedNodeIds, parentObjs);
	for(size_t i=0; i<parentObjs->ways.size(); i++)
	{
		class OsmWay &way = parentObjs->ways[i];
		auto it = parentWayIds.find(way.objId);
		if(it == parentWayIds.end())
		{
			parentWayIds.insert(way.objId);
			outAffectedObjects->ways.push_back(way);
		}
	}

	//For all ways affected, find all parent relations affected
	GetRelationsForObjs("node", affectedNodeIds, parentObjs);
	GetRelationsForObjs("way", parentWayIds, parentObjs);
	for(size_t i=0; i<parentObjs->relations.size(); i++)
	{
		class OsmRelation &relation = parentObjs->relations[i];
		auto it = parentRelationIds.find(relation.objId);
		if(it == parentRelationIds.end())
		{
			parentRelationIds.insert(relation.objId);
			outAffectedObjects->relations.push_back(relation);
		}
	}

	//For all relations affected, recusively find parent relations
	std::set<int64_t> currentRelationIds = parentRelationIds;
	int depth = 0;
	while(currentRelationIds.size() > 0 and depth < 10)
	{
		std::shared_ptr<class OsmData> parentRelationObjs(new class OsmData());
		GetRelationsForObjs("relation", currentRelationIds, parentRelationObjs);
		
		currentRelationIds.clear();
		for(size_t i=0; i<parentRelationObjs->relations.size(); i++)
		{
			class OsmRelation &relation = parentRelationObjs->relations[i];
			auto it = parentRelationIds.find(relation.objId);
			if(it == parentRelationIds.end())
			{
				outAffectedObjects->relations.push_back(relation);
				currentRelationIds.insert(relation.objId);
				parentRelationIds.insert(relation.objId);
			}
		}
		depth ++;
	}

	//For affected parents, find all referenced (child) objects, starting 
	//with relations, then ways, then nodes.

	std::vector<class OsmRelation> currentRelations = outAffectedObjects->relations;
	depth = 0;
	while(currentRelations.size() > 0 and depth < 10)
	{
		std::set<int64_t> memberRelationIds;
		for(size_t i=0; i<currentRelations.size(); i++)
		{
			class OsmRelation &relation = currentRelations[i];

			//Iterate over members, find relations
			for(size_t j=0;j<relation.refTypeStrs.size(); j++)
			{		
				if(relation.refTypeStrs[j] != "relation")
					continue;
			
				auto it = parentRelationIds.find(relation.refIds[j]);
				if(it != parentRelationIds.end())
					continue; //Relation already known
				memberRelationIds.insert(relation.refIds[j]);
			}
		}

		std::shared_ptr<class OsmData> newMemberObjs(new class OsmData());
		GetObjectsById("relation", memberRelationIds, newMemberObjs);
		for(size_t i=0; i<newMemberObjs->relations.size(); i++)
		{
			class OsmRelation &relation = newMemberObjs->relations[i];
			parentRelationIds.insert(relation.objId);
			outAffectedObjects->relations.push_back(relation);
		}
		currentRelations = newMemberObjs->relations;

		depth ++;
	}

	//For affected relations, get all way members
	std::set<int64_t> memberWayIds;
	for(size_t i=0; i<outAffectedObjects->relations.size(); i++)
	{
		class OsmRelation &relation = outAffectedObjects->relations[i];

		//Iterate over members, find ways
		for(size_t j=0;j<relation.refTypeStrs.size(); j++)
		{		
			if(relation.refTypeStrs[j] != "way")
				continue;
		
			auto it = parentWayIds.find(relation.refIds[j]);
			if(it != parentWayIds.end())
				continue; //Way already known
			memberWayIds.insert(relation.refIds[j]);
		}
	}

	std::shared_ptr<class OsmData> newMemberObjs(new class OsmData());
	GetObjectsById("way", memberWayIds, newMemberObjs);
	for(size_t i=0; i<newMemberObjs->ways.size(); i++)
	{
		class OsmWay &way = newMemberObjs->ways[i];
		parentWayIds.insert(way.objId);
		outAffectedObjects->ways.push_back(way);
	}

	//For affected relations, get all member node IDs
	std::set<int64_t> memberNodeIds;
	for(size_t i=0; i<currentRelations.size(); i++)
	{
		class OsmRelation &relation = currentRelations[i];

		//Iterate over relation members, find nodes
		for(size_t j=0;j<relation.refTypeStrs.size(); j++)
		{		
			if(relation.refTypeStrs[j] != "node")
				continue;
		
			auto it = affectedNodeIds.find(relation.refIds[j]);
			if(it != affectedNodeIds.end())
				continue; //Way already known
			memberNodeIds.insert(relation.refIds[j]);
		}
	}

	//For affected ways, get member node IDs 
	for(size_t i=0; i<outAffectedObjects->ways.size(); i++)
	{
		class OsmWay &way = outAffectedObjects->ways[i];
		//Iterate over way members, find nodes
		for(size_t j=0;j<way.refs.size(); j++)
		{
			auto it = affectedNodeIds.find(way.refs[j]);
			if(it != affectedNodeIds.end())
				continue; //Node already known
			memberNodeIds.insert(way.refs[j]);
		}
	}
	GetObjectsById("node", memberNodeIds, outAffectedObjects);
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

void PgTransaction::GetReplicateDiff(int64_t timestampStart, int64_t timestampEnd, std::shared_ptr<IDataStreamHandler> enc)
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

void PgTransaction::Dump(bool order, std::shared_ptr<IDataStreamHandler> enc)
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
	int ret = GetChangesetFromDb(work.get(),
		this->tableActivePrefix,
		objId,
		changesetOut,
		errStrNative);

	if(ret == -1)
	{
		ret = GetChangesetFromDb(work.get(),
			this->tableStaticPrefix,
			objId,
			changesetOut,
			errStrNative);
	}

	errStr.errStr = errStrNative;
	return ret;
}

bool PgTransaction::GetChangesets(std::vector<class PgChangeset> &changesetsOut,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	string errStrNative;
	size_t targetNum = 100;
	bool ok = GetChangesetsFromDb(work.get(),
		this->tableActivePrefix, "",
		targetNum,
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
		ok = GetChangesetsFromDb(work.get(),
			this->tableStaticPrefix,
			this->tableActivePrefix,
			targetNum - changesetsOut.size(),
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

	class PgChangeset changesetMod(changeset);
	if(changesetMod.objId != 0)
		throw invalid_argument("Changeset ID should be zero since it is allocated by pgmap");

	int64_t cid = this->GetAllocatedId("changeset");
	changesetMod.objId = cid;
	string errStrNative;	

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
	size_t rowsAffected = 0;

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

PgAdmin::PgAdmin(shared_ptr<pqxx::connection> dbconnIn,
		const string &tableStaticPrefixIn, 
		const string &tableModPrefixIn,
		const string &tableTestPrefixIn,
		std::shared_ptr<pqxx::transaction_base> workIn):
	work(workIn)
{
	dbconn = dbconnIn;
	tableStaticPrefix = tableStaticPrefixIn;
	tableModPrefix = tableModPrefixIn;
	tableTestPrefix = tableTestPrefixIn;
	string errStr;
}

PgAdmin::~PgAdmin()
{
	work.reset();
}

bool PgAdmin::CreateMapTables(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;

	bool ok = DbCreateTables(*dbconn, work.get(), verbose, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateTables(*dbconn, work.get(), verbose, this->tableModPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateTables(*dbconn, work.get(), verbose, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::DropMapTables(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;

	bool ok = DbDropTables(*dbconn, work.get(), verbose, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbDropTables(*dbconn, work.get(), verbose, this->tableModPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbDropTables(*dbconn, work.get(), verbose, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::CopyMapData(int verbose, const std::string &filePrefix, class PgMapError &errStr)
{
	std::string nativeErrStr;

	bool ok = DbCopyData(*dbconn, work.get(), verbose, filePrefix, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::CreateMapIndices(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;

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

bool PgAdmin::RefreshMapIds(int verbose, class PgMapError &errStr)
{
	ClearNextIdValues(work.get(), this->tableStaticPrefix);
	ClearNextIdValues(work.get(), this->tableModPrefix);
	ClearNextIdValues(work.get(), this->tableTestPrefix);
	
	std::string nativeErrStr;

	bool ok = DbRefreshMaxIds(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

void PgAdmin::Commit()
{
	//Release locks
	work->commit();
}

void PgAdmin::Abort()
{
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

std::shared_ptr<class PgAdmin> PgMap::GetAdmin()
{
	dbconn->cancel_query();
	std::shared_ptr<pqxx::nontransaction> work(new pqxx::nontransaction(*dbconn));
	shared_ptr<class PgAdmin> out(new class PgAdmin(dbconn, tableStaticPrefix, tableModPrefix, tableTestPrefix, work));
	return out;
}

