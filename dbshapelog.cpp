#include <algorithm>
#include "dbstore.h"
#include "dbids.h"
#include "dbcommon.h"
#include "dbjson.h"
#include "dbquery.h"
using namespace std;

bool CalcBboxForNodes(const std::vector<class OsmNode> &nodes, std::vector<double> &bboxOut)
{
	bboxOut.resize(4);

	if(nodes.size() == 0)
	{
		bboxOut.clear();
		return false;
	}

	//bbox is the usual left,bottom,right,top format
	const class OsmNode &firstNode = nodes[0];
	bboxOut[0] = firstNode.lon;
	bboxOut[1] = firstNode.lat;
	bboxOut[2] = firstNode.lon;
	bboxOut[3] = firstNode.lat;

	for(size_t i=1; i<nodes.size(); i++)
	{
		const class OsmNode &node = nodes[i];
		if(node.lon < bboxOut[0]) bboxOut[0] = node.lon;
		if(node.lat < bboxOut[1]) bboxOut[1] = node.lat;
		if(node.lon > bboxOut[2]) bboxOut[2] = node.lon;
		if(node.lat > bboxOut[3]) bboxOut[3] = node.lat;
	}

	return true;
}

template<typename T> std::string IntArrayToString(std::vector<T> arr)
{
	std::string out="{";
	for(size_t i=0; i<arr.size(); i++)
	{
		if(i>0) out += ",";
		out += std::to_string(arr[i]);
	}
	out += "}";
	return out;
}

bool DbGetNodeIdsBbox(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const std::set<int64_t> &memNodeIds,
	int64_t &maxTimestamp,
	std::vector<int32_t> &memNodeVers,
	std::vector<double> &bbox)
{
	//Get node objects
	std::shared_ptr<class OsmData> memNodes(new class OsmData());
	DbGetObjectsById(c, work,
		staticTablePrefix, activeTablePrefix, 
		dbUsernameLookup, "node", memNodeIds, memNodes);

	for(size_t j=0; j<memNodes->nodes.size(); j++)
	{
		const class OsmNode &n = memNodes->nodes[j];
		memNodeVers.push_back(n.metaData.version);
		if(n.metaData.timestamp > maxTimestamp)
			maxTimestamp = n.metaData.timestamp;
	}
	bool ok = CalcBboxForNodes(memNodes->nodes, bbox);
	return ok;
}

bool DbStoreWayShapeInTable(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const class OsmWay &way,
	int64_t maxTimestamp,
	int64_t timestamp,
	const std::vector<int32_t> &memNodeVers,
	const std::vector<double> &bbox,
	std::string &errStr)
{
	stringstream ss;
	ss << "INSERT INTO "<< c.quote_name(activeTablePrefix+"wayshapes") + " (id, way_id, way_version, start_timestamp, end_timestamp, nvers, bbox) VALUES ";
	ss << "(DEFAULT,$1,$2,$3,$4,$5,ST_MakeEnvelope($6,$7,$8,$9,4326));";

	try
	{
		c.prepare(activeTablePrefix+"insertwayshape", ss.str());

		pqxx::prepare::invocation invoc = work->prepared(activeTablePrefix+"insertwayshape");
		invoc(way.objId);
		invoc(way.metaData.version);
		invoc(maxTimestamp);
		invoc(timestamp);

		invoc(IntArrayToString(memNodeVers));

		invoc(bbox[0]);
		invoc(bbox[1]);
		invoc(bbox[2]);
		invoc(bbox[3]);

		invoc.exec();
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = e.what();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}
	return true;
}

bool DbLogWayShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	int64_t timestamp,
	const class OsmData &osmData,
	std::set<int64_t> &touchedWayIdsOut,
	std::string &errStr)
{
	//Get affected ways by modified nodes
	std::set<int64_t> touchedNodeIds = osmData.GetNodeIds();

	std::shared_ptr<class OsmData> affectedWays(new class OsmData());
	DbGetWaysForNodes(c, work,
		staticTablePrefix, 
		activeTablePrefix, 
		dbUsernameLookup,  
		touchedNodeIds, 
		affectedWays);

	// Get pre-edit ways
	std::set<int64_t> wayIds = osmData.GetWayIds();

	std::shared_ptr<class OsmData> changedWays(new class OsmData());
	DbGetObjectsById(c, work,
		staticTablePrefix, activeTablePrefix, 
		dbUsernameLookup, "way", wayIds, changedWays);

	//Merge way ID lists
	touchedWayIdsOut.clear();
	std::map<int64_t, const class OsmWay*> touchedWayIdsMap;
	for(size_t i=0; i<affectedWays->ways.size(); i++)
	{
		const class OsmWay &way = affectedWays->ways[i];
		touchedWayIdsOut.insert(way.objId);
		touchedWayIdsMap[way.objId] = &way;
	}
	for(size_t i=0; i<changedWays->ways.size(); i++)
	{
		const class OsmWay &way = changedWays->ways[i];
		touchedWayIdsOut.insert(way.objId);
		touchedWayIdsMap[way.objId] = &way;
	}

	std::vector<int64_t> touchedWayIdsLi(touchedWayIdsOut.begin(), touchedWayIdsOut.end());

	vector<int64_t> wayMaxTimestamps;
	std::vector<std::vector<int32_t> > wayMemNodeVers;
	std::vector<std::vector<double> > wayBboxes;
	for(size_t i=0; i<touchedWayIdsLi.size(); i++)
	{
		//Get way bbox
		const class OsmWay &way = *touchedWayIdsMap[touchedWayIdsLi[i]];
		int64_t maxTimestamp = way.metaData.timestamp;
		std::vector<int32_t> memNodeVers;
		std::vector<double> bbox;

		bool ok = DbGetNodeIdsBbox(c, work, 
			staticTablePrefix, 
			activeTablePrefix, 
			dbUsernameLookup, 
			way.GetRefIds(), maxTimestamp, memNodeVers, bbox);
		if(!ok)
		{
			errStr = "Way has invalid bbox (not enough nodes?)";
			return false;
		}

		wayMaxTimestamps.push_back(maxTimestamp);
		wayMemNodeVers.push_back(memNodeVers);
		wayBboxes.push_back(bbox);
	}

	//Insert old way shape into log
	for(size_t i=0; i<touchedWayIdsLi.size(); i++)
	{
		const class OsmWay &way = *touchedWayIdsMap[touchedWayIdsLi[i]];
		int64_t maxTimestamp = wayMaxTimestamps[i];
		const std::vector<int32_t> &memNodeVers = wayMemNodeVers[i];
		const std::vector<double> &bbox = wayBboxes[i];

		bool ok = DbStoreWayShapeInTable(c, work, 
			staticTablePrefix,
			activeTablePrefix, dbUsernameLookup,
			way, maxTimestamp, timestamp, memNodeVers, bbox, errStr);
		if (!ok) return false;
	}

	return true;
}

void DbGetNodesIdInRelationRecursive(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	int depth,
	std::set<int64_t> relIdsDone,
	const class OsmRelation &relation,
	std::set<int64_t> &nodeIdsOut)
{
	//Get member objects
	std::set<int64_t> memWayIds, memRelIds;
	for(size_t i=0; i<relation.refTypeStrs.size(); i++)
	{
		if(relation.refTypeStrs[i] == "node")
			nodeIdsOut.insert(relation.refIds[i]);
		else if(relation.refTypeStrs[i] == "way")
			memWayIds.insert(relation.refIds[i]);
		else if(relation.refTypeStrs[i] == "relation")
			memRelIds.insert(relation.refIds[i]);
	}

	//Get node IDs for ways
	std::shared_ptr<class OsmData> wayObjs(new class OsmData());
	DbGetObjectsById(c, work,
		staticTablePrefix, activeTablePrefix, 
		dbUsernameLookup, "way", memWayIds, wayObjs);
	for(size_t i=0; i<wayObjs->ways.size(); i++)
	{
		const class OsmWay &way = wayObjs->ways[i];
		std::set<int64_t> wayRefs = way.GetRefIds();
		nodeIdsOut.insert(wayRefs.begin(), wayRefs.end());
	}
	
	if(depth < 10)
	{
		//Prevent searching circular dependencies within relations
		relIdsDone.insert(relation.objId);
		std::set<int64_t> relsNotDone;
		std::set_difference(memRelIds.begin(), memRelIds.end(),
			relIdsDone.begin(), relIdsDone.end(),
			std::inserter(relsNotDone, relsNotDone.begin()));

		//Get member relation objects
		std::shared_ptr<class OsmData> memRelObjs(new class OsmData());
		DbGetObjectsById(c, work,
			staticTablePrefix, activeTablePrefix, 
			dbUsernameLookup, "relation", relsNotDone, memRelObjs);

		//Get node IDs for other relations
		for(size_t i=0; i<memRelObjs->relations.size(); i++)
		{
			const class OsmRelation &memrel = memRelObjs->relations[i];

			DbGetNodesIdInRelationRecursive(c, work, 
				staticTablePrefix, 
				activeTablePrefix, 
				dbUsernameLookup, 
				depth+1, relIdsDone,
				memrel,
				nodeIdsOut);
		}
	}
}

bool DbGetRelationBbox(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const class OsmRelation &relation,
	int64_t &maxTimestamp,
	std::vector<double> &bbox)
{
	maxTimestamp = relation.metaData.timestamp;

	std::set<int64_t> nodeIds;
	std::set<int64_t> relIdsDone;
	DbGetNodesIdInRelationRecursive(c, work, 
		staticTablePrefix, 
		activeTablePrefix, 
		dbUsernameLookup, 
		0, relIdsDone,
		relation,
		nodeIds);
	if(nodeIds.size()==0) return false;

	std::vector<int32_t> memNodeVers;
	bool ok = DbGetNodeIdsBbox(c, work, 
		staticTablePrefix, 
		activeTablePrefix, 
		dbUsernameLookup, 
		nodeIds, maxTimestamp, memNodeVers, bbox);

	return ok;
}

bool DbStoreRelationShapeInTable(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const class OsmRelation &relation,
	int64_t start_timestamp,
	int64_t timestamp,
	bool has_bbox,
	const std::vector<double> &bbox,
	std::string &errStr)
{
	try
	{
		stringstream ss;
		std::shared_ptr<pqxx::prepare::invocation> pinvoc;
		ss << "INSERT INTO "<< c.quote_name(activeTablePrefix+"relshapes") + " (rel_id, rel_version, start_timestamp, end_timestamp, bbox) VALUES ";
		if(has_bbox)
		{
			ss << "($1,$2,$3,$4,ST_MakeEnvelope($5,$6,$7,$8,4326));";
			c.prepare(activeTablePrefix+"insertrelshape1", ss.str());
			pinvoc = make_shared<pqxx::prepare::invocation>(work->prepared(activeTablePrefix+"insertrelshape1"));
		}
		else
		{
			ss << "($1,$2,$3,$4,null);";
			c.prepare(activeTablePrefix+"insertrelshape2", ss.str());
			pinvoc = make_shared<pqxx::prepare::invocation>(work->prepared(activeTablePrefix+"insertrelshape2"));
		}

		pqxx::prepare::invocation &invoc = *pinvoc.get();
		invoc(relation.objId);
		invoc(relation.metaData.version);
		invoc(start_timestamp);
		invoc(timestamp);

		if(has_bbox)
		{
			invoc(bbox[0]);
			invoc(bbox[1]);
			invoc(bbox[2]);
			invoc(bbox[3]);
		}

		invoc.exec();
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = e.what();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}

	return true;

}

bool DbLogRelationShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	int64_t timestamp,
	const std::set<int64_t> &touchedNodeIds,
	const std::set<int64_t> &touchedWayIds,
	const std::set<int64_t> &touchedRelationIds,
	std::string &errStr)
{
	//Get affected relations by modified nodes
	std::shared_ptr<class OsmData> affectedRelations(new class OsmData());
	DbGetRelationsForObjs(c, work,
		staticTablePrefix, 
		activeTablePrefix, 
		dbUsernameLookup,  
		"node", touchedNodeIds, 
		affectedRelations);

	//Get affected relations by modified ways
	DbGetRelationsForObjs(c, work,
		staticTablePrefix, 
		activeTablePrefix, 
		dbUsernameLookup,  
		"way", touchedWayIds, 
		affectedRelations);

	//Get pre-edit relations
	DbGetObjectsById(c, work,
		staticTablePrefix, activeTablePrefix, 
		dbUsernameLookup, "relation", touchedRelationIds, affectedRelations);

	//Get relation parents recursively
	std::set<int64_t> knownRelIds = affectedRelations->GetRelationIds();
	std::set<int64_t> activeRelIds = knownRelIds;
	int searchCount = 0;
	while(searchCount < 10 and activeRelIds.size()>0)
	{
		std::shared_ptr<class OsmData> parentRelations(new class OsmData());
		DbGetRelationsForObjs(c, work,
			staticTablePrefix, 
			activeTablePrefix, 
			dbUsernameLookup,  
			"relation", activeRelIds, 
			parentRelations);

		knownRelIds.insert(activeRelIds.begin(), activeRelIds.end());
		activeRelIds.clear();
		for(size_t i=0; i<parentRelations->relations.size(); i++)
		{
			const class OsmRelation &parentrel = parentRelations->relations[i];
			auto it = knownRelIds.find(parentrel.objId);
			if(it != knownRelIds.end())
				continue;

			//Found a new parent relation
			affectedRelations->relations.push_back(parentrel);
			activeRelIds.insert(parentrel.objId);
		}

		searchCount ++;
	}

	std::vector<int64_t> relMaxTimestamps;
	std::vector<std::vector<double> > relBboxes;
	std::vector<bool> relHasBbox;	
	for(size_t i=0; i<affectedRelations->relations.size(); i++)	{

		class OsmRelation &rel = affectedRelations->relations[i];

		//Get bbox for relation
		int64_t maxTimestamp = 0;
		std::vector<double> bbox;
		bool has_bbox = DbGetRelationBbox(c, work, 
			staticTablePrefix, 
			activeTablePrefix, 
			dbUsernameLookup, 
			rel,
			maxTimestamp,
			bbox);
		relMaxTimestamps.push_back(maxTimestamp);
		relBboxes.push_back(bbox);
		relHasBbox.push_back(has_bbox);
	}

	//Insert old relation shape into log
	for(size_t i=0; i<affectedRelations->relations.size(); i++)	{

		class OsmRelation &rel = affectedRelations->relations[i];
		int64_t maxTimestamp = relMaxTimestamps[i];
		const std::vector<double> &bbox = relBboxes[i];
		bool has_bbox = relHasBbox[i];

		bool ok = DbStoreRelationShapeInTable(c, work, 
			staticTablePrefix,
			activeTablePrefix, dbUsernameLookup,
			rel, maxTimestamp, timestamp, has_bbox, bbox, errStr);
		if (!ok) return false;
	}

	return true;
}

