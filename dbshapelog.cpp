#include <algorithm>
#include <chrono>
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

bool DbUpdateWayBboxInTable(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const vector<int64_t> &wayIds,
	const vector<int64_t> &wayMaxTimestamps,
	const std::vector<std::vector<double> > &wayBboxes,
	int &affectedRowsOut,
	std::string &errStr)
{
	stringstream proc;
	proc << tablePrefix << "updatewaybbox" << wayIds.size();

	stringstream ss;

	//Based on https://stackoverflow.com/a/18799497/4288232
	ss << "UPDATE " << c.quote_name(tablePrefix+"liveways") << " AS t SET\n";
	ss << "	bbox = c.column_a,\n";
	ss << "	bbox_timestamp = CAST(c.column_c AS BIGINT)\n";
	ss << "from (values\n";
	int64_t p = 1;
	for(size_t i=0; i<wayIds.size(); i++)
	{
		if (i>0) ss << ",";
		ss << "	($"<<p<<",ST_MakeEnvelope($"<<(p+1)<<",$"<<(p+2)<<",$"<<(p+3)<<",$"<<(p+4)<<",4326),$"<<(p+5)<<")\n";
		p += 6;
	}
	ss << ") as c(column_b, column_a, column_c) \n";
	ss << "where CAST(c.column_b AS BIGINT) = t.id;\n";

	affectedRowsOut = 0;

	try
	{
		c.prepare(proc.str(), ss.str());

		pqxx::prepare::invocation invoc = work->prepared(proc.str());

		for(size_t i=0; i<wayIds.size(); i++)
		{
			int64_t maxTimestamp = wayMaxTimestamps[i];
			const std::vector<double> &bbox = wayBboxes[i];

			invoc(wayIds[i]);

			invoc(bbox[0]);
			invoc(bbox[1]);
			invoc(bbox[2]);
			invoc(bbox[3]);

			invoc(maxTimestamp);
		}

		pqxx::result result = invoc.exec();
		affectedRowsOut = result.affected_rows();
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

void DbGetAffectedWays(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const std::set<int64_t> &touchedNodeIds,
	const std::set<int64_t> &touchedWayIds,
	std::set<int64_t> &touchedWayIdsOut,
	std::map<int64_t, class OsmWay> &touchedWayIdsMapOut,
	std::string &errStr)
{
	//Get affected ways by modified nodes
	std::shared_ptr<class OsmData> affectedWays(new class OsmData());
	DbGetWaysForNodes(c, work,
		staticTablePrefix, 
		activeTablePrefix, 
		dbUsernameLookup,  
		touchedNodeIds, 
		affectedWays);

	// Get pre-edit ways
	std::shared_ptr<class OsmData> changedWays(new class OsmData());
	DbGetObjectsById(c, work,
		staticTablePrefix, activeTablePrefix, 
		dbUsernameLookup, "way", touchedWayIds, changedWays);

	//Merge way ID lists
	touchedWayIdsOut.clear();
	for(size_t i=0; i<affectedWays->ways.size(); i++)
	{
		const class OsmWay &way = affectedWays->ways[i];
		touchedWayIdsOut.insert(way.objId);
		touchedWayIdsMapOut[way.objId] = way;
	}
	for(size_t i=0; i<changedWays->ways.size(); i++)
	{
		const class OsmWay &way = changedWays->ways[i];
		touchedWayIdsOut.insert(way.objId);
		touchedWayIdsMapOut[way.objId] = way;
	}
}

bool DbLogWayShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	int64_t timestamp,
	const std::set<int64_t> &touchedNodeIds,
	const std::set<int64_t> &touchedWayIds,
	std::set<int64_t> &touchedWayIdsOut,
	std::string &errStr)
{
	//Get affected ways
	std::map<int64_t, class OsmWay> touchedWayIdsMap;
	DbGetAffectedWays(c, work, 
		staticTablePrefix, 
		activeTablePrefix, 
		dbUsernameLookup, 
		touchedNodeIds,
		touchedWayIds,
		touchedWayIdsOut,
		touchedWayIdsMap,
		errStr);

	std::vector<int64_t> touchedWayIdsLi(touchedWayIdsOut.begin(), touchedWayIdsOut.end());

	vector<int64_t> wayMaxTimestamps;
	std::vector<std::vector<int32_t> > wayMemNodeVers;
	std::vector<std::vector<double> > wayBboxes;
	for(size_t i=0; i<touchedWayIdsLi.size(); i++)
	{
		//Get way bbox
		const class OsmWay &way = touchedWayIdsMap[touchedWayIdsLi[i]];
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
		const class OsmWay &way = touchedWayIdsMap[touchedWayIdsLi[i]];
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

bool DbUpdateWayShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const std::set<int64_t> &touchedNodeIds,
	const std::set<int64_t> &touchedWayIds,
	std::set<int64_t> &touchedWayIdsOut,
	std::string &errStr)
{
	int verbose = 0;

	//Get affected ways
	auto start = chrono::steady_clock::now();

	std::map<int64_t, class OsmWay> touchedWayIdsMap;
	DbGetAffectedWays(c, work, 
		staticTablePrefix, 
		activeTablePrefix, 
		dbUsernameLookup, 
		touchedNodeIds,
		touchedWayIds,
		touchedWayIdsOut,
		touchedWayIdsMap,
		errStr);

	std::vector<int64_t> touchedWayIdsLi(touchedWayIdsOut.begin(), touchedWayIdsOut.end());

	auto end = chrono::steady_clock::now();
	auto diff = end - start;
	if(verbose > 0)
		cout << "get affected ways " << chrono::duration <double, milli> (diff).count() << " ms" << endl;

	//Extract relevant node IDs
	start = chrono::steady_clock::now();
	set<int64_t> relevantNodeIds;
	for(size_t i=0; i<touchedWayIdsLi.size(); i++)
	{
		//Get way bbox
		const class OsmWay &way = touchedWayIdsMap[touchedWayIdsLi[i]];
		set<int64_t> refIds = way.GetRefIds();
		relevantNodeIds.insert(refIds.begin(), refIds.end());
	}

	//Get all relevant nodes
	std::shared_ptr<class OsmData> memNodes(new class OsmData());
	DbGetObjectsById(c, work,
		staticTablePrefix, activeTablePrefix, 
		dbUsernameLookup, "node", relevantNodeIds, memNodes);

	map<int64_t, const class OsmNode *> nodeMap;
	for(size_t i=0; i<memNodes->nodes.size(); i++)
	{
		const class OsmNode &n = memNodes->nodes[i];
		nodeMap[n.objId] = &n;
	}

	end = chrono::steady_clock::now();
	diff = end - start;
	if(verbose > 0)
		cout << "get "<< relevantNodeIds.size() <<" relevant nodes " << chrono::duration <double, milli> (diff).count() << " ms" << endl;

	//Get bbox of nodes in each way
	start = chrono::steady_clock::now();

	vector<int64_t> wayMaxTimestamps;
	std::vector<std::vector<double> > wayBboxes;
	for(size_t i=0; i<touchedWayIdsLi.size(); i++)
	{
		//Get way bbox
		const class OsmWay &way = touchedWayIdsMap[touchedWayIdsLi[i]];
		int64_t maxTimestamp = way.metaData.timestamp;
		std::vector<int32_t> memNodeVers;
		std::vector<double> bbox;

		std::vector<class OsmNode> wayNodes;
		for(size_t j=0; j<way.refs.size(); j++)
		{
			auto it = nodeMap.find(way.refs[j]);
			if(it == nodeMap.end() or it->second == nullptr)
			{
				cout << "Missing node " << way.refs[j] << "?" << endl;
				continue;
			}
			const class OsmNode &n = *it->second;
			memNodeVers.push_back(n.metaData.version);
			if(n.metaData.timestamp > maxTimestamp)
				maxTimestamp = n.metaData.timestamp;
			wayNodes.push_back(n);
		}
		CalcBboxForNodes(wayNodes, bbox);

		wayMaxTimestamps.push_back(maxTimestamp);
		wayBboxes.push_back(bbox);
	}

	end = chrono::steady_clock::now();
	diff = end - start;
	if(verbose > 0)
		cout << "get bbox from node objs " << chrono::duration <double, milli> (diff).count() << " ms" << endl;

	bool ocdnSupported = true;
	string ocdn;
	DbCheckOcdnSupport(c, work, ocdnSupported, ocdn);

	if(activeTablePrefix != "")
	{
		//Move the object to the active table in all cases
		DbCheckAndCopyObjectsToActiveTable(c, work, 
			staticTablePrefix, 
			activeTablePrefix, 
			dbUsernameLookup,
			"way", touchedWayIdsOut, 
			ocdnSupported,
			ocdn,
			errStr, 0);
	}

	//Update way shape in appropriate table
	string insertTable = activeTablePrefix;
	if(activeTablePrefix == "")
		insertTable = staticTablePrefix;

	start = chrono::steady_clock::now();

	int affectedRows = 0;
	bool ok = DbUpdateWayBboxInTable(c, work, 
		insertTable,
		dbUsernameLookup,
		touchedWayIdsLi, wayMaxTimestamps, wayBboxes, affectedRows, errStr);
	if (!ok) return false;

	end = chrono::steady_clock::now();
	diff = end - start;
	if(verbose > 0)
		cout << "write "<< touchedWayIdsLi.size() << " bboxes " << chrono::duration <double, milli> (diff).count() << " ms" << endl;

	return true;
}

// *****************************************************************************************

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

bool DbUpdateRelationBboxInTable(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const class OsmRelation &relation,
	int64_t maxTimestamp,
	bool has_bbox,
	const std::vector<double> &bbox,
	int &affectedRowsOut,
	std::string &errStr)
{
	affectedRowsOut = 0;
	try
	{
		if(has_bbox)
		{
			stringstream ss;
			ss << "UPDATE "<< c.quote_name(tablePrefix+"liverelations") + " SET bbox = ST_MakeEnvelope($1,$2,$3,$4,4326), bbox_timestamp = $5";
			ss << " WHERE id=$6;";

			c.prepare(tablePrefix+"updaterelbbox", ss.str());

			pqxx::prepare::invocation invoc = work->prepared(tablePrefix+"updaterelbbox");
			invoc(bbox[0]);
			invoc(bbox[1]);
			invoc(bbox[2]);
			invoc(bbox[3]);

			invoc(maxTimestamp);

			invoc(relation.objId);
			pqxx::result result = invoc.exec();
			affectedRowsOut = result.affected_rows();
		}
		else
		{
			stringstream ss;
			ss << "UPDATE "<< c.quote_name(tablePrefix+"liverelations") + " SET bbox = NULL, bbox_timestamp = $1";
			ss << " WHERE id=$2;";

			c.prepare(tablePrefix+"updaterelbbox2", ss.str());

			pqxx::prepare::invocation invoc = work->prepared(tablePrefix+"updaterelbbox2");

			invoc(maxTimestamp);
			invoc(relation.objId);
			pqxx::result result = invoc.exec();
			affectedRowsOut = result.affected_rows();
		}
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

void DbGetAffectedRelations(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const std::set<int64_t> &touchedNodeIds,
	const std::set<int64_t> &touchedWayIds,
	const std::set<int64_t> &touchedRelationIds,
	std::shared_ptr<class OsmData> affectedRelations,
	std::string &errStr)
{
	//Get affected relations by modified nodes
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
	//Get affected relations
	std::shared_ptr<class OsmData> affectedRelations(new class OsmData());
	DbGetAffectedRelations(c, work, 
			staticTablePrefix, 
			activeTablePrefix, 
			dbUsernameLookup, 
			touchedNodeIds,
			touchedWayIds,
			touchedRelationIds,
			affectedRelations,
			errStr);

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

bool DbUpdateRelationShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	const std::set<int64_t> &touchedNodeIds,
	const std::set<int64_t> &touchedWayIds,
	const std::set<int64_t> &touchedRelationIds,
	std::string &errStr)
{
	//Get affected relations
	std::shared_ptr<class OsmData> affectedRelations(new class OsmData());
	DbGetAffectedRelations(c, work, 
			staticTablePrefix, 
			activeTablePrefix, 
			dbUsernameLookup, 
			touchedNodeIds,
			touchedWayIds,
			touchedRelationIds,
			affectedRelations,
			errStr);

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

	bool ocdnSupported = true;
	string ocdn;
	DbCheckOcdnSupport(c, work, ocdnSupported, ocdn);

	if(activeTablePrefix != "")
	{
		//Move the object to the active table
		DbCheckAndCopyObjectsToActiveTable(c, work, 
			staticTablePrefix, 
			activeTablePrefix, 
			dbUsernameLookup,
			"relation", affectedRelations->GetRelationIds(), 
			ocdnSupported,
			ocdn,
			errStr, 0);
	}

	//Update way shape in appropriate table
	string insertTable = activeTablePrefix;
	if(activeTablePrefix == "")
		insertTable = staticTablePrefix;

	for(size_t i=0; i<affectedRelations->relations.size(); i++)	{

		class OsmRelation &rel = affectedRelations->relations[i];
		int64_t maxTimestamp = relMaxTimestamps[i];
		const std::vector<double> &bbox = relBboxes[i];
		bool has_bbox = relHasBbox[i];

		//Update bbox in active table
		int affectedRows = 0;
		bool ok = DbUpdateRelationBboxInTable(c, work, 
			insertTable, dbUsernameLookup,
			rel, maxTimestamp, has_bbox, bbox, affectedRows, errStr);
		if (!ok) return false;
	}

	return true;
}

