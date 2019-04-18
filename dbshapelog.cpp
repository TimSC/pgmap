#include "dbstore.h"
#include "dbids.h"
#include "dbcommon.h"
#include "dbjson.h"
#include "dbquery.h"
using namespace std;

bool GetBboxForNodes(const std::vector<class OsmNode> &nodes, std::vector<double> &bboxOut)
{
	bboxOut.resize(4);

	if(nodes.size() == 0)
	{
		bboxOut = {0.0, 0.0, 0.0, 0.0};
		return false;
	}

	//bbox is the usual left,bottom,right,top format
	const class OsmNode &firstNode = nodes[0];
	bboxOut[0] = firstNode.lat;
	bboxOut[1] = firstNode.lon;
	bboxOut[2] = firstNode.lat;
	bboxOut[3] = firstNode.lon;

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

bool DbStoreWayInTable(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &usernames, 
	const class OsmWay &way,
	int64_t timestamp,
	std::string &errStr)
{
	//Get member node objects
	std::set<int64_t> memNodeIds = way.GetRefIds();

	std::shared_ptr<class OsmData> memNodes(new class OsmData());
	DbGetObjectsById(c, work,
		staticTablePrefix, activeTablePrefix, 
		usernames, "node", memNodeIds, memNodes);

	std::vector<int32_t> memNodeVers2;
	int64_t maxTimestamp = way.metaData.timestamp;
	for(size_t j=0; j<memNodes->nodes.size(); j++)
	{
		const class OsmNode &n = memNodes->nodes[j];
		memNodeVers2.push_back(n.metaData.version);
		if(n.metaData.timestamp > maxTimestamp)
			maxTimestamp = n.metaData.timestamp;
	}
	std::vector<double> bbox;
	bool ok = GetBboxForNodes(memNodes->nodes, bbox);
	if(!ok)
	{
		errStr = "Way has invalid bbox (not enough nodes?)";
		return false;
	}

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

		invoc(IntArrayToString(memNodeVers2));

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
	class DbUsernameLookup &usernames, 
	int64_t timestamp,
	const class OsmData &osmData,
	std::set<int64_t> &touchedWayIdsOut,
	std::string &errStr)
{
	//Get affected ways by modified nodes
	std::set<int64_t> nodeIds = osmData.GetNodeIds();

	std::shared_ptr<class OsmData> affectedWays(new class OsmData());
	GetLiveWaysThatContainNodes(c, work, usernames,
		staticTablePrefix, activeTablePrefix, nodeIds, affectedWays);

	GetLiveWaysThatContainNodes(c, work, usernames,
		activeTablePrefix, "", nodeIds, affectedWays);

	// Get pre-edit ways
	std::set<int64_t> wayIds = osmData.GetWayIds();

	std::shared_ptr<class OsmData> changedWays(new class OsmData());
	DbGetObjectsById(c, work,
		staticTablePrefix, activeTablePrefix, 
		usernames, "way", wayIds, changedWays);

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

	//Insert old way shape into log
	for(auto it=touchedWayIdsMap.begin(); it!=touchedWayIdsMap.end(); it++)
	{
		const class OsmWay &way = *it->second;
		cout << "way " << way.objId << " affected" << endl;

		bool ok = DbStoreWayInTable(c, work, 
			staticTablePrefix,
			activeTablePrefix, usernames,
			way, timestamp, errStr);
		if (!ok) return false;
	}

	return true;
}

bool DbLogRelationShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &usernames, 
	int64_t timestamp,
	const class OsmData &osmData,
	const std::set<int64_t> &touchedWayIds,
	std::string &errStr)
{

	//Get affected relations by modified nodes
	std::set<int64_t> nodeIds = osmData.GetNodeIds();

	std::shared_ptr<class OsmData> affectedRelations(new class OsmData());

	//Get affected relations by modified ways

	//Get pre-edit relations

	//Merge relations ID lists

	//Insert old way shape into log


	return true;
}

