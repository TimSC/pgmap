#include "dbstore.h"
#include "dbquery.h"
#include "dbcommon.h"
using namespace std;

bool DbRefreshWayShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tablePrefix, 
	const string &backingTablePrefix, 
	const std::set<int64_t> &objIds,
	std::string &errStr)
{
	bool ok = true;
	if(backingTablePrefix.size() > 0)
	{
		//Ensure way is in active table, so we can update it safely
		//First check existing rows in active table
		std::shared_ptr<class OsmData> existingObjs(new class OsmData());
		std::set<int64_t>::const_iterator it = objIds.begin();
		while(it != objIds.end())
			GetLiveWaysById(c, work, tablePrefix, "", objIds, 
				it, 1000, existingObjs);
		std::set<int64_t> existingIds;
		for(size_t i=0; i<existingObjs->ways.size(); i++)
			existingIds.insert(existingObjs->ways[i].objId);

		//Identify missing ways
		std::set<int64_t> missingIds;
		it = objIds.begin();
		while(it != objIds.end())
			if(existingIds.find(*it) == existingIds.end())
				missingIds.insert(*it);

		//Transfer missing ways from static to active table
		std::shared_ptr<class OsmData> transferObjs(new class OsmData());
		it = missingIds.begin();
		while(it != missingIds.end())
			GetLiveWaysById(c, work, backingTablePrefix, "", missingIds, 
				it, 1000, transferObjs);

		std::map<int64_t, int64_t> createdNodeIds, createdWayIds, createdRelationIds;
		ok = StoreObjects(c, work, 
			tablePrefix, 
			*transferObjs.get(), createdNodeIds, createdWayIds, createdRelationIds,
			errStr);
		if(!ok) return false;
	}

	//Refresh shapes of specified ways in active table
	//First get ways with their associated nodes
	std::shared_ptr<class OsmData> fullObjs(new class OsmData());
	DbGetFullObjectById(c, work,
		"way", objIds, 
		tablePrefix, backingTablePrefix, fullObjs);
	
	std::map<int64_t, class OsmNode *> nodes;
	std::map<int64_t, class OsmWay *> ways;
	std::map<int64_t, class OsmRelation *> relations;
	IndexObjects(*fullObjs.get(), nodes, ways, relations);

	//Calculate bounding boxes
	vector<vector<double> > bboxes;
	for(size_t i=0; i<fullObjs->ways.size(); i++)
	{
		class OsmWay &way = fullObjs->ways[i];
		std::set<int64_t> memberNodes;
		double left=0.0, bottom=0.0, right=0.0, top=0.0;
		bool bbox_set = false;
		for(size_t i=0; i<way.refs.size(); i++)
		{
			auto it = nodes.find(way.refs[i]);
			if(it == nodes.end()) continue; //Node not found: this shouldn't happen
			class OsmNode *node = it->second;
			
			if(node->lat < bottom || !bbox_set) bottom = node->lat;
			if(node->lat > top || !bbox_set) top = node->lat;
			if(node->lon < left || !bbox_set) left = node->lon;
			if(node->lon > right || !bbox_set) right = node->lon;
			bbox_set = true;
		}
		vector<double> bbox = {left, bottom, right, top};
		bboxes.push_back(bbox);
	}

	//Store updated way bbox in database
	//TODO

	return ok;
}

