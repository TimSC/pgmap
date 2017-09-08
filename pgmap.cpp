
#include "pgmap.h"
#include "db.h"
#include "util.h"
#include "cppo5m/OsmData.h"
#include <algorithm>
using namespace std;

PgMap::PgMap(const string &connection, const string &tableStaticPrefixIn, 
	const string &tableModifyPrefixIn) : dbconn(connection)
{
	connectionString = connection;
	this->tableStaticPrefix = tableStaticPrefixIn;
	this->tableModifyPrefix = tableModifyPrefixIn;
}

PgMap::~PgMap()
{
	dbconn.disconnect();
}

bool PgMap::Ready()
{
	return dbconn.is_open();
}

int PgMap::MapQuery(const vector<double> &bbox, unsigned int maxNodes, IDataStreamHandler &enc)
{
	enc.StoreIsDiff(false);
	enc.StoreBounds(bbox[0], bbox[1], bbox[2], bbox[3]);

	//Lock database for reading (this must always be done in a set order)
	pqxx::work work(dbconn);
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relations IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "way_mems IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relation_mems_n IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relation_mems_w IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relation_mems_r IN ACCESS SHARE MODE;");

	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "relations IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "way_mems IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "relation_mems_n IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "relation_mems_w IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "relation_mems_r IN ACCESS SHARE MODE;");

	//Get nodes in bbox
	DataStreamRetainIds retainNodeIds(enc);
	if(maxNodes == 0)
		GetLiveNodesInBbox(work, this->tableStaticPrefix, bbox, 0, retainNodeIds); 
	else
		GetLiveNodesInBbox(work, this->tableStaticPrefix, bbox, maxNodes+1, retainNodeIds); 
	cout << "Found " << retainNodeIds.nodeIds.size() << " nodes in bbox" << endl;
	if(maxNodes > 0 && retainNodeIds.nodeIds.size() > maxNodes)
	{
		work.commit();
		return -1; //Too many nodes
	}

	//Get way objects that reference these nodes
	class OsmData wayObjects;
	DataStreamRetainIds retainWayIds(wayObjects);
	DataStreamRetainMemIds retainWayMemIds(retainWayIds);
	GetLiveWaysThatContainNodes(work, this->tableStaticPrefix, retainNodeIds.nodeIds, retainWayMemIds);
	cout << "Ways depend on " << retainWayMemIds.nodeIds.size() << " nodes" << endl;

	//Identify extra node IDs to complete ways
	set<int64_t> extraNodes;
	std::set_difference(retainWayMemIds.nodeIds.begin(), retainWayMemIds.nodeIds.end(), 
		retainNodeIds.nodeIds.begin(), retainNodeIds.nodeIds.end(),
	    std::inserter(extraNodes, extraNodes.end()));
	cout << "num extraNodes " << extraNodes.size() << endl;
	if(maxNodes > 0 && retainNodeIds.nodeIds.size() + extraNodes.size() > maxNodes)
	{
		work.commit();
		return -1; //Too many nodes
	}

	//Get node objects to complete these ways
	GetLiveNodesById(work, this->tableStaticPrefix, extraNodes, enc);

	//Write ways to output
	enc.Reset();
	wayObjects.StreamTo(enc, false);

	//Get relations that reference any of the above nodes
	enc.Reset();
	set<int64_t> empty;
	DataStreamRetainIds retainRelationIds(enc);
	GetLiveRelationsForObjects(work, this->tableStaticPrefix, 
		'n', retainNodeIds.nodeIds, empty, retainRelationIds);
	GetLiveRelationsForObjects(work, this->tableStaticPrefix, 
		'n', extraNodes, retainRelationIds.relationIds, retainRelationIds);

	//Get relations that reference any of the above ways
	GetLiveRelationsForObjects(work, this->tableStaticPrefix, 
		'w', retainWayIds.wayIds, retainRelationIds.relationIds, retainRelationIds);
	cout << "found " << retainRelationIds.relationIds.size() << " relations" << endl;

	//Release database lock by finishing the transaction
	work.commit();

	enc.Finish();
	return 0;
}

void PgMap::Dump(bool onlyLiveData, IDataStreamHandler &enc)
{
	enc.StoreIsDiff(false);

	//Lock database for reading (this must always be done in a set order)
	pqxx::work work(dbconn);
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableStaticPrefix+ "relations IN ACCESS SHARE MODE;");

	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tableModifyPrefix+ "relations IN ACCESS SHARE MODE;");

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

