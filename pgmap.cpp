
#include "pgmap.h"
#include "db.h"
#include "util.h"
#include "cppo5m/OsmData.h"
#include <algorithm>
using namespace std;

PgMap::PgMap(const string &connection, const string &tablePrefixIn) : dbconn(connection)
{
	tablePrefix = tablePrefixIn;
}

PgMap::~PgMap()
{
	dbconn.disconnect();
}

bool PgMap::Ready()
{
	return dbconn.is_open();
}

void PgMap::MapQuery(const vector<double> &bbox, IDataStreamHandler &enc)
{
	enc.StoreIsDiff(false);

	//Lock database for reading (this must always be done in a set order)
	pqxx::work work(dbconn);
	work.exec("LOCK TABLE "+this->tablePrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tablePrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tablePrefix+ "relations IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tablePrefix+ "way_mems IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tablePrefix+ "relation_mems_n IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tablePrefix+ "relation_mems_w IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tablePrefix+ "relation_mems_r IN ACCESS SHARE MODE;");

	//Get nodes in bbox
	DataStreamRetainIds retainNodeIds(enc);
	GetLiveNodesInBbox(work, this->tablePrefix, bbox, retainNodeIds); 
	cout << "Found " << retainNodeIds.nodeIds.size() << " nodes in bbox" << endl;

	//Get way objects that reference these nodes
	class OsmData wayObjects;
	DataStreamRetainIds retainWayIds(wayObjects);
	DataStreamRetainMemIds retainWayMemIds(retainWayIds);
	GetLiveWaysThatContainNodes(work, this->tablePrefix, retainNodeIds.nodeIds, retainWayMemIds);
	cout << "Ways depend on " << retainWayMemIds.nodeIds.size() << " nodes" << endl;

	//Identify extra node IDs to complete ways
	set<int64_t> extraNodes;
	std::set_difference(retainWayMemIds.nodeIds.begin(), retainWayMemIds.nodeIds.end(), 
		retainNodeIds.nodeIds.begin(), retainNodeIds.nodeIds.end(),
	    std::inserter(extraNodes, extraNodes.end()));
	cout << "num extraNodes " << extraNodes.size() << endl;

	//Get node objects to complete these ways
	GetLiveNodesById(work, this->tablePrefix, extraNodes, enc);

	//Write ways to output
	enc.Reset();
	wayObjects.StreamTo(enc, false);

	//Get relations that reference any of the above nodes
	enc.Reset();
	set<int64_t> empty;
	DataStreamRetainIds retainRelationIds(enc);
	GetLiveRelationsForObjects(work, this->tablePrefix, 
		'n', retainNodeIds.nodeIds, empty, retainRelationIds);
	GetLiveRelationsForObjects(work, this->tablePrefix, 
		'n', extraNodes, retainRelationIds.relationIds, retainRelationIds);

	//Get relations that reference any of the above ways
	GetLiveRelationsForObjects(work, this->tablePrefix, 
		'w', retainWayIds.wayIds, retainRelationIds.relationIds, retainRelationIds);
	cout << "found " << retainRelationIds.relationIds.size() << " relations" << endl;

	//Release database lock by finishing the transaction
	work.commit();

	enc.Finish();
}

void PgMap::Dump(bool onlyLiveData, IDataStreamHandler &enc)
{
	enc.StoreIsDiff(false);

	//Lock database for reading (this must always be done in a set order)
	pqxx::work work(dbconn);
	work.exec("LOCK TABLE "+this->tablePrefix+ "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tablePrefix+ "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+this->tablePrefix+ "relations IN ACCESS SHARE MODE;");

	DumpNodes(work, this->tablePrefix, onlyLiveData, enc);

	enc.Reset();

	DumpWays(work, this->tablePrefix, onlyLiveData, enc);

	enc.Reset();
		
	DumpRelations(work, this->tablePrefix, onlyLiveData, enc);

	//Release locks
	work.commit();

	enc.Finish();

}

