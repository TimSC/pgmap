#include <fstream>
#include <iostream>
#include <algorithm>
#include "common.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/OsmData.h"

int main(int argc, char **argv)
{	
	std::filebuf outfi;
	outfi.open("extract.o5m.gz", std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);

	O5mEncode enc(*gzipEnc);
	enc.StoreIsDiff(false);

	string configContent;
	cout << "Reading settings from config.cfg" << endl;
	ReadFileContents("config.cfg", false, configContent);
	std::vector<std::string> lines = split(configContent, '\n');
	std::map<string, string> config;
	for(size_t i=0; i < lines.size(); i++)
	{
		const std::string &line = lines[i];
		std::vector<std::string> parts = split(line, ':');
		if (parts.size() < 2) continue;
		config[parts[0]] = parts[1];
	}
	
	std::stringstream ss;
	ss << "dbname=";
	ss << config["dbname"];
	ss << " user=";
	ss << config["dbuser"];
	ss << " password='";
	ss << config["dbpass"];
	ss << "' hostaddr=";
	ss << config["dbhost"];
	ss << " port=";
	ss << "5432";
	
	pqxx::connection dbconn(ss.str());
	if (dbconn.is_open()) {
		cout << "Opened database successfully: " << dbconn.dbname() << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}

	//Lock database for reading (this must always be done in a set order)
	pqxx::work work(dbconn);
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "relations IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "way_mems IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "relation_mems_n IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "relation_mems_w IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "relation_mems_r IN ACCESS SHARE MODE;");

	//Get nodes in bbox
	vector<double> bbox = {-1.1473846,50.7360206,-0.9901428,50.8649113};
	DataStreamRetainIds retainNodeIds(enc);
	GetLiveNodesInBbox(work, config, bbox, retainNodeIds); 
	cout << "Found " << retainNodeIds.nodeIds.size() << " nodes in bbox" << endl;

	//Get way objects that reference these nodes
	class OsmData wayObjects;
	DataStreamRetainIds retainWayIds(wayObjects);
	DataStreamRetainMemIds retainWayMemIds(retainWayIds);
	GetLiveWaysThatContainNodes(work, config, retainNodeIds.nodeIds, retainWayMemIds);
	cout << "Ways depend on " << retainWayMemIds.nodeIds.size() << " nodes" << endl;

	//Identify extra node IDs to complete ways
	set<int64_t> extraNodes;
	std::set_difference(retainWayMemIds.nodeIds.begin(), retainWayMemIds.nodeIds.end(), 
		retainNodeIds.nodeIds.begin(), retainNodeIds.nodeIds.end(),
	    std::inserter(extraNodes, extraNodes.end()));
	cout << "num extraNodes " << extraNodes.size() << endl;

	//Get node objects to complete these ways
	GetLiveNodesById(work, config, extraNodes, enc);

	//Write ways to output
	enc.Reset();
	wayObjects.StreamTo(enc, false);

	//Get relations that reference any of the above nodes
	enc.Reset();
	set<int64_t> empty;
	DataStreamRetainIds retainRelationIds(enc);
	GetLiveRelationsForObjects(work, config, 
		'n', retainNodeIds.nodeIds, empty, retainRelationIds);
	GetLiveRelationsForObjects(work, config, 
		'n', extraNodes, retainRelationIds.relationIds, retainRelationIds);

	//Get relations that reference any of the above ways
	GetLiveRelationsForObjects(work, config, 
		'w', retainWayIds.wayIds, retainRelationIds.relationIds, retainRelationIds);
	cout << "found " << retainRelationIds.relationIds.size() << " relations" << endl;

	//Release database lock by finishing the transaction
	work.commit();

	enc.Finish();

	delete gzipEnc;
	outfi.close();
	dbconn.disconnect ();
	return 0;
}
