#include <fstream>
#include <iostream>
#include <algorithm>
#include "common.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"

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

	//Get nodes in bbox
	vector<double> bbox = {-1.1473846,50.7360206,-0.9901428,50.8649113};
	DataStreamRetainIds retainIds(enc);
	GetNodesInBbox(dbconn, config, bbox, retainIds); 
	cout << "Found " << retainIds.nodeIds.size() << " nodes in bbox" << endl;

	//Get way objects that reference these nodes
	//TODO retain these ways in memory
	enc.Reset();
	DataStreamRetainMemIds retainMemIds(enc);
	GetWaysThatContainNodes(dbconn, config, retainIds.nodeIds, retainMemIds);
	cout << "Ways depend on " << retainMemIds.nodeIds.size() << " nodes" << endl;

	//Identify extra node IDs to complete ways
	set<int64_t> extraNodes;
	std::set_difference(retainMemIds.nodeIds.begin(), retainMemIds.nodeIds.end(), 
		retainIds.nodeIds.begin(), retainIds.nodeIds.end(),
	    std::inserter(extraNodes, extraNodes.end()));
	cout << "num extraNodes " << extraNodes.size() << endl;

	//Get node objects to complete these ways

/*
	enc.Reset();
		
	DumpRelations(dbconn, config, enc);
*/
	enc.Finish();

	delete gzipEnc;
	outfi.close();
	dbconn.disconnect ();
	return 0;
}
