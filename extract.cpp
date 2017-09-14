#include <fstream>
#include <iostream>
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/OsmData.h"
#include "cppo5m/osmxml.h"
#include "pgmap.h"

int main(int argc, char **argv)
{	
	std::filebuf outfi;

#if 0
	outfi.open("extract.o5m.gz", std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);

	shared_ptr<IDataStreamHandler> enc (new O5mEncode(*gzipEnc));
#else
	outfi.open("extract.osm.gz", std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);

	shared_ptr<IDataStreamHandler> enc(new OsmXmlEncode(*gzipEnc));
#endif

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
	
	class PgMap pgMap(ss.str(), config["dbtableprefix"], config["dbtablemodifyprefix"]);

	if (pgMap.Ready()) {
		cout << "Opened database successfully" << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}

	vector<double> bbox = {-1.1473846,50.7360206,-0.9901428,50.8649113};

	int ret = pgMap.pgMapQuery.Start(bbox, enc);
	while(ret == 0)
	{
		ret = pgMap.pgMapQuery.Continue();
	}

	delete gzipEnc;
	outfi.close();
	return 0;
}
