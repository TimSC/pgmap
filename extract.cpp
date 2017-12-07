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

	cout << "Reading settings from config.cfg" << endl;
	std::map<string, string> config;
	ReadSettingsFile("config.cfg", config);
	
	string cstr = GeneratePgConnectionString(config);
	
	class PgMap pgMap(cstr, config["dbtableprefix"], config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);

	if (pgMap.Ready()) {
		cout << "Opened database successfully" << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}

	std::shared_ptr<class PgTransaction> transaction = pgMap.GetTransaction("ACCESS SHARE");

	vector<double> bbox = {-1.0785318,50.788086,-1.0741129,50.790117};

	std::shared_ptr<class PgMapQuery> mapQuery = transaction->GetQueryMgr();
	int ret = mapQuery->Start(bbox, enc);
	while(ret == 0)
	{
		ret = mapQuery->Continue();
	}

	delete gzipEnc;
	outfi.close();
	return 0;
}
