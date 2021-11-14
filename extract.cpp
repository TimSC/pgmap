#include <fstream>
#include <iostream>
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/OsmData.h"
#include "cppo5m/osmxml.h"
#include "pgmap.h"
#include <boost/program_options.hpp>
namespace po = boost::program_options;

int main(int argc, char **argv)
{	
	// Declare the supported options.
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "produce help message")
		("wkt", po::value<string>(), "WKT polygon file name")
		("bbox", po::value<string>(), "bbox shape (e.g -1.078,50.788,-1.074,50.790)")
		("out", po::value<string>(), "Output file name (extension must be .osm.gz or .o5m.gz)")
	;

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
		cout << desc << "\n";
		return 1;
	}

	string wkt;
	if (vm.count("wkt"))
	{
		string wktFina = vm["wkt"].as<string>();
		ReadFileContents(wktFina.c_str(), false, wkt);
	}

	vector<double> bbox;
	if (vm.count("bbox"))
	{
		vector<string> bboxvals = split(vm["bbox"].as<string>(), ',');
		for(size_t i=0; i < bboxvals.size(); i++)
		{
			bbox.push_back(atof(bboxvals[i].c_str()));
		}
		if(bbox.size() != 4)
		{
			cerr << "Bbox must have 4 numbers" << endl;
			exit(-1);
		}
	}

	if(bbox.size() == 0 && wkt.size() == 0)
	{
		cerr << "Bbox or WKT filename must be specified" << endl;
		exit(-2);
	}

	string outFina = "extract.o5m.gz";
	if (vm.count("out"))
	{
		outFina = vm["out"].as<string>();
	}
	vector<string> outFinaSp = split(outFina, '.');
	if(outFinaSp.size() < 3)
	{
		cerr << "Output file name does not have a recognized extension" << endl;
		exit(-2);
	}


	std::filebuf outfi;
	EncodeGzip *gzipEnc = nullptr;
	shared_ptr<IDataStreamHandler> enc;

	if(outFinaSp[outFinaSp.size()-1] == "gz" && outFinaSp[outFinaSp.size()-2] == "o5m")
	{
		outfi.open(outFina, std::ios::out);
		gzipEnc = new class EncodeGzip(outfi);

		enc.reset(new O5mEncode(*gzipEnc));
	}
	else if(outFinaSp[outFinaSp.size()-1] == "gz" && outFinaSp[outFinaSp.size()-2] == "osm")
	{
		outfi.open(outFina, std::ios::out);
		gzipEnc = new class EncodeGzip(outfi);

		TagMap empty;
		enc.reset(new OsmXmlEncode(*gzipEnc, empty));
	}
	else
	{
		cerr << "Output file name does not have a recognized extension" << endl;
		exit(-2);
	}

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

	std::shared_ptr<class PgMapQuery> mapQuery = transaction->GetQueryMgr();
	int ret = 0;
	if(bbox.size() > 0)
		ret = mapQuery->Start(bbox, time(nullptr), enc);
	else
		ret = mapQuery->Start(wkt, time(nullptr), enc);
	while(ret == 0)
	{
		ret = mapQuery->Continue();
	}

	delete gzipEnc;
	outfi.close();
	return 0;
}
