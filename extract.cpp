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
		("save-db", "Save rectangular extract to database instead of a file")
		("name", po::value<string>()->default_value(""), "Optional database extract name")
		("out", po::value<string>(), "Output file name (extension must be .osm.gz or .o5m.gz)")
		("edit-ids", "Add the latest edit activity ID and atomic edit ID as attributes of the root XML element (.osm.gz only)")
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
			try {
				size_t consumed = 0;
				double value = stod(bboxvals[i], &consumed);
				if(consumed != bboxvals[i].size()) throw invalid_argument("trailing characters");
				bbox.push_back(value);
			} catch(const exception &) {
				cerr << "Invalid bbox coordinate" << endl;
				return 2;
			}
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

	if(vm.count("save-db"))
	{
		if(bbox.size() != 4 || vm.count("wkt") || vm.count("out") || vm.count("edit-ids"))
		{
			cerr << "--save-db requires --bbox and cannot be combined with --wkt, --out or --edit-ids" << endl;
			return 2;
		}
		try
		{
			map<string,string> config;
			ReadSettingsFile("config.cfg", config);
			PgMap map(GeneratePgConnectionString(config), config["dbtableprefix"],
				config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);
			auto transaction = map.GetTransaction("ACCESS SHARE");
			int64_t id = transaction->SaveExtract(bbox, vm["name"].as<string>());
			transaction->Commit();
			cout << "Saved database extract " << id << endl;
			return 0;
		}
		catch(const exception &error)
		{
			cerr << "Database extract failed: " << error.what() << endl;
			return 1;
		}
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


	bool o5mOut = outFinaSp[outFinaSp.size()-1] == "gz" && outFinaSp[outFinaSp.size()-2] == "o5m";
	bool xmlOut = outFinaSp[outFinaSp.size()-1] == "gz" && outFinaSp[outFinaSp.size()-2] == "osm";
	if(!o5mOut && !xmlOut)
	{
		cerr << "Output file name does not have a recognized extension" << endl;
		exit(-2);
	}
	if(vm.count("edit-ids") && !xmlOut)
	{
		cerr << "--edit-ids requires .osm.gz output; o5m has no root element to hold attributes" << endl;
		return 2;
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

	// The encoder writes the root element when constructed, so the edit IDs are
	// read first. They come from the same snapshot as the map query below.
	TagMap rootAttribs;
	if(vm.count("edit-ids"))
		rootAttribs = transaction->GetLatestEditIdAttribs();

	std::filebuf outfi;
	outfi.open(outFina, std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);
	shared_ptr<IDataStreamHandler> enc;
	if(o5mOut)
		enc.reset(new O5mEncode(*gzipEnc));
	else
		enc.reset(new OsmXmlEncode(*gzipEnc, rootAttribs));

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
