#include <fstream>
#include <iostream>
#include "util.h"
#include "osmfile.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/model.h"
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
		("out", po::value<string>(), (string("Output file name, ending in ") + OsmFileWriter::SupportedNames()).c_str())
		("edit-ids", "Add the latest edit activity ID and atomic edit ID to the file header (.osm or .json output, with or without .gz)")
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
	OsmFormat outFormat = OsmFormat::O5m;
	try
	{
		outFormat = OsmFileWriter::FormatOf(outFina);
	}
	catch(const invalid_argument &error)
	{
		cerr << error.what() << endl;
		return 2;
	}
	if(vm.count("edit-ids") && !OsmFileWriter::HasHeaderAttribs(outFormat))
	{
		cerr << "--edit-ids requires .osm or .json output; the other formats have nowhere to hold the IDs" << endl;
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

	try
	{
		std::shared_ptr<class PgTransaction> transaction = pgMap.GetTransaction("ACCESS SHARE");

		// The edit IDs come from the same snapshot as the map query below.
		TagMap rootAttribs;
		if(vm.count("edit-ids"))
			rootAttribs = transaction->GetLatestEditIdAttribs();

		OsmFileWriter writer(outFina, rootAttribs);
		shared_ptr<IDataStreamHandler> enc = writer.Encoder();

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
		if(ret < 0)
			throw runtime_error("Map query failed");
		mapQuery.reset();
		enc.reset();
		writer.Close();
	}
	catch(const exception &error)
	{
		cerr << "Extract failed: " << error.what() << endl;
		return 1;
	}
	return 0;
}
