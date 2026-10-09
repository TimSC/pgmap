#include <fstream>
#include <iostream>
#include "pgmap.h"
#include "util.h"
#include "osmfile.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/osmxml.h"
#include <boost/program_options.hpp>
namespace po = boost::program_options;

int main(int argc, char **argv)
{	
	po::options_description desc("Dump the whole map");
	desc.add_options()
		("help", "produce help message")
		("out", po::value<string>()->default_value("dump.o5m.gz"), (string("Output file name, ending in ") + OsmFileWriter::SupportedNames()).c_str())
		("edit-ids", "Add the latest edit activity ID and atomic edit ID to the file header (.osm or .json output, with or without .gz)")
	;
	po::variables_map vm;
	try
	{
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);
	}
	catch(const exception &error)
	{
		cerr << error.what() << endl;
		return 2;
	}
	if (vm.count("help")) {
		cout << desc << "\n";
		return 1;
	}

	string outFina = vm["out"].as<string>();
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
	bool order = true;

	try
	{
		std::shared_ptr<class PgTransaction> transaction = pgMap.GetTransaction("ACCESS SHARE");

		// The edit IDs come from the same snapshot as the dump below.
		TagMap rootAttribs;
		if(vm.count("edit-ids"))
			rootAttribs = transaction->GetLatestEditIdAttribs();

		OsmFileWriter writer(outFina, rootAttribs);
		transaction->Dump(order, true, true, true, writer.Encoder());
		writer.Close();
	}
	catch(const exception &error)
	{
		cerr << "Dump failed: " << error.what() << endl;
		return 1;
	}
	
	cout << "Add done!" << endl;
	return 0;
}
