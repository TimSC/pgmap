#include <fstream>
#include <iostream>
#include "pgmap.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/osmxml.h"
#include <boost/program_options.hpp>
namespace po = boost::program_options;

int main(int argc, char **argv)
{	
	po::options_description desc("Dump the whole map");
	desc.add_options()
		("help", "produce help message")
		("out", po::value<string>()->default_value("dump.o5m.gz"), "Output file name (extension must be .osm.gz or .o5m.gz)")
		("edit-ids", "Add the latest edit activity ID and atomic edit ID as attributes of the root XML element (.osm.gz only)")
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
	vector<string> outFinaSp = split(outFina, '.');
	bool o5mOut = outFinaSp.size() >= 3 && outFinaSp[outFinaSp.size()-1] == "gz" && outFinaSp[outFinaSp.size()-2] == "o5m";
	bool xmlOut = outFinaSp.size() >= 3 && outFinaSp[outFinaSp.size()-1] == "gz" && outFinaSp[outFinaSp.size()-2] == "osm";
	if(!o5mOut && !xmlOut)
	{
		cerr << "Output file name does not have a recognized extension" << endl;
		return 2;
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
	bool order = true;

	std::shared_ptr<class PgTransaction> transaction = pgMap.GetTransaction("ACCESS SHARE");

	// The encoder writes the root element when constructed, so the edit IDs are
	// read first. They come from the same snapshot as the dump below.
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

	transaction->Dump(order, true, true, true, enc);

	enc.reset();
	delete gzipEnc;
	outfi.close();
	
	cout << "Add done!" << endl;
	return 0;
}
