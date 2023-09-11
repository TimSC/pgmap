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
		("in", po::value<string>(), "path to diffs, or diff file name")
		("verbose", po::value<int>(), "verbosity level (default is 1)")
	;

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
		cout << desc << "\n";
		return 1;
	}

	cout << "Reading settings from config.cfg" << endl;
	std::map<string, string> config;
	ReadSettingsFile("config.cfg", config);

	string inPath = config["diffs_path"];
	if (vm.count("in"))
		inPath = vm["in"].as<string>();
	if(inPath.size() == 0)
	{
		cerr << "Input path must be specified" << endl;
		cout << desc << "\n";
		return -1;
	}

	int verbose = 1;
	if (vm.count("verbose"))
		inPath = vm["in"].as<string>();
	
	string cstr = GeneratePgConnectionString(config);
	
	class PgMap pgMap(cstr, config["dbtablespace"], config["dbtableprefix"], config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);
	class PgMapError errStr;

	//Apply diffs to database	
	std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("EXCLUSIVE");
	bool ok = admin->ApplyDiffs(inPath, verbose, errStr);

	if(!ok)
	{
		cout << errStr.errStr << endl;
		admin->Abort();
		return -2;
	}

	//Refresh node, way and relation max IDs
	ok = admin->RefreshMapIds(verbose, errStr);

	if(!ok)
	{
		cout << errStr.errStr << endl;
		admin->Abort();
		return -3;
	}

	//Note that changeset, user ID counts are not updated (because this is currently very slow).

	admin->Commit();
	cout << "All done!" << endl;
	return 0;
}

