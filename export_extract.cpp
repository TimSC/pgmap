#include <fstream>
#include <iostream>
#include <filesystem>
#include <memory>
#include <unistd.h>
#include <boost/program_options.hpp>
#include "pgmap.h"
#include "util.h"
#include "osmfile.h"
#include "cppGzip/EncodeGzip.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
	try
	{
		po::options_description options("Export a stored database extract");
		options.add_options()
			("help", "Show usage")
			("id", po::value<int64_t>(), "Extract ID")
			("name", po::value<std::string>(), "Unique extract name")
			("config", po::value<std::string>()->default_value("config.cfg"), "Database configuration")
			("out", po::value<std::string>(), (std::string("Output file name, ending in ") + OsmFileWriter::SupportedNames()).c_str());
		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, options), vm);
		po::notify(vm);
		if(vm.count("help")) { std::cout << options << std::endl; return 0; }
		if(vm.count("id") + vm.count("name") != 1 || !vm.count("out"))
			throw std::invalid_argument("Specify exactly one of --id or --name, and --out");
		int64_t id = vm.count("id") ? vm["id"].as<int64_t>() : 0;
		std::string name = vm.count("name") ? vm["name"].as<std::string>() : "";
		if((vm.count("id") && id <= 0) || (vm.count("name") && name.empty()))
			throw std::invalid_argument("ID must be positive and name must not be empty");
		std::string destination = vm["out"].as<std::string>();
		OsmFileWriter::FormatOf(destination); // Reject an unusable name before touching the database

		std::map<std::string,std::string> config;
		ReadSettingsFile(vm["config"].as<std::string>(), config);
		PgMap map(GeneratePgConnectionString(config), config["dbtableprefix"],
			config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);
		auto transaction = map.GetTransaction("ACCESS SHARE");

		// The writer publishes only a complete file, preserving an existing output on failure.
		OsmFileWriter writer(destination);
		id = transaction->ExportExtract(id, name, writer.Encoder());
		transaction->Abort(); // Read-only export; no database changes to commit.
		writer.Close();
		std::cout << "Exported extract " << id << " to " << destination << std::endl;
		return 0;
	}
	catch(const std::exception &error)
	{
		std::cerr << "Extract export failed: " << error.what() << std::endl;
		return 1;
	}
}
