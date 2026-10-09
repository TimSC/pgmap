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
			("edit-ids", "Add the extract's edit activity ID and atomic edit ID checkpoint to the file header (not available for .pbf)")
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
		OsmFormat format = OsmFileWriter::FormatOf(destination); // Reject an unusable name before touching the database
		if(vm.count("edit-ids") && !OsmFileWriter::HasHeaderAttribs(format))
			throw std::invalid_argument("--edit-ids is not available for .pbf output, which has nowhere to hold the IDs");

		std::map<std::string,std::string> config;
		ReadSettingsFile(vm["config"].as<std::string>(), config);
		PgMap map(GeneratePgConnectionString(config), config["dbtableprefix"],
			config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);
		auto transaction = map.GetTransaction("ACCESS SHARE");

		// The IDs an extract is current to are its stored checkpoint, not the
		// map's latest: the extract may be waiting for an update.
		TagMap headerAttribs;
		if(vm.count("edit-ids"))
		{
			std::vector<ExtractInfo> extracts, selected;
			transaction->ListExtracts(extracts);
			for(const ExtractInfo &info : extracts)
				if(id ? info.extractId == id : info.name == name)
					selected.push_back(info);
			// Not found or ambiguous is reported by the export itself below
			if(selected.size() == 1)
			{
				if(selected[0].editActivityId < 0 || selected[0].atomicEditId < 0)
					throw std::runtime_error("Extract has no checkpoint to write with --edit-ids");
				headerAttribs["edit_activity_id"] = std::to_string(selected[0].editActivityId);
				headerAttribs["atomic_edit_id"] = std::to_string(selected[0].atomicEditId);
			}
		}

		// The writer publishes only a complete file, preserving an existing output on failure.
		OsmFileWriter writer(destination, headerAttribs);
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
