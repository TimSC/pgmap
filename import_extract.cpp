#include <iostream>
#include <memory>
#include <boost/program_options.hpp>
#include "pgmap.h"
#include "util.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
	try
	{
		po::options_description options("Import a map file as a stored database extract");
		options.add_options()
			("help", "Show usage")
			("in", po::value<std::string>(), (std::string("Input file name, ending in ") + OsmInputFileNames()).c_str())
			("name", po::value<std::string>(), "Name for the new extract")
			("bbox", po::value<std::string>(), "Area the extract covers (e.g -1.078,50.788,-1.074,50.790), instead of the bounds in the file")
			("edit-activity-id", po::value<int64_t>(), "Edit activity ID the contents are current to, instead of the one in the file")
			("atomic-edit-id", po::value<int64_t>(), "Atomic edit ID the contents are current to, instead of the one in the file")
			("config", po::value<std::string>()->default_value("config.cfg"), "Database configuration");
		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, options), vm);
		po::notify(vm);
		if(vm.count("help")) { std::cout << options << std::endl; return 0; }
		if(!vm.count("in"))
			throw std::invalid_argument("Specify --in");
		std::string source = vm["in"].as<std::string>();
		std::string name = vm.count("name") ? vm["name"].as<std::string>() : "";
		if(vm.count("name") && name.empty())
			throw std::invalid_argument("Name must not be empty");

		std::vector<double> bbox;
		if(vm.count("bbox"))
		{
			for(const std::string &text : split(vm["bbox"].as<std::string>(), ','))
			{
				size_t consumed = 0;
				double value = 0.0;
				try { value = std::stod(text, &consumed); }
				catch(const std::exception &) { consumed = std::string::npos; }
				if(consumed != text.size()) throw std::invalid_argument("Invalid bbox coordinate");
				bbox.push_back(value);
			}
			if(bbox.size() != 4) throw std::invalid_argument("Bbox must have four coordinates");
		}

		if(vm.count("edit-activity-id") != vm.count("atomic-edit-id"))
			throw std::invalid_argument("Give both --edit-activity-id and --atomic-edit-id, or neither");
		int64_t editActivityId = -1, atomicEditId = -1;
		if(vm.count("edit-activity-id"))
		{
			editActivityId = vm["edit-activity-id"].as<int64_t>();
			atomicEditId = vm["atomic-edit-id"].as<int64_t>();
			if(editActivityId < 0 || atomicEditId < 0)
				throw std::invalid_argument("Edit IDs must not be negative");
		}

		std::map<std::string,std::string> config;
		ReadSettingsFile(vm["config"].as<std::string>(), config);
		PgMap map(GeneratePgConnectionString(config), config["dbtableprefix"],
			config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);
		auto transaction = map.GetTransaction("ACCESS SHARE");
		try
		{
			auto importer = transaction->StartImportExtract(name, bbox, editActivityId, atomicEditId);
			LoadOsmFromFile(source, importer);
			// The extract and all of its contents appear together, or not at all.
			transaction->Commit();
			std::cout << "Imported " << source << " as extract " << importer->GetId() << ": "
				<< importer->GetNumNodes() << " nodes, " << importer->GetNumWays() << " ways, "
				<< importer->GetNumRelations() << " relations" << std::endl;
		}
		catch(...)
		{
			transaction->Abort();
			throw;
		}
		return 0;
	}
	catch(const std::exception &error)
	{
		std::cerr << "Extract import failed: " << error.what() << std::endl;
		return 1;
	}
}
