#include <fstream>
#include <iostream>
#include <filesystem>
#include <memory>
#include <unistd.h>
#include <boost/program_options.hpp>
#include "pgmap.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"

namespace po = boost::program_options;

int main(int argc, char **argv)
{
	std::string temporary;
	try
	{
		po::options_description options("Export a stored database extract");
		options.add_options()
			("help", "Show usage")
			("id", po::value<int64_t>(), "Extract ID")
			("name", po::value<std::string>(), "Unique extract name")
			("config", po::value<std::string>()->default_value("config.cfg"), "Database configuration")
			("out", po::value<std::string>(), "Output .osm, .o5m, .osm.gz or .o5m.gz file");
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
		std::string format = destination;
		bool compressed = format.size() >= 3 && format.substr(format.size()-3) == ".gz";
		if(compressed) format.resize(format.size()-3);
		std::string extension = std::filesystem::path(format).extension().string();
		if(extension != ".osm" && extension != ".o5m")
			throw std::invalid_argument("Output must end in .osm, .o5m, .osm.gz or .o5m.gz");

		std::map<std::string,std::string> config;
		ReadSettingsFile(vm["config"].as<std::string>(), config);
		PgMap map(GeneratePgConnectionString(config), config["dbtableprefix"],
			config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);
		auto transaction = map.GetTransaction("ACCESS SHARE");

		// Publish only a complete file, preserving an existing output on failure.
		std::string pattern = destination + ".tmp.XXXXXX";
		std::vector<char> path(pattern.begin(), pattern.end()); path.push_back('\0');
		int descriptor = mkstemp(path.data());
		if(descriptor < 0) throw std::runtime_error("Cannot create output temporary file");
		temporary = path.data();
		close(descriptor);
		std::filebuf file;
		if(!file.open(temporary, std::ios::out | std::ios::binary))
			throw std::runtime_error("Cannot open output file");
		std::unique_ptr<EncodeGzip> gzip;
		std::streambuf *stream = &file;
		if(compressed) { gzip = std::make_unique<EncodeGzip>(file); stream = gzip.get(); }
		std::shared_ptr<IDataStreamHandler> encoder;
		if(extension == ".osm") encoder = std::make_shared<OsmXmlEncode>(*stream, TagMap{});
		else encoder = std::make_shared<O5mEncode>(*stream);
		id = transaction->ExportExtract(id, name, encoder);
		encoder.reset();
		gzip.reset();
		if(!file.close()) throw std::runtime_error("Failed to finish output file");
		transaction->Abort(); // Read-only export; no database changes to commit.
		std::filesystem::rename(temporary, destination);
		temporary.clear();
		std::cout << "Exported extract " << id << " to " << destination << std::endl;
		return 0;
	}
	catch(const std::exception &error)
	{
		if(!temporary.empty()) { std::error_code ignored; std::filesystem::remove(temporary, ignored); }
		std::cerr << "Extract export failed: " << error.what() << std::endl;
		return 1;
	}
}
