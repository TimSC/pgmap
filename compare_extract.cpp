#include <iostream>
#include <boost/program_options.hpp>
#include "pgmap.h"
#include "util.h"
namespace po = boost::program_options;

static void Report(const ExtractComparison &result)
{
	std::cout<<"Extract "<<result.extractId<<" bbox "<<result.bbox[0]<<","<<result.bbox[1]<<","
		<<result.bbox[2]<<","<<result.bbox[3]<<std::endl;
	const char *kinds[]={"nodes","ways","relations"};
	for(size_t i=0;i<3;i++)
		std::cout<<kinds[i]<<": extract "<<result.extractCounts[i]<<", query "<<result.queryCounts[i]<<std::endl;
	for(const auto &d : result.differences)
	{
		std::cout<<d.type<<" "<<d.objId<<": ";
		if(!d.extractVersion) std::cout<<"missing from extract (query version "<<d.queryVersion<<")";
		else if(!d.queryVersion) std::cout<<"not in query (extract version "<<d.extractVersion<<")";
		else std::cout<<"extract version "<<d.extractVersion<<", query version "<<d.queryVersion;
		std::cout<<std::endl;
	}
	if(result.pendingActivity)
		std::cout<<"Note: the map has edits later than the extract's checkpoint; run update_extract"<<std::endl;
	if(result.queryModeDiffers)
		std::cout<<"Note: the extract was saved with a different useBboxInQuery mode from the map's current one"<<std::endl;
	if(result.Matches()) std::cout<<"Extract "<<result.extractId<<" matches query"<<std::endl;
	else std::cout<<"Extract "<<result.extractId<<" differs from query: "<<result.missingFromExtract
		<<" missing from extract, "<<result.notInQuery<<" not in query, "
		<<result.versionMismatches<<" version mismatches"<<std::endl;
}

// Exit status: 0 when every extract checked matches its query, 1 when any differs, 2 on error.
int main(int argc, char **argv)
{
	try
	{
		po::options_description options("Compare stored extracts with fresh map queries of their bboxes");
		options.add_options()("help","Show usage")
			("id",po::value<int64_t>(),"Extract ID")
			("name",po::value<std::string>(),"Unique extract name")
			("all","Check every stored extract")
			("config",po::value<std::string>()->default_value("config.cfg"),"Database configuration");
		po::variables_map vm;
		po::store(po::parse_command_line(argc,argv,options),vm); po::notify(vm);
		if(vm.count("help")) { std::cout<<options<<std::endl; return 0; }
		if(vm.count("id")+vm.count("name")+vm.count("all")!=1)
			throw std::invalid_argument("Specify exactly one of --id, --name or --all");
		int64_t id=vm.count("id")?vm["id"].as<int64_t>():0;
		std::string name=vm.count("name")?vm["name"].as<std::string>():"";
		if((vm.count("id") && id<=0)||(vm.count("name") && name.empty()))
			throw std::invalid_argument("ID must be positive and name nonempty");
		std::map<std::string,std::string> config; ReadSettingsFile(vm["config"].as<std::string>(),config);
		PgMap map(GeneratePgConnectionString(config),config["dbtableprefix"],
			config["dbtablemodifyprefix"],config["dbtablemodifyprefix"],config["dbtabletestprefix"]);
		// One snapshot serves both sides, so concurrent edits cannot cause differences.
		auto transaction=map.GetTransaction("ACCESS SHARE");
		std::vector<ExtractComparison> results;
		if(vm.count("all")) transaction->CompareAllExtracts(results);
		else
		{
			results.emplace_back();
			transaction->CompareExtract(id,name,results.back());
		}
		transaction->Abort(); // Read-only comparison; nothing to commit.

		size_t differing=0;
		for(const auto &result : results)
		{
			Report(result);
			if(!result.Matches()) differing++;
		}
		if(vm.count("all"))
			std::cout<<"Checked "<<results.size()<<" extracts: "<<differing<<" differ from their queries"<<std::endl;
		return differing ? 1 : 0;
	}
	catch(const std::exception &error) { std::cerr<<"Extract comparison failed: "<<error.what()<<std::endl; return 2; }
}
