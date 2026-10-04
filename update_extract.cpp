#include <iostream>
#include <boost/program_options.hpp>
#include "pgmap.h"
#include "util.h"
namespace po = boost::program_options;
int main(int argc, char **argv)
{
	try
	{
		po::options_description options("Update a stored extract to the current map");
		options.add_options()("help","Show usage")
			("id",po::value<int64_t>(),"Extract ID")
			("name",po::value<std::string>(),"Unique extract name")
			("config",po::value<std::string>()->default_value("config.cfg"),"Database configuration");
		po::variables_map vm;
		po::store(po::parse_command_line(argc,argv,options),vm); po::notify(vm);
		if(vm.count("help")) { std::cout<<options<<std::endl; return 0; }
		if(vm.count("id")+vm.count("name")!=1) throw std::invalid_argument("Specify exactly one of --id or --name");
		int64_t id=vm.count("id")?vm["id"].as<int64_t>():0;
		std::string name=vm.count("name")?vm["name"].as<std::string>():"";
		if((vm.count("id") && id<=0)||(vm.count("name") && name.empty()))
			throw std::invalid_argument("ID must be positive and name nonempty");
		std::map<std::string,std::string> config; ReadSettingsFile(vm["config"].as<std::string>(),config);
		PgMap map(GeneratePgConnectionString(config),config["dbtableprefix"],
			config["dbtablemodifyprefix"],config["dbtablemodifyprefix"],config["dbtabletestprefix"]);
		auto transaction=map.GetTransaction("ACCESS SHARE");
		id=transaction->UpdateExtract(id,name); transaction->Commit();
		std::cout<<"Updated extract "<<id<<std::endl; return 0;
	}
	catch(const std::exception &error) { std::cerr<<"Extract update failed: "<<error.what()<<std::endl; return 1; }
}
