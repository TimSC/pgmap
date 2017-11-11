#include <fstream>
#include <iostream>
#include "pgmap.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/osmxml.h"

int main(int argc, char **argv)
{	

	string configContent;
	cout << "Reading settings from config.cfg" << endl;
	ReadFileContents("config.cfg", false, configContent);
	std::vector<std::string> lines = split(configContent, '\n');
	std::map<string, string> config;
	for(size_t i=0; i < lines.size(); i++)
	{
		const std::string &line = lines[i];
		std::vector<std::string> parts = split(line, ':');
		if (parts.size() < 2) continue;
		config[parts[0]] = parts[1];
	}
	
	std::stringstream ss;
	ss << "dbname=";
	ss << config["dbname"];
	ss << " user=";
	ss << config["dbuser"];
	ss << " password='";
	ss << config["dbpass"];
	ss << "' hostaddr=";
	ss << config["dbhost"];
	ss << " port=";
	ss << "5432";
	
	class PgMap pgMap(ss.str(), config["dbtableprefix"], config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);

	if (pgMap.Ready()) {
		cout << "Opened database successfully" << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}

	bool running = true;
	string inputStr;
	class PgMapError errStr;
	while(running)
	{

		cout << "1. Drop tables" << endl;
		cout << "2. Create tables" << endl;
		cout << "3. Copy data" << endl;
		cout << "4. Create indicies" << endl;
		cout << "5. Refresh max IDs" << endl;
		cout << "q. Quit" << endl;

		cin >> inputStr;
		int verbose = 1;

		if(inputStr == "1")
		{

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->DropMapTables(verbose, errStr);
			//bool ok = admin->CopyMapData("/home/tim/dev/osm2pgcopy/portsmouth-", errStr);
			//bool ok = admin->CreateMapIndices(errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "2")
		{

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CreateMapTables(verbose, errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "3")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CopyMapData(verbose, "/home/tim/dev/osm2pgcopy/portsmouth-", errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "4")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CreateMapIndices(verbose, errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "5")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->RefreshMapIds(verbose, errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "q")
		{
			running = false;
			continue;
		}

	}



	return 0;
}

