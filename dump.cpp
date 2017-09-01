#include <fstream>
#include <iostream>
#include "pgmap.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"

int main(int argc, char **argv)
{	
	std::filebuf outfi;
	outfi.open("dump.o5m.gz", std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);

	O5mEncode enc(*gzipEnc);

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
	
	class PgMap pgMap(ss.str(), config["dbtableprefix"]);

	if (pgMap.Ready()) {
		cout << "Opened database successfully" << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}
	bool onlyLiveData = false;	

	pgMap.Dump(onlyLiveData, enc);

	delete gzipEnc;
	outfi.close();
	
	return 0;
}
