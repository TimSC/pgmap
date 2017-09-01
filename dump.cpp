#include <fstream>
#include <iostream>
#include "common.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"

int main(int argc, char **argv)
{	
	std::filebuf outfi;
	outfi.open("dump.o5m.gz", std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);

	O5mEncode enc(*gzipEnc);
	enc.StoreIsDiff(false);

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
	
	pqxx::connection dbconn(ss.str());
	if (dbconn.is_open()) {
		cout << "Opened database successfully: " << dbconn.dbname() << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}
	bool onlyLiveData = false;	

	//Lock database for reading (this must always be done in a set order)
	pqxx::work work(dbconn);
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "nodes IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "ways IN ACCESS SHARE MODE;");
	work.exec("LOCK TABLE "+config["dbtableprefix"] + "relations IN ACCESS SHARE MODE;");

	DumpNodes(work, config, onlyLiveData, enc);

	enc.Reset();

	DumpWays(work, config, onlyLiveData, enc);

	enc.Reset();
		
	DumpRelations(work, config, onlyLiveData, enc);

	//Release locks
	work.commit();

	enc.Finish();
	delete gzipEnc;
	outfi.close();
	dbconn.disconnect ();
	return 0;
}
