#include <fstream>
#include <iostream>
#include "pgmap.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/osmxml.h"
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

//https://stackoverflow.com/a/440240/4288232
void gen_random(char *s, const int len) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    s[len] = '\0';
}

int main(int argc, char **argv)
{	
	srand(time(NULL));

	//Check config.cfg doesn't exist
	fs::path p("config.cfg");
	if(fs::exists(p))
	{
		if(argc >= 1)
			cout << "config.cfg already exists. Delete this file before running " << argv[0] << endl;
		return -1;
	}

	cout << "Reading settings from config.cfg.template" << endl;
	std::map<string, string> config;
	ReadSettingsFile("config.cfg.template", config);
	
	pqxx::connection c; //Connect to local machine
	
	if (!c.is_open())
	{
		cout << "Could not connect to database" << endl;
		return -1;
	}

	char passwd[21];
	gen_random(passwd, 20);

	pqxx::nontransaction w(c);
	pqxx::result r = w.exec("CREATE DATABASE "+config["dbname"]+";");
	string sql = "CREATE USER "+config["dbuser"]+" WITH PASSWORD '";
	sql += passwd;
	sql += "';";
	r = w.exec(sql);
	r = w.exec("GRANT ALL PRIVILEGES ON DATABASE db_map to pycrocosm;");

	config["dbpass"] = passwd;
	WriteSettingsFile("config.cfg", config);

	pqxx::connection c2("dbname='"+config["dbname"]+"'");
	pqxx::nontransaction w2(c2);
	r = w2.exec("ALTER USER "+config["dbuser"]+" WITH SUPERUSER;");
	r = w2.exec("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "+config["dbuser"]+";");
	r = w2.exec("CREATE EXTENSION postgis;");
	r = w2.exec("CREATE EXTENSION postgis_topology;");

	string cstr = GeneratePgConnectionString(config);
	class PgMap pgMap(cstr, config["dbtableprefix"], config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);

	if (pgMap.Ready()) {
		cout << "Opened database successfully" << endl;
	} else {
		cout << "Can't open database" << endl;
		return -1;
	}

	bool verbose = true;
	class PgMapError errStr;
	std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
	bool ok = admin->CreateMapTables(verbose, 0, true, errStr);
	if(!ok)
	{
		cout << errStr.errStr << endl;
		return -1;
	}

	ok = admin->CreateMapIndices(verbose, errStr);
	if(!ok)
	{
		cout << errStr.errStr << endl;
		return -1;
	}

	ok = admin->RefreshMapIds(verbose, errStr);
	if(!ok)
	{
		cout << errStr.errStr << endl;
		return -1;
	}

	ok = admin->RefreshMaxChangesetUid(verbose, errStr);
	if(!ok)
	{
		cout << errStr.errStr << endl;
		return -1;
	}

	return 0;

}

