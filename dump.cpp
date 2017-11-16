#include <fstream>
#include <iostream>
#include "pgmap.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/osmxml.h"

int main(int argc, char **argv)
{	
	std::filebuf outfi;
	outfi.open("dump.o5m.gz", std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);

	shared_ptr<IDataStreamHandler> enc(new O5mEncode(*gzipEnc));

	cout << "Reading settings from config.cfg" << endl;
	std::map<string, string> config;
	ReadSettingsFile("config.cfg", config);
	
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
	bool order = true;	

	std::shared_ptr<class PgTransaction> transaction = pgMap.GetTransaction("ACCESS SHARE");
	transaction->Dump(order, enc);

	delete gzipEnc;
	outfi.close();
	
	cout << "Add done!" << endl;
	return 0;
}
