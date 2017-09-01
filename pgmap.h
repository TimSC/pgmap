#ifndef _PGMAP_H
#define _PGMAP_H

#include <string>
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"

class PgMap
{
private:
	pqxx::connection dbconn;
	string tablePrefix;

public:
	PgMap(const std::string &connection, const string &tablePrefixIn);
	virtual ~PgMap();

	bool Ready();
	void MapQuery(const vector<double> &bbox, IDataStreamHandler &enc);
	void Dump(bool onlyLiveData, IDataStreamHandler &enc);
};

#endif //_PGMAP_H

