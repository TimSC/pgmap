#ifndef _PGMAP_H
#define _PGMAP_H

#include <string>
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/osmxml.h"

class PgMap
{
private:
	pqxx::connection dbconn;
	std::string tablePrefix;
	std::string connectionString;

public:
	PgMap(const std::string &connection, const std::string &tablePrefixIn);
	virtual ~PgMap();

	bool Ready();
	void MapQuery(const std::vector<double> &bbox, IDataStreamHandler &enc);
	void Dump(bool onlyLiveData, IDataStreamHandler &enc);
};

#endif //_PGMAP_H

