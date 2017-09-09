#ifndef _PGMAP_H
#define _PGMAP_H

#include <string>
#include <set>
#include <map>
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/osmxml.h"

class PgMap
{
private:
	pqxx::connection dbconn;
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::string connectionString;

public:
	PgMap(const string &connection, const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn);
	virtual ~PgMap();

	bool Ready();
	int MapQuery(const std::vector<double> &bbox, unsigned int maxNodes, IDataStreamHandler &enc);
	void Dump(bool onlyLiveData, IDataStreamHandler &enc);

	void GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, class IDataStreamHandler &out);
	void StoreObjects(class OsmData &data, 
		std::map<int64_t, int64_t> &createdNodeIds, 
		std::map<int64_t, int64_t> &createdWaysIds,
		std::map<int64_t, int64_t> &createdRelationsIds);
};

#endif //_PGMAP_H

