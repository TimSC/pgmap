#ifndef _PGMAP_H
#define _PGMAP_H

#include <string>
#include <set>
#include <map>
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/osmxml.h"
#include "cppo5m/OsmData.h"

class PgMapError
{
public:
	PgMapError();
	PgMapError(const string &connection);
	PgMapError(const PgMapError &obj);
	virtual ~PgMapError();

	std::string errStr;
};

class PgMapQuery
{
private:
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	pqxx::connection *dbconn; //Borrowed reference
	bool mapQueryActive;
	int mapQueryPhase;
	class DataStreamRetainIds *retainNodeIds;
	IDataStreamHandler *mapQueryEnc; //Borrowed reference
	vector<double> mapQueryBbox;
	pqxx::work *mapQueryWork;
	set<int64_t> extraNodes;
	class DataStreamRetainIds *retainWayIds;
	class DataStreamRetainMemIds *retainWayMemIds;

public:
	PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn);
	virtual ~PgMapQuery();

	void SetDbConn(pqxx::connection &db);
	int Start(const std::vector<double> &bbox, IDataStreamHandler &enc);
	int Continue();
	void Reset();
};

class PgMap
{
private:
	pqxx::connection dbconn;
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::string connectionString;

public:
	class PgMapQuery pgMapQuery;

	PgMap(const string &connection, const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn);
	virtual ~PgMap();

	bool Ready();

	void Dump(bool onlyLiveData, IDataStreamHandler &enc);

	void GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, class IDataStreamHandler &out);
	bool StoreObjects(class OsmData &data, 
		std::map<int64_t, int64_t> &createdNodeIds, 
		std::map<int64_t, int64_t> &createdWaysIds,
		std::map<int64_t, int64_t> &createdRelationsIds,
		class PgMapError &errStr);
};

#endif //_PGMAP_H

