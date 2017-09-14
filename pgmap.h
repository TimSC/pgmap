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
	shared_ptr<pqxx::connection> dbconn;
	bool mapQueryActive;
	int mapQueryPhase;
	std::shared_ptr<class DataStreamRetainIds> retainNodeIds;
	std::shared_ptr<IDataStreamHandler> mapQueryEnc;
	vector<double> mapQueryBbox;
	std::shared_ptr<pqxx::work> mapQueryWork;
	set<int64_t> extraNodes;
	std::shared_ptr<class DataStreamRetainIds> retainWayIds;
	std::shared_ptr<class DataStreamRetainMemIds> retainWayMemIds;
	std::shared_ptr<pqxx::icursorstream> cursor;
	std::set<int64_t>::const_iterator setIterator;
	IDataStreamHandler nullEncoder;

public:
	PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn);
	virtual ~PgMapQuery();
	PgMapQuery& operator=(const PgMapQuery&);

	void SetDbConn(shared_ptr<pqxx::connection> db);
	int Start(const std::vector<double> &bbox, std::shared_ptr<IDataStreamHandler> &enc);
	int Continue();
	void Reset();
};

class PgMap
{
private:
	shared_ptr<pqxx::connection> dbconn;
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::string connectionString;

public:
	PgMap(const string &connection, const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn);
	virtual ~PgMap();
	PgMap& operator=(const PgMap&) {return *this;};

	bool Ready();

	void Dump(bool onlyLiveData, std::shared_ptr<IDataStreamHandler> &enc);

	std::shared_ptr<class PgMapQuery> GetQueryMgr();

	void GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> &out);
	bool StoreObjects(class OsmData &data, 
		std::map<int64_t, int64_t> &createdNodeIds, 
		std::map<int64_t, int64_t> &createdWaysIds,
		std::map<int64_t, int64_t> &createdRelationsIds,
		class PgMapError &errStr);
};

#endif //_PGMAP_H

