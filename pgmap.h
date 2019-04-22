#ifndef _PGMAP_H
#define _PGMAP_H

#include <string>
#include <set>
#include <map>
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/osmxml.h"
#include "cppo5m/OsmData.h"
#include "dbusername.h"

class PgMapError
{
public:
	PgMapError();
	PgMapError(const std::string &connection);
	PgMapError(const PgMapError &obj);
	virtual ~PgMapError();

	std::string errStr;
};

class PgStringWrap
{
public:
	PgStringWrap();
	PgStringWrap(const std::string &strIn);
	PgStringWrap(const PgStringWrap &obj);
	virtual ~PgStringWrap();

	std::string str;
};

class PgChangeset
{
public:
	PgChangeset();
	PgChangeset(const PgChangeset &obj);
	virtual ~PgChangeset();
	PgChangeset& operator=(const PgChangeset &obj);
	
	int64_t objId, uid, open_timestamp, close_timestamp;
	std::string username;
	TagMap tags;
	bool is_open, bbox_set;
	double x1, y1, x2, y2;
};

class PgWork
{
public:
	PgWork();
	PgWork(pqxx::transaction_base *workIn);
	PgWork(const PgWork &obj);
	virtual ~PgWork();
	PgWork& operator=(const PgWork &obj);

	std::shared_ptr<pqxx::transaction_base> work;
};

class PgMapQuery
{
private:
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::shared_ptr<pqxx::connection> dbconn;
	bool mapQueryActive;
	int mapQueryPhase;
	std::shared_ptr<class DataStreamRetainIds> retainNodeIds;
	std::shared_ptr<IDataStreamHandler> mapQueryEnc;
	std::vector<double> mapQueryBbox;
	std::string mapQueryWkt;
	std::shared_ptr<class PgWork> sharedWork;
	std::set<int64_t> extraNodes;
	std::shared_ptr<class DataStreamRetainIds> retainWayIds;
	std::shared_ptr<class DataStreamRetainMemIds> retainWayMemIds;
	std::shared_ptr<class DataStreamRetainIds> retainRelationIds;
	std::shared_ptr<pqxx::icursorstream> cursor;
	std::set<int64_t>::const_iterator setIterator;
	IDataStreamHandler nullEncoder;
	class DbUsernameLookup &dbUsernameLookup;

public:
	PgMapQuery(const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		std::shared_ptr<pqxx::connection> &db,
		std::shared_ptr<class PgWork> sharedWorkIn,
		class DbUsernameLookup &dbUsernameLookupIn);
	virtual ~PgMapQuery();
	PgMapQuery& operator=(const PgMapQuery&);

	int Start(const std::vector<double> &bbox, std::shared_ptr<IDataStreamHandler> &enc);
	int Start(const std::string &wkt, std::shared_ptr<IDataStreamHandler> &enc);
	int Continue();
	void Reset();
};

class PgTransaction
{
private:
	std::shared_ptr<pqxx::connection> dbconn;
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::string connectionString;
	std::string shareMode;
	std::shared_ptr<class PgWork> sharedWork;
	class DbUsernameLookup dbUsernameLookup;

public:
	PgTransaction(std::shared_ptr<pqxx::connection> dbconnIn,
		const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		std::shared_ptr<class PgWork> sharedWorkIn,
		const std::string &shareMode);
	virtual ~PgTransaction();

	std::shared_ptr<class PgMapQuery> GetQueryMgr();

	void GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);
	void GetFullObjectById(const std::string &type, int64_t objectId, 
		std::shared_ptr<IDataStreamHandler> out);
	void GetObjectsByIdVer(const std::string &type, const std::set<std::pair<int64_t, int64_t> > &objectIdVers, 
		std::shared_ptr<IDataStreamHandler> out);
	void GetObjectsHistoryById(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);

	bool LogWayShapes(const std::set<int64_t> &touchedNodeIds,
		const std::set<int64_t> &touchedWayIds,
		int64_t timestamp,
		std::set<int64_t> &touchedWayIdsOut,
		class PgMapError &errStr);
	bool LogRelationShapes(int64_t timestamp,
		const std::set<int64_t> &touchedNodeIds,
		const std::set<int64_t> &touchedWayIds,
		const std::set<int64_t> &touchedRelationIds,
		class PgMapError &errStr);
	bool UpdateWayShapes(const std::set<int64_t> &touchedNodeIds,
		const std::set<int64_t> &touchedWayIds,
		std::set<int64_t> &touchedWayIdsOut,
		class PgMapError &errStr);
	bool UpdateRelationShapes(const std::set<int64_t> &touchedNodeIds,
		const std::set<int64_t> &touchedWayIds,
		const std::set<int64_t> &touchedRelationIds,
		class PgMapError &errStr);
	bool QueryAugDiff(int64_t startTimestamp,
		bool bboxSet,
		const std::vector<double> &bbox,
		class PgStringWrap &fiOut,
		class PgMapError &errStr);

	bool StoreObjects(class OsmData &data, 
		std::map<int64_t, int64_t> &createdNodeIds, 
		std::map<int64_t, int64_t> &createdWaysIds,
		std::map<int64_t, int64_t> &createdRelationsIds,
		bool saveToStaticTables,
		class PgMapError &errStr);
	void GetWaysForNodes(const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);	
	void GetRelationsForObjs(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);	

	bool ResetActiveTables(class PgMapError &errStr);
	void GetReplicateDiff(int64_t timestampStart, int64_t timestampEnd,
		class OsmChange &out);
	void Dump(bool order, bool nodes, bool ways, bool relations, 
		std::shared_ptr<IDataStreamHandler> enc);

	int64_t GetAllocatedId(const std::string &type);
	int64_t PeekNextAllocatedId(const std::string &type);

	int GetChangeset(int64_t objId,
		class PgChangeset &changesetOut,
		class PgMapError &errStr);
	int GetChangesetOsmChange(int64_t changesetId,
		std::shared_ptr<class IOsmChangeBlock> output,
		class PgMapError &errStr);
	bool GetChangesets(std::vector<class PgChangeset> &changesetsOut,
		int64_t user_uid, //0 means don't filter
		int64_t openedBeforeTimestamp, //-1 means don't filter
		int64_t closedAfterTimestamp, //-1 means don't filter
		bool is_open_only,
		bool is_closed_only,
		class PgMapError &errStr);
	int64_t CreateChangeset(const class PgChangeset &changeset,
		class PgMapError &errStr);
	bool UpdateChangeset(const class PgChangeset &changeset,
		class PgMapError &errStr);
	bool CloseChangeset(int64_t changesetId,
		int64_t closedTimestamp,
		class PgMapError &errStr);
	bool CloseChangesetsOlderThan(int64_t whereBeforeTimestamp,
		int64_t closedTimestamp,
		class PgMapError &errStr);

	std::string GetMetaValue(const std::string &key, 
		class PgMapError &errStr);
	bool SetMetaValue(const std::string &key, 
		const std::string &value, 
		class PgMapError &errStr);
	bool UpdateUsername(int uid, const std::string &username,
		class PgMapError &errStr);

	bool GetHistoricMapQuery(const std::vector<double> &bbox,
		int64_t existsAtTimestamp, 
		std::shared_ptr<IDataStreamHandler> &enc);

	void Commit();
	void Abort();
};

class PgAdmin
{
private:
	std::shared_ptr<pqxx::connection> dbconn;
	std::string tableStaticPrefix;
	std::string tableModPrefix;
	std::string tableTestPrefix;
	std::string connectionString;
	std::shared_ptr<class PgWork> sharedWork;
	std::string shareMode;

public:
	PgAdmin(std::shared_ptr<pqxx::connection> dbconnIn,
		const std::string &tableStaticPrefixIn, 
		const std::string &tableModPrefixIn,
		const std::string &tableTestPrefixIn,
		std::shared_ptr<class PgWork> sharedWorkIn,
		const std::string &shareModeIn);
	virtual ~PgAdmin();

	bool CreateMapTables(int verbose, class PgMapError &errStr);
	bool DropMapTables(int verbose, class PgMapError &errStr);
	bool CopyMapData(int verbose, const std::string &filePrefix, class PgMapError &errStr);
	bool CreateMapIndices(int verbose, class PgMapError &errStr);
	bool ApplyDiffs(const std::string &diffPath, int verbose, class PgMapError &errStr);
	bool RefreshMapIds(int verbose, class PgMapError &errStr);
	bool ImportChangesetMetadata(const std::string &fina, int verbose, class PgMapError &errStr);
	bool RefreshMaxChangesetUid(int verbose, class PgMapError &errStr);
	bool GenerateUsernameTable(int verbose, class PgMapError &errStr);

	bool CheckNodesExistForWays(class PgMapError &errStr);
	bool CheckObjectIdTables(class PgMapError &errStr);

	void Commit();
	void Abort();
};

class PgMap
{
private:
	std::shared_ptr<pqxx::connection> dbconn;
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::string tableModPrefix;
	std::string tableTestPrefix;
	std::string connectionString;
	std::shared_ptr<class PgWork> sharedWork;

public:
	PgMap(const std::string &connection, const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		const std::string &tableModPrefixIn,
		const std::string &tableTestPrefixIn);
	virtual ~PgMap();
	PgMap& operator=(const PgMap&) {return *this;};

	bool Ready();

	//pqxx only supports one active transaction per connection
	std::shared_ptr<class PgTransaction> GetTransaction(const std::string &shareMode);
	std::shared_ptr<class PgAdmin> GetAdmin();
	std::shared_ptr<class PgAdmin> GetAdmin(const std::string &shareMode);
};

#endif //_PGMAP_H

