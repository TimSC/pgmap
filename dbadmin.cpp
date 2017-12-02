#include "dbadmin.h"
#include "dbids.h"
#include "dbcommon.h"
#include "dbstore.h"
#include "cppGzip/DecodeGzip.h"
#include <map>
#include <boost/filesystem.hpp>
#include <fstream>
using namespace std;
using namespace boost::filesystem;

bool ClearTable(pqxx::work *work, const string &tableName, std::string &errStr)
{
	try
	{
		string deleteSql = "DELETE FROM "+ tableName + ";";
		work->exec(deleteSql);
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = e.what();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}
	return true;
}

bool ResetActiveTables(pqxx::connection &c, pqxx::work *work, 
	const string &tableActivePrefix, 
	const string &tableStaticPrefix,
	std::string &errStr)
{
	bool ok = ClearTable(work, tableActivePrefix + "livenodes", errStr);      if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "liveways", errStr);            if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "liverelations", errStr);       if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "oldnodes", errStr);            if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "oldways", errStr);             if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "oldrelations", errStr);        if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "nodeids", errStr);             if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "wayids", errStr);              if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "relationids", errStr);         if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "way_mems", errStr);            if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "relation_mems_n", errStr);     if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "relation_mems_w", errStr);     if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "relation_mems_r", errStr);     if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "nextids", errStr);             if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "changesets", errStr);             if(!ok) return false;

	map<string, int64_t> nextIdMap;
	ok = GetNextObjectIds(work, 
		tableStaticPrefix,
		nextIdMap,
		errStr);
	if(!ok) return false;

	map<string, int64_t> emptyIdMap;
	ok = UpdateNextObjectIds(work, tableActivePrefix, nextIdMap, emptyIdMap, errStr);
	if(!ok) return false;

	//Update next changeset and UIDs
	ok = ResetChangesetUidCounts(work, 
		tableStaticPrefix, tableActivePrefix, 
		errStr);
	if(!ok) return false;

	return ok;	
}

bool DbCreateTables(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	string sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"oldnodes (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags JSONB, geom GEOMETRY(Point, 4326));";
	bool ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"oldnodes (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags JSONB, geom GEOMETRY(Point, 4326));";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"oldways (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"oldrelations (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB, memberroles JSONB);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"livenodes (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, geom GEOMETRY(Point, 4326));";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"liveways (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"liverelations (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB, memberroles JSONB);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"nodeids (id BIGINT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"wayids (id BIGINT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"relationids (id BIGINT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"way_mems (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"relation_mems_n (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"relation_mems_w (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"relation_mems_r (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"nextids (id VARCHAR(16), maxid BIGINT, PRIMARY KEY(id));";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"meta (key TEXT, value TEXT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "DELETE FROM "+tablePrefix+"meta WHERE key = 'schema_version';";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "INSERT INTO "+tablePrefix+"meta (key, value) VALUES ('schema_version', '11');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"changesets (id BIGINT, username TEXT, uid INTEGER, tags JSONB, open_timestamp BIGINT, close_timestamp BIGINT, is_open BOOLEAN, geom GEOMETRY(Polygon, 4326), PRIMARY KEY(id));";
	ok = DbExec(work, sql, errStr, nullptr, verbose);

	//sql = "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "+tablePrefix+";";
	//ok = DbExec(work, ss, errStr); if(!ok) return ok;
	return ok;
}

bool DbDropTables(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	string sql = "DROP TABLE IF EXISTS "+tablePrefix+"oldnodes;";
	bool ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"oldways;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"oldrelations;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"livenodes;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"liveways;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"liverelations;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"nodeids;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"wayids;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"relationids;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"way_mems;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"relation_mems_n;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"relation_mems_w;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"relation_mems_r;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"nextids;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"meta;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"changesets;";
	ok = DbExec(work, sql, errStr, nullptr, verbose);
	return ok;	

}

bool DbCopyData(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &filePrefix,
	const string &tablePrefix, 
	std::string &errStr)
{
	string sql = "COPY "+tablePrefix+"oldnodes FROM PROGRAM 'zcat "+filePrefix+"oldnodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	bool ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "COPY "+tablePrefix+"oldways FROM PROGRAM 'zcat "+filePrefix+"oldways.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+tablePrefix+"oldrelations FROM PROGRAM 'zcat "+filePrefix+"oldrelations.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "COPY "+tablePrefix+"livenodes FROM PROGRAM 'zcat "+filePrefix+"livenodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+tablePrefix+"liveways FROM PROGRAM 'zcat "+filePrefix+"liveways.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+tablePrefix+"liverelations FROM PROGRAM 'zcat "+filePrefix+"liverelations.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "COPY "+tablePrefix+"nodeids FROM PROGRAM 'zcat "+filePrefix+"nodeids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+tablePrefix+"wayids FROM PROGRAM 'zcat "+filePrefix+"wayids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+tablePrefix+"relationids FROM PROGRAM 'zcat "+filePrefix+"relationids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "COPY "+tablePrefix+"way_mems FROM PROGRAM 'zcat "+filePrefix+"waymems.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+tablePrefix+"relation_mems_n FROM PROGRAM 'zcat "+filePrefix+"relationmems-n.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+tablePrefix+"relation_mems_w FROM PROGRAM 'zcat "+filePrefix+"relationmems-w.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+tablePrefix+"relation_mems_r FROM PROGRAM 'zcat "+filePrefix+"relationmems-r.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); 
	return ok;	
}

bool DbCreateIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	bool ok = true;
	string sql;

	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"oldnodes")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"oldnodes ADD PRIMARY KEY (id, version);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"oldways")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"oldways ADD PRIMARY KEY (id, version);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"oldrelations")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"oldrelations ADD PRIMARY KEY (id, version);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"livenodes")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"livenodes ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"liveways")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"liveways ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"liverelations")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"liverelations ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"nodeids")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"nodeids ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"wayids")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"wayids ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"relationids")==0)
	{
		sql = "ALTER TABLE "+tablePrefix+"relationids ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"livenodes_gix ON "+tablePrefix+"livenodes USING GIST (geom);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "VACUUM ANALYZE "+tablePrefix+"livenodes(geom);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"way_mems_mids ON "+tablePrefix+"way_mems (member);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"relation_mems_n_mids ON "+tablePrefix+"relation_mems_n (member);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"relation_mems_w_mids ON "+tablePrefix+"relation_mems_w (member);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"relation_mems_r_mids ON "+tablePrefix+"relation_mems_r (member);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"livenodes_ts ON "+tablePrefix+"livenodes (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"liveways_ts ON "+tablePrefix+"liveways (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"liverelations_ts ON "+tablePrefix+"liverelations (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	//Timestamp indicies
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldnodes_ts ON "+tablePrefix+"livenodes (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldways_ts ON "+tablePrefix+"liveways (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldrelations_ts ON "+tablePrefix+"liverelations (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"livenodes_ts ON "+tablePrefix+"livenodes (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"liveways_ts ON "+tablePrefix+"liveways (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"liverelations_ts ON "+tablePrefix+"liverelations (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	//Object user indices
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldnodes_uid ON "+tablePrefix+"livenodes USING BRIN(uid);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldways_uid ON "+tablePrefix+"liveways USING BRIN(uid);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldrelations_uid ON "+tablePrefix+"liverelations USING BRIN(uid);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"livenodes_uid ON "+tablePrefix+"livenodes USING BRIN(uid);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"liveways_uid ON "+tablePrefix+"liveways USING BRIN(uid);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"liverelations_uid ON "+tablePrefix+"liverelations USING BRIN(uid);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	//Changeset indices
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldnodes_cs ON "+tablePrefix+"livenodes (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldways_cs ON "+tablePrefix+"liveways (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"oldrelations_cs ON "+tablePrefix+"liverelations (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"livenodes_cs ON "+tablePrefix+"livenodes (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"liveways_cs ON "+tablePrefix+"liveways (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"liverelations_cs ON "+tablePrefix+"liverelations (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"changesets_uidx ON "+tablePrefix+"changesets (uid);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"changesets_open_timestampx ON "+tablePrefix+"changesets (open_timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"changesets_close_timestampx ON "+tablePrefix+"changesets (close_timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"changesets_is_openx ON "+tablePrefix+"changesets (is_open);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX IF NOT EXISTS "+tablePrefix+"changesets_gix ON "+tablePrefix+"changesets USING GIST (geom);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "VACUUM ANALYZE "+tablePrefix+"changesets(geom);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); 
	return ok;
}

bool DbRefreshMaxIdsOfType(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	const string &objType,
	int64_t minValIn,
	std::string &errStr,
	int64_t *valOut)
{
	int64_t val;
	bool ok = GetMaxObjIdLiveOrOld(work, tablePrefix, objType, "id", errStr, val);
	if(!ok) return false;
	val ++; //Increment to make it the next valid ID
	if(val < minValIn)
		val = minValIn;
	if(valOut != nullptr)
		*valOut = val;

	cout << val << endl;
	stringstream ss;
	ss << "INSERT INTO "<<tablePrefix<<"nextids(id, maxid) VALUES ('"<<objType<< "', "<< (val) << ");";
	ok = DbExec(work, ss.str(), errStr, nullptr, verbose); if(!ok) return ok;
	if(!ok) return false;

	return ok;
}

bool DbRefreshMaxIds(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	std::string &errStr)
{
	int64_t maxStaticNode=0, maxStaticWay=0, maxStaticRelation=0;
	bool ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableStaticPrefix, 
		"node", 1,
		errStr, &maxStaticNode);
	if(!ok) return false;
	ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableModPrefix, 
		"node", maxStaticNode,
		errStr, nullptr);
	if(!ok) return false;
	ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableTestPrefix, 
		"node", maxStaticNode,
		errStr, nullptr);
	if(!ok) return false;

	ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableStaticPrefix, 
		"way", 1,
		errStr, &maxStaticWay);
	if(!ok) return false;
	ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableModPrefix, 
		"way", maxStaticWay,
		errStr, nullptr);
	if(!ok) return false;
	ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableTestPrefix, 
		"way", maxStaticWay,
		errStr, nullptr);
	if(!ok) return false;

	ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableStaticPrefix, 
		"relation", 1,
		errStr, &maxStaticRelation);
	if(!ok) return false;
	ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableModPrefix, 
		"relation", maxStaticRelation,
		errStr, nullptr);
	if(!ok) return false;
	ok = DbRefreshMaxIdsOfType(c, work, 
		verbose, tableTestPrefix, 
		"relation", maxStaticRelation,
		errStr, nullptr);
	if(!ok) return false;

	return true;
}

bool DbRefreshMaxChangesetUid(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	std::string &errStr)
{
	//Update next changeset and UIDs
	bool ok = ResetChangesetUidCounts(work, 
		"", tableStaticPrefix,
		errStr);
	if(!ok) return false;

	ok = ResetChangesetUidCounts(work, 
		tableStaticPrefix, tableModPrefix, 
		errStr);
	if(!ok) return false;

	ok = ResetChangesetUidCounts(work, 
		tableStaticPrefix, tableTestPrefix, 
		errStr);
	if(!ok) return false;
	return true;
}

bool DbApplyDiffs(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	const std::string &diffPath, 
	std::string &errStr)
{
	path p (diffPath);

	if(is_directory(p))
	{
		//Recusively walk through directories
		vector<path> result;
		copy(directory_iterator(p), directory_iterator(),
			back_inserter(result));
		sort(result.begin(), result.end());
		for (vector<path>::const_iterator it (result.begin()); it != result.end(); ++it)
		{
			//cout << "   " << *it << endl;

			string pathStr(it->native());
			bool ok = DbApplyDiffs(c, work, 
				verbose, 
				tableStaticPrefix, 
				tableModPrefix, 
				tableTestPrefix, 
				pathStr, 
				errStr);
			if(!ok) return false;
		}
	}
	else
	{
		if (extension(diffPath) == ".gz")
		{
			cout << "   " << diffPath << endl;
			std::string xmlData;
			DecodeGzipQuickFromFilename(diffPath, xmlData);
			
			shared_ptr<class OsmChange> data(new class OsmChange());
			std::stringbuf sb(xmlData);
			LoadFromOsmChangeXml(sb, data);

			for(size_t i=0; i<data->blocks.size(); i++)
			{
				cout << data->actions[i] << endl;
				class OsmData &block = data->blocks[i];

				//Set visibility flag depending on action
				bool isDelete = data->actions[i] == "delete";
				for(size_t j=0; j<block.nodes.size(); j++)
					block.nodes[j].metaData.visible = !isDelete;
				for(size_t j=0; j<block.ways.size(); j++)
					block.ways[j].metaData.visible = !isDelete;
				for(size_t j=0; j<block.relations.size(); j++)
					block.relations[j].metaData.visible = !isDelete;

				//Store objects
				std::map<int64_t, int64_t> createdNodeIds, createdWayIds, createdRelationIds;

				bool ok = ::StoreObjects(c, work, tableModPrefix, block, 
					createdNodeIds, createdWayIds, createdRelationIds, errStr);
				if(!ok)
					cout << "Warning: " << errStr << endl;
			}
		}
	}

	return true;
}

