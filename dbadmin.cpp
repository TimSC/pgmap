#include "dbadmin.h"
#include "dbids.h"
#include "dbcommon.h"
#include <map>
using namespace std;

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
	const string &tablePrefix, 
	std::string &errStr)
{
	string sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"oldnodes (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags JSONB, geom GEOMETRY(Point, 4326));";
	bool ok = DbExec(work, sql, errStr); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"oldnodes (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags JSONB, geom GEOMETRY(Point, 4326));";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"oldways (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"oldrelations (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB, memberroles JSONB);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"livenodes (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, geom GEOMETRY(Point, 4326));";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"liveways (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"liverelations (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB, memberroles JSONB);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"nodeids (id BIGINT);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"wayids (id BIGINT);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"relationids (id BIGINT);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"way_mems (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"relation_mems_n (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"relation_mems_w (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"relation_mems_r (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"nextids (id VARCHAR(16), maxid BIGINT, PRIMARY KEY(id));";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"meta (key TEXT, value TEXT);";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "DELETE FROM "+tablePrefix+"meta WHERE key = 'schema_version';";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;
	sql = "INSERT INTO "+tablePrefix+"meta (key, value) VALUES ('schema_version', '11');";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+tablePrefix+"changesets (id BIGINT, username TEXT, uid INTEGER, tags JSONB, open_timestamp BIGINT, close_timestamp BIGINT, is_open BOOLEAN, geom GEOMETRY(Polygon, 4326), PRIMARY KEY(id));";
	ok = DbExec(work, sql, errStr);

	//sql = "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "+tablePrefix+";";
	//ok = DbExec(work, ss, errStr); if(!ok) return ok;
	return ok;
}

bool DbDropTables(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tablePrefix, 
	std::string &errStr)
{
	string sql = "DROP TABLE IF EXISTS "+tablePrefix+"oldnodes;";
	bool ok = DbExec(work, sql, errStr); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"oldways;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"oldrelations;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"livenodes;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"liveways;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"liverelations;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"nodeids;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"wayids;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"relationids;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"way_mems;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"relation_mems_n;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"relation_mems_w;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"relation_mems_r;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+tablePrefix+"nextids;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"meta;";
	ok = DbExec(work, sql, errStr); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+tablePrefix+"changesets;";
	ok = DbExec(work, sql, errStr);
	return ok;	

}

