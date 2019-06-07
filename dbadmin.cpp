#include "dbadmin.h"
#include "dbids.h"
#include "dbcommon.h"
#include "dbstore.h"
#include "dbdecode.h"
#include "dbquery.h"
#include "dbusername.h"
#include "dbmeta.h"
#include "dbdump.h"
#include "dbshapelog.h"
#include "util.h"
#include "cppGzip/DecodeGzip.h"
#include <map>
#include <set>
#include <boost/filesystem.hpp>
#include <fstream>
using namespace std;
using namespace boost::filesystem;

bool ClearTable(pqxx::connection &c, pqxx::transaction_base *work, const string &tableName, std::string &errStr)
{
	try
	{
		string deleteSql = "DELETE FROM "+ c.quote_name(tableName) + ";";
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

bool ResetActiveTables(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tableActivePrefix, 
	const string &tableStaticPrefix,
	std::string &errStr)
{
	bool ok = ClearTable(c, work, tableActivePrefix + "livenodes", errStr);      if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "liveways", errStr);            if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "liverelations", errStr);       if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "oldnodes", errStr);            if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "oldways", errStr);             if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "oldrelations", errStr);        if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "nodeids", errStr);             if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "wayids", errStr);              if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "relationids", errStr);         if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "way_mems", errStr);            if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "relation_mems_n", errStr);     if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "relation_mems_w", errStr);     if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "relation_mems_r", errStr);     if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "nextids", errStr);             if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "changesets", errStr);          if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "usernames", errStr);           if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "wayshapes", errStr);           if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "relshapes", errStr);           if(!ok) return false;

	map<string, int64_t> nextIdMap;
	ok = GetNextObjectIds(c, work, 
		tableStaticPrefix,
		nextIdMap,
		errStr);
	if(!ok) return false;

	map<string, int64_t> emptyIdMap;
	ok = UpdateNextObjectIds(c, work, tableActivePrefix, nextIdMap, emptyIdMap, errStr);
	if(!ok) return false;

	//Update next changeset and UIDs
	ok = ResetChangesetUidCounts(c, work, 
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
	//PostgreSQL 9.3 and earlier does not support JSONB
	int majorVer=0, minorVer=0;
	DbGetVersion(c, work, majorVer, minorVer);
	string j = "JSONB";
	if(majorVer < 9 || (majorVer == 9 && minorVer <= 3))
		j = "JSON";
	bool ok = true;

	//Create schema version 11 if no meta table is defined
	if (not DbCheckTableExists(c, work, tablePrefix+"meta"))
	{
		string sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"oldnodes")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags "+j+", geom GEOMETRY(Point, 4326));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"oldnodes")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags "+j+", geom GEOMETRY(Point, 4326));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"oldways")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags "+j+", members "+j+");";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"oldrelations")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags "+j+", members "+j+", memberroles "+j+");";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"livenodes")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags "+j+", geom GEOMETRY(Point, 4326));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"liveways")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags "+j+", members "+j+");";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"liverelations")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags "+j+", members "+j+", memberroles "+j+");";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"nodeids")+" (id BIGINT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"wayids")+" (id BIGINT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"relationids")+" (id BIGINT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"way_mems")+" (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"relation_mems_n")+" (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"relation_mems_w")+" (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"relation_mems_r")+" (id BIGINT, version INTEGER, index INTEGER, member BIGINT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"nextids")+" (id VARCHAR(16), maxid BIGINT, PRIMARY KEY(id));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"changesets")+" (id BIGINT, username TEXT, uid INTEGER, tags "+j+", open_timestamp BIGINT, close_timestamp BIGINT, is_open BOOLEAN, geom GEOMETRY(Polygon, 4326), PRIMARY KEY(id));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"usernames")+" (uid INTEGER, timestamp BIGINT, username TEXT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"meta")+" (key TEXT, value TEXT);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		DbSetMetaValue(c, work, "schema_version", to_string(11), tablePrefix, errStr);
	}

	int schemaVersion = std::stoi(DbGetMetaValue(c, work, "schema_version", tablePrefix, errStr));
	if (schemaVersion == 11)
	{
		//Update to schema ver 12
		string sql = "ALTER TABLE "+c.quote_name(tablePrefix+"oldnodes")+" ADD COLUMN end_timestamp BIGINT;";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"wayshapes")+" (id BIGSERIAL PRIMARY KEY, way_id BIGINT, way_version INTEGER, start_timestamp BIGINT, end_timestamp BIGINT, nvers INTEGER[], bbox GEOMETRY(Polygon, 4326));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"relshapes")+" (id BIGSERIAL PRIMARY KEY, rel_id BIGINT, rel_version INTEGER, start_timestamp BIGINT, end_timestamp BIGINT, bbox GEOMETRY(Polygon, 4326));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"liveways")+" ADD COLUMN bbox GEOMETRY(Polygon, 4326), ADD COLUMN bbox_timestamp BIGINT;";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"liverelations")+" ADD COLUMN bbox GEOMETRY(Polygon, 4326), ADD COLUMN bbox_timestamp BIGINT;";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		DbSetMetaValue(c, work, "schema_version", to_string(12), tablePrefix, errStr);
	}

	return ok;
}

bool DbDropTables(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	string sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"oldnodes")+";";
	bool ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"oldways")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"oldrelations")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"livenodes")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"liveways")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"liverelations")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"nodeids")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"wayids")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relationids")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"way_mems")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relation_mems_n")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relation_mems_w")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relation_mems_r")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"nextids")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"meta")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"changesets")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose);
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"usernames")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose);
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"wayshapes")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose);
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relshapes")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose);

	return ok;

}

bool DbCopyData(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &filePrefix,
	const string &tablePrefix, 
	std::string &errStr)
{
	string sql = "COPY "+c.quote_name(tablePrefix+"oldnodes")+" FROM PROGRAM 'zcat "+filePrefix+"oldnodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	bool ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "COPY "+c.quote_name(tablePrefix+"oldways")+" FROM PROGRAM 'zcat "+filePrefix+"oldways.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+c.quote_name(tablePrefix+"oldrelations")+" FROM PROGRAM 'zcat "+filePrefix+"oldrelations.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "COPY "+c.quote_name(tablePrefix+"livenodes")+" FROM PROGRAM 'zcat "+filePrefix+"livenodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+c.quote_name(tablePrefix+"liveways")+" FROM PROGRAM 'zcat "+filePrefix+"liveways.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+c.quote_name(tablePrefix+"liverelations")+" FROM PROGRAM 'zcat "+filePrefix+"liverelations.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "COPY "+c.quote_name(tablePrefix+"nodeids")+" FROM PROGRAM 'zcat "+filePrefix+"nodeids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+c.quote_name(tablePrefix+"wayids")+" FROM PROGRAM 'zcat "+filePrefix+"wayids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+c.quote_name(tablePrefix+"relationids")+" FROM PROGRAM 'zcat "+filePrefix+"relationids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "COPY "+c.quote_name(tablePrefix+"way_mems")+" FROM PROGRAM 'zcat "+filePrefix+"waymems.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+c.quote_name(tablePrefix+"relation_mems_n")+" FROM PROGRAM 'zcat "+filePrefix+"relationmems-n.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+c.quote_name(tablePrefix+"relation_mems_w")+" FROM PROGRAM 'zcat "+filePrefix+"relationmems-w.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "COPY "+c.quote_name(tablePrefix+"relation_mems_r")+" FROM PROGRAM 'zcat "+filePrefix+"relationmems-r.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');";
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
	int majorVer=0, minorVer=0;
	DbGetVersion(c, work, majorVer, minorVer);
	string ine = "IF NOT EXISTS ";
	bool brinSupported = true;
	if(majorVer < 9 || (majorVer == 9 && minorVer <= 3))
	{
		ine = "";
		brinSupported = false;
	}

	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"oldnodes")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"oldnodes")+" ADD PRIMARY KEY (id, version);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"oldways")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"oldways")+" ADD PRIMARY KEY (id, version);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"oldrelations")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"oldrelations")+" ADD PRIMARY KEY (id, version);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"livenodes")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"livenodes")+" ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"liveways")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"liveways")+" ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"liverelations")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"liverelations")+" ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"nodeids")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"nodeids")+" ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"wayids")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"wayids")+" ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"relationids")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"relationids")+" ADD PRIMARY KEY (id);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	if(!DbCheckIndexExists(c, work, tablePrefix+"livenodes_gix"))
	{
		//Used to do a standard map query
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"livenodes_gix")+" ON "+c.quote_name(tablePrefix+"livenodes")+" USING GIST (geom);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "VACUUM ANALYZE "+c.quote_name(tablePrefix+"livenodes")+"(geom);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	if(!DbCheckIndexExists(c, work, tablePrefix+"oldnodes_gix"))
	{
		//Used for quering nodes at a particular point in time
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldnodes_gix")+" ON "+c.quote_name(tablePrefix+"oldnodes")+" USING GIST (geom);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "VACUUM ANALYZE "+c.quote_name(tablePrefix+"oldnodes")+"(geom);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"way_mems_mids")+" ON "+c.quote_name(tablePrefix+"way_mems")+" (member);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"relation_mems_n_mids")+" ON "+c.quote_name(tablePrefix+"relation_mems_n")+" (member);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"relation_mems_w_mids")+" ON "+c.quote_name(tablePrefix+"relation_mems_w")+" (member);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"relation_mems_r_mids")+" ON "+c.quote_name(tablePrefix+"relation_mems_r")+" (member);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	//Timestamp indicies
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldnodes_ts2")+" ON "+c.quote_name(tablePrefix+"oldnodes")+" (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldways_ts2")+" ON "+c.quote_name(tablePrefix+"oldways")+" (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldrelations_ts2")+" ON "+c.quote_name(tablePrefix+"oldrelations")+" (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"livenodes_ts")+" ON "+c.quote_name(tablePrefix+"livenodes")+" (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"liveways_ts")+" ON "+c.quote_name(tablePrefix+"liveways")+" (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"liverelations_ts")+" ON "+c.quote_name(tablePrefix+"liverelations")+" (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	if(brinSupported)
	{
		//Object user indices
		//Is this really used?
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldnodes_uid2")+" ON "+c.quote_name(tablePrefix+"oldnodes")+" USING BRIN(uid);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldways_uid2")+" ON "+c.quote_name(tablePrefix+"oldways")+" USING BRIN(uid);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldrelations_uid2")+" ON "+c.quote_name(tablePrefix+"oldrelations")+" USING BRIN(uid);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"livenodes_uid")+" ON "+c.quote_name(tablePrefix+"livenodes")+" USING BRIN(uid);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"liveways_uid")+" ON "+c.quote_name(tablePrefix+"liveways")+" USING BRIN(uid);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"liverelations_uid")+" ON "+c.quote_name(tablePrefix+"liverelations")+" USING BRIN(uid);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	//Changeset indices
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldnodes_cs2")+" ON "+c.quote_name(tablePrefix+"oldnodes")+" (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldways_cs2")+" ON "+c.quote_name(tablePrefix+"oldways")+" (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"oldrelations_cs2")+" ON "+c.quote_name(tablePrefix+"oldrelations")+" (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"livenodes_cs")+" ON "+c.quote_name(tablePrefix+"livenodes")+" (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"liveways_cs")+" ON "+c.quote_name(tablePrefix+"liveways")+" (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"liverelations_cs")+" ON "+c.quote_name(tablePrefix+"liverelations")+" (changeset);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"changesets_uidx")+" ON "+c.quote_name(tablePrefix+"changesets")+" (uid);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"changesets_open_timestampx")+" ON "+c.quote_name(tablePrefix+"changesets")+" (open_timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"changesets_close_timestampx")+" ON "+c.quote_name(tablePrefix+"changesets")+" (close_timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"changesets_is_openx")+" ON "+c.quote_name(tablePrefix+"changesets")+" (is_open);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	if(!DbCheckIndexExists(c, work, tablePrefix+"changesets_gix"))
	{
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"changesets_gix")+" ON "+c.quote_name(tablePrefix+"changesets")+" USING GIST (geom);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "VACUUM ANALYZE "+c.quote_name(tablePrefix+"changesets")+"(geom);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); 
	}

	if(DbCountPrimaryKeyCols(c, work, tablePrefix+"usernames")==0)
	{
		sql = "ALTER TABLE "+c.quote_name(tablePrefix+"usernames")+" ADD PRIMARY KEY (uid);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	}
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
	bool ok = GetMaxObjIdLiveOrOld(c, work, tablePrefix, objType, "id", errStr, val);
	if(!ok) return false;
	val ++; //Increment to make it the next valid ID
	if(val < minValIn)
		val = minValIn;
	if(valOut != nullptr)
		*valOut = val;

	cout << val << endl;
	stringstream ss;
	ss << "INSERT INTO "<<c.quote_name(tablePrefix+"nextids") << "(id, maxid) VALUES ("<<c.quote(objType)<< ", "<< (val) << ");";
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
	bool ok = ResetChangesetUidCounts(c, work, 
		"", tableStaticPrefix,
		errStr);
	if(!ok) return false;

	ok = ResetChangesetUidCounts(c, work, 
		tableStaticPrefix, tableModPrefix, 
		errStr);
	if(!ok) return false;

	ok = ResetChangesetUidCounts(c, work, 
		tableStaticPrefix, tableTestPrefix, 
		errStr);
	if(!ok) return false;
	return true;
}

bool ExtractUsernamesFromTable(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableName, 
	const std::string &tablePrefix, 
	std::string &errStr)
{
	stringstream sql;
	sql << "SELECT uid, username FROM " << tableName << ";";

	int step = 100;
	pqxx::icursorstream cursor( *work, sql.str(), "buildusernames", step );	
	DbUpsertUsernamePrepare(c, work, tablePrefix);
	set<int> foundUids;

	while(true)
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() )
			break;

		int uidCol = rows.column_number("uid");
		int usernameCol = rows.column_number("username");

		for (pqxx::result::const_iterator ci = rows.begin(); ci != rows.end(); ++ci) 
		{
			if (ci[uidCol].is_null() or ci[usernameCol].is_null())
				continue;
			
			int64_t uid = ci[uidCol].as<int64_t>();
			if(foundUids.find(uid) != foundUids.end()) continue; //Skip repeatedly inserting the same data
			string username = ci[usernameCol].as<string>();
			foundUids.insert(uid);

			DbUpsertUsername(c, work, tablePrefix, 
				uid, username);
		}
	}

	return true;
}

bool ExtractUsernamesFromTableSet(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tablePrefix, 
	std::string &errStr)
{
	string oNodeTable = c.quote_name(tablePrefix + "oldnodes");
	string oWayTable = c.quote_name(tablePrefix + "oldways");
	string oRelationTable = c.quote_name(tablePrefix + "oldrelations");
	string lNodeTable = c.quote_name(tablePrefix + "livenodes");
	string lWayTable = c.quote_name(tablePrefix + "liveways");
	string lRelationTable = c.quote_name(tablePrefix + "liverelations");

	ExtractUsernamesFromTable(c, work, verbose, oNodeTable, tablePrefix, errStr);
	ExtractUsernamesFromTable(c, work, verbose, oWayTable, tablePrefix, errStr);
	ExtractUsernamesFromTable(c, work, verbose, oRelationTable, tablePrefix, errStr);
	ExtractUsernamesFromTable(c, work, verbose, lNodeTable, tablePrefix, errStr);
	ExtractUsernamesFromTable(c, work, verbose, lWayTable, tablePrefix, errStr);
	ExtractUsernamesFromTable(c, work, verbose, lRelationTable, tablePrefix, errStr);

	return true;
}

bool DbGenerateUsernameTable(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	std::string &errStr)
{
	ExtractUsernamesFromTableSet(c, work, verbose, tableStaticPrefix, errStr);
	ExtractUsernamesFromTableSet(c, work, verbose, tableModPrefix, errStr);
	ExtractUsernamesFromTableSet(c, work, verbose, tableTestPrefix, errStr);

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

				bool ok = DbStoreObjects(c, work, tableModPrefix, block,
					createdNodeIds, createdWayIds, createdRelationIds, errStr);
				if(!ok)
					cout << "Warning: " << errStr << endl;
			}
		}
	}

	return true;
}

size_t DbCheckWaysFromCursor(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tablePrefix, 
	const string &excludeTablePrefix, 
	pqxx::icursorstream &cursor,
	int &numWaysProcessed, int &numWaysReported,
	const string &nodeStaticPrefix, 
	const string &nodeActivePrefix)
{
	class MetaData metaData;
	JsonToStringMap tagHandler;
	JsonToWayMembers wayMemHandler;
	const std::vector<int64_t> refs;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	bool verbose = false;
	size_t count = 0;

	//Get a batch of rows
	pqxx::result rows;
	cursor.get(rows);
	if ( rows.empty() )
	{	
		return 0; // nothing left to read
	}

	MetaDataCols metaDataCols;

	//Prepare to decode rows to way objects
	int idCol = rows.column_number("id");
	metaDataCols.changesetCol = rows.column_number("changeset");
	metaDataCols.usernameCol = rows.column_number("username");
	metaDataCols.uidCol = rows.column_number("uid");
	metaDataCols.timestampCol = rows.column_number("timestamp");
	metaDataCols.versionCol = rows.column_number("version");
	metaDataCols.visibleCol = -1;
	try
	{
		metaDataCols.visibleCol = rows.column_number("visible");
	}
	catch (invalid_argument &err) {}

	int tagsCol = rows.column_number("tags");
	int membersCol = rows.column_number("members");
	std::set<int64_t> nodeIds;
	std::map<int64_t, std::set<int64_t> > wayMemDict;
	
	//Collect list of nodes for the ways in this batch of rows
	for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

		int64_t objId = c[idCol].as<int64_t>();

		DecodeMetadata(c, metaDataCols, metaData);
	
		DecodeTags(c, tagsCol, tagHandler);

		DecodeWayMembers(c, membersCol, wayMemHandler);
		count ++;

		if(wayMemHandler.refs.size() < 2)
			cout << "Way " << objId << " has too few nodes" << endl;

		nodeIds.insert(wayMemHandler.refs.begin(), wayMemHandler.refs.end());
		wayMemDict[objId].insert(wayMemHandler.refs.begin(), wayMemHandler.refs.end());
	}

	numWaysProcessed += rows.size();

	//Query member nodes in database
	std::set<int64_t>::const_iterator it = nodeIds.begin();
	std::shared_ptr<class OsmData> data(new class OsmData());
	class DbUsernameLookup dbUsernameLookup(c, work, "", ""); //Don't care about accurate usernames
	while(it != nodeIds.end())
	{
		GetLiveNodesById(c, work, dbUsernameLookup,
			nodeStaticPrefix, 
			nodeActivePrefix, 
			nodeIds, it, 
			1000, data);
	}

	it = nodeIds.begin();
	while(it != nodeIds.end())
	{
		GetLiveNodesById(c, work, dbUsernameLookup,
			nodeActivePrefix, 
			"", 
			nodeIds, it, 
			1000, data);
	}

	//Gather found node IDs
	std::set<int64_t> foundNodeIds;
	for(size_t i=0; i<data->nodes.size(); i++)
	{
		const class OsmNode &node = data->nodes[i];
		foundNodeIds.insert(node.objId);
	}

	//Check we have found all nodes we expect
	for(std::map<int64_t, std::set<int64_t> >::iterator it = wayMemDict.begin();
		it != wayMemDict.end();
		it++)
	{
		int64_t wayId = it->first;
		const std::set<int64_t> &memIds = it->second;
		for(std::set<int64_t>::const_iterator it2 = memIds.begin(); it2 != memIds.end(); it2++)
		{
			std::set<int64_t>::iterator exists = foundNodeIds.find(*it2);
			if(exists == foundNodeIds.end())
				cout << "Way " << wayId << " references non-existent node " << (*it2) << endl;
		}
	}

	if(numWaysProcessed - numWaysReported > 100000)
	{
		cout << "Num ways processed " << numWaysProcessed << endl;
		numWaysReported = numWaysProcessed;
	}
	return count;
}

void DbCheckNodesExistForAllWays(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	const std::string &nodeStaticPrefix, 
	const std::string &nodeActivePrefix)
{
	string wayTable = c.quote_name(tablePrefix + "liveways");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "wayids");

	stringstream sql;
	sql << "SELECT " << wayTable << ".* FROM ";
	sql << wayTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<wayTable<<".id = "<<excludeTable<<".id";
		sql << " WHERE "<<excludeTable<<".id IS NULL";
	}
	sql << ";";

	int step = 100;
	int numWaysProcessed = 0;
	int numWaysReported = 0;
	pqxx::icursorstream cursor( *work, sql.str(), "waycursor", step );	

	size_t count = 1;
	while (count > 0)
		count = DbCheckWaysFromCursor(c, work, tablePrefix, excludeTablePrefix, cursor, 
			numWaysProcessed, numWaysReported,
			nodeStaticPrefix, nodeActivePrefix);
}

void DbCheckObjectIdTables(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, const std::string &edition, const std::string &objType)
{
	string objTable = c.quote_name(tablePrefix + edition + objType + "s");
	string excludeTable = c.quote_name(tablePrefix + objType + "ids");

	stringstream sql;
	sql << "SELECT " << objTable << ".id FROM ";
	sql << objTable;
	sql << " LEFT JOIN "<<excludeTable<<" ON "<<objTable<<".id = "<<excludeTable<<".id";
	sql << " WHERE "<<excludeTable<<".id IS NULL";
	sql << ";";

	cout << sql.str() << endl;

	int step = 100;
	pqxx::icursorstream cursor( *work, sql.str(), "objcursor", step );	

	bool found = false;
	while(true)
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() )
			break;

		int idCol = rows.column_number("id");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

			int64_t objId = c[idCol].as<int64_t>();
			cout << objType << " ID " << objId << " missing from " << excludeTable << endl;
			found = true;
		}
	}

	if(not found)
		cout << "No missing IDs found" << endl;
}

// *********************************************************************************

class CollectObjsUpdateBbox : public IDataStreamHandler
{
private:
	std::set<int64_t> wayBuffer;
	std::set<int64_t> relationBuffer;
	pqxx::connection &c;
	std::string staticTablePrefix; 
	std::string activeTablePrefix;

	void ProcessWayBuffer();
	void ProcessRelationBuffer();

public:
	std::string errStr;
	int64_t wayCount, relationCount;
	int64_t waysSinceCommit, relationsSinceCommit;
	int64_t prevWayId, prevRelationId;
    bool incrementalCommit;
	pqxx::transaction_base *work;

	CollectObjsUpdateBbox(pqxx::connection &c,
		const std::string &staticTablePrefix, const std::string &activeTablePrefix);
	virtual ~CollectObjsUpdateBbox() {};

	virtual bool Finish();

	virtual bool StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	virtual bool StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles);

	void Flush();
};

CollectObjsUpdateBbox::CollectObjsUpdateBbox(pqxx::connection &c,
	const std::string &staticTablePrefix, const std::string &activeTablePrefix) : 
	IDataStreamHandler(), c(c),
	staticTablePrefix(staticTablePrefix),
	activeTablePrefix(activeTablePrefix)
{
	work = nullptr;
	wayCount = 0;
	relationCount = 0;
	waysSinceCommit = 0;
	relationsSinceCommit = 0;
	prevWayId=-1;
	prevRelationId=-1;
	incrementalCommit=true;
}

bool CollectObjsUpdateBbox::Finish()
{
	if(wayBuffer.size() > 0)
		this->ProcessWayBuffer();
	if(relationBuffer.size() > 0)
		this->ProcessRelationBuffer();
	return false;
}

bool CollectObjsUpdateBbox::StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs)
{
	wayBuffer.insert(objId);
	prevWayId = objId;
	if(wayBuffer.size() > 1000)
		this->ProcessWayBuffer();
	if(incrementalCommit and waysSinceCommit > 5000)
		return true;	
	return false;
}

bool CollectObjsUpdateBbox::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	relationBuffer.insert(objId);
	prevRelationId = relationCount;
	if(relationBuffer.size() > 1000)
		this->ProcessRelationBuffer();
	if(incrementalCommit and relationsSinceCommit > 5000)		
		return true;
	return false;
}

void CollectObjsUpdateBbox::ProcessWayBuffer()
{
	if(wayBuffer.size() == 0) return;

	//Update bbox of ways in buffer
	class DbUsernameLookup *usernames = nullptr;
	std::set<int64_t> emptyNodeIds;
	std::set<int64_t> touchedWayIds2;
	bool ok = DbUpdateWayShapes(c, work, 
		staticTablePrefix, 
		"", 
		*usernames, 
		emptyNodeIds,
		wayBuffer,
		touchedWayIds2,
		errStr);

	wayCount += wayBuffer.size();
	waysSinceCommit += wayBuffer.size();
	cout << "w " << wayCount << endl;
	if(!ok)
		cout << errStr << endl;
	wayBuffer.clear();
}

void CollectObjsUpdateBbox::ProcessRelationBuffer()
{
	if(relationBuffer.size() == 0) return;

	//Update bbox of relations in buffer
	class DbUsernameLookup *usernames = nullptr;
	std::set<int64_t> emptyNodeIds, emptyWayIds;
	bool ok = DbUpdateRelationShapes(c, work, 
		staticTablePrefix, 
		"", 
		*usernames, 
		emptyNodeIds,
		emptyWayIds,
		relationBuffer,
		errStr);

	relationCount += relationBuffer.size();
	relationsSinceCommit += relationBuffer.size();
	cout << "r " << relationCount << endl;
	if(!ok)
		cout << errStr << endl;
	relationBuffer.clear();
}

void CollectObjsUpdateBbox::Flush()
{
	this->ProcessWayBuffer();
	this->ProcessRelationBuffer();
}

int DbUpdateWayBboxes(pqxx::connection &c, pqxx::transaction_base *work,
    int verbose,
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix,
	std::shared_ptr<class PgWork> (*transactionFactory)(void *adminObj),
	void *adminObj,
	std::string &errStr)
{
	//Process static ways
	class DbUsernameLookup *usernames = nullptr;
	std::shared_ptr<class CollectObjsUpdateBbox> collectObjsUpdateBbox = make_shared<class CollectObjsUpdateBbox>(
		c,
		staticTablePrefix, activeTablePrefix);

	int dumpFinished = 0;
	work->commit();

	while(!dumpFinished)
	{
		cout << "prevWayId " << collectObjsUpdateBbox->prevWayId << endl;
		std::shared_ptr<class PgWork> work2 = transactionFactory(adminObj);
		collectObjsUpdateBbox->work = work2->work.get();

		dumpFinished = DumpWays(c, work2->work.get(), *usernames, 
			staticTablePrefix, 
			"",
			false, collectObjsUpdateBbox->prevWayId,
			collectObjsUpdateBbox);
		cout << "dumpFinished " << dumpFinished << endl;
		collectObjsUpdateBbox->Flush();

		work2->work->commit();
		collectObjsUpdateBbox->waysSinceCommit = 0;
	}
	collectObjsUpdateBbox->Finish();

	dumpFinished = 0;
	/*while(!dumpFinished)
	{
		cout << "prevRelationId " << collectObjsUpdateBbox->prevRelationId << endl;
		std::shared_ptr<class PgWork> work2 = transactionFactory(adminObj);
		collectObjsUpdateBbox->work = work2->work.get();

		dumpFinished = DumpRelations(c, work2->work.get(), *usernames, 
			staticTablePrefix, 
			"",
			false, collectObjsUpdateBbox->prevRelationId,
			collectObjsUpdateBbox);
		
		work2->work->commit();
		collectObjsUpdateBbox->relationsSinceCommit = 0;		
	}*/
	collectObjsUpdateBbox->Finish();
	errStr = collectObjsUpdateBbox->errStr;

	return 0;	
}

void DbOutputBboxRows(const string &objType, pqxx::icursorstream &cursor)
{
	while(true)
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() )
			break;

		//for(size_t i=0; i<rows.columns(); i++)
		//	cout << "col " << i << "," << rows.column_name(i) << endl;

		int idCol = rows.column_number("id");
		int bboxCol = rows.column_number("bbox");
		int tsCol = rows.column_number("bbox_timestamp");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

			int64_t objId = c[idCol].as<int64_t>();
			cout << objType << "," << objId;

			if (!c[bboxCol].is_null())
			{
				string bboxWkt = c[bboxCol].as<string>();
				cout << "," << bboxWkt;
			}
			else
			{
				cout << ", NULL";
			}

			if (!c[tsCol].is_null())
			{
				cout << "," << c[tsCol].as<int64_t>();
			}
			else
			{
				cout << ", NULL";
			}

			cout << endl;
		}
	}
}

int DbSaveBboxes(pqxx::connection &c, pqxx::transaction_base *work,
    int verbose,
	const std::string &tablePrefix, 
	std::string &errStr)
{
	string objTable = c.quote_name(tablePrefix + "liveways");

	stringstream sql;
	sql << "SELECT id, ST_AsEWKT(bbox) AS bbox, bbox_timestamp FROM ";
	sql << objTable;
	//sql << " WHERE "<< objTable <<".bbox IS NOT NULL";
	sql << ";";

	int step = 1000;
	pqxx::icursorstream cursor( *work, sql.str(), "bboxcursor", step );	
	DbOutputBboxRows("way", cursor);

	objTable = c.quote_name(tablePrefix + "liverelations");

	stringstream sql2;
	sql2 << "SELECT id, ST_AsEWKT(bbox) AS bbox, bbox_timestamp FROM ";
	sql2 << objTable;
	//sql2 << " WHERE "<< objTable <<".bbox IS NOT NULL";
	sql2 << ";";

	pqxx::icursorstream cursor2( *work, sql.str(), "bboxcursor", step );	
	DbOutputBboxRows("relation", cursor2);

	return 0;
}

// ******************************************************************

