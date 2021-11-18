#include "dbadmin.h"
#include "dbids.h"
#include "dbcommon.h"
#include "dbstore.h"
#include "dbdecode.h"
#include "dbquery.h"
#include "dbusername.h"
#include "dbmeta.h"
#include "util.h"
#include "cppGzip/DecodeGzip.h"
#include "cppo5m/utils.h"
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
	ok = ClearTable(c, work, tableActivePrefix + "usernames", errStr);            if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "edit_activity", errStr);            if(!ok) return false;
	ok = ClearTable(c, work, tableActivePrefix + "query_activity", errStr);            if(!ok) return false;

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

bool DbUpgradeTables0to11(pqxx::connection &c, pqxx::transaction_base *work, 
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

	string sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"oldnodes")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags "+j+", geom GEOMETRY(Point, 4326));";
	bool ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

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

	sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"meta")+" (key TEXT, value TEXT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "DELETE FROM "+c.quote_name(tablePrefix+"meta")+" WHERE key = 'schema_version';";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "INSERT INTO "+c.quote_name(tablePrefix+"meta")+" (key, value) VALUES ('schema_version', '11');";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"changesets")+" (id BIGINT, username TEXT, uid INTEGER, tags "+j+", open_timestamp BIGINT, close_timestamp BIGINT, is_open BOOLEAN, geom GEOMETRY(Polygon, 4326), PRIMARY KEY(id));";
	ok = DbExec(work, sql, errStr, nullptr, verbose);
	sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"usernames")+" (uid INTEGER, timestamp BIGINT, username TEXT);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	return ok;
}

bool DbUpgradeTables11to12(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &parentPrefix, 
	const std::string &tablePrefix, 
	std::string &errStr)
{
	cout << "DbUpgradeTables11to12" << endl;
	bool ok = true;
	string sql;

	sql = "ALTER TABLE "+c.quote_name(tablePrefix+"liveways")+" ADD COLUMN bbox GEOMETRY(Geometry, 4326);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "ALTER TABLE "+c.quote_name(tablePrefix+"liverelations")+" ADD COLUMN bbox GEOMETRY(Geometry, 4326);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	string vn = c.quote_name(tablePrefix+"visiblenodes");
	string ln = c.quote_name(tablePrefix+"livenodes");
	if(parentPrefix.size() > 0)
	{
		string pvn = c.quote_name(parentPrefix+"visiblenodes");
		string nids = c.quote_name(tablePrefix+"nodeids");

		sql = "CREATE VIEW "+vn+" AS SELECT "+pvn+".* FROM "+pvn+" LEFT JOIN "+nids+" ON "+nids+".id = "+pvn+".id WHERE "+nids+".id IS NULL UNION SELECT * FROM "+ln+";";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}
	else
	{
		sql = "CREATE VIEW "+vn+" AS SELECT * FROM "+ln+";";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	string vw = c.quote_name(tablePrefix+"visibleways");
	string lw = c.quote_name(tablePrefix+"liveways");
	if(parentPrefix.size() > 0)
	{
		string pvw = c.quote_name(parentPrefix+"visibleways");
		string wids = c.quote_name(tablePrefix+"wayids");

		sql = "CREATE VIEW "+vw+" AS SELECT "+pvw+".* FROM "+pvw+" LEFT JOIN "+wids+" ON "+wids+".id = "+pvw+".id WHERE "+wids+".id IS NULL UNION SELECT * FROM "+lw+";";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}
	else
	{
		sql = "CREATE VIEW "+vw+" AS SELECT * FROM "+lw+";";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	string vr = c.quote_name(tablePrefix+"visiblerelations");
	string lr = c.quote_name(tablePrefix+"liverelations");
	if(parentPrefix.size() > 0)
	{
		string pvr = c.quote_name(parentPrefix+"visiblerelations");
		string rids = c.quote_name(tablePrefix+"relationids");

		sql = "CREATE VIEW "+vr+" AS SELECT "+pvr+".* FROM "+pvr+" LEFT JOIN "+rids+" ON "+rids+".id = "+pvr+".id WHERE "+rids+".id IS NULL UNION SELECT * FROM "+lr+";";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}
	else
	{
		sql = "CREATE VIEW "+vr+" AS SELECT * FROM "+lr+";";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	return ok;
}

bool DbUpgradeTables12to13(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &parentPrefix, 
	const std::string &tablePrefix, 
	std::string &errStr)
{
	cout << "DbUpgradeTables12to13" << endl;
	bool ok = true;
	string sql;

	int majorVer=0, minorVer=0;
	DbGetVersion(c, work, majorVer, minorVer);
	string ine = "IF NOT EXISTS ";
	if(majorVer < 9 || (majorVer == 9 && minorVer <= 3))
	{
		ine = "";
	}

	sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"edit_activity")+" (id SERIAL PRIMARY KEY, changeset BIGINT, timestamp BIGINT, uid INTEGER, bbox GEOMETRY(Geometry, 4326), action VARCHAR(16), nodes INT, ways INT, relations INT, existing JSONB, updated JSONB, affectedparents JSONB, related JSONB);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"activity_ts")+" ON "+c.quote_name(tablePrefix+"edit_activity")+" (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"query_activity")+" (id SERIAL PRIMARY KEY, timestamp BIGINT, bbox GEOMETRY(Geometry, 4326), metrics JSONB);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"query_activity_ts")+" ON "+c.quote_name(tablePrefix+"query_activity")+" (timestamp);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "ALTER TABLE "+c.quote_name(tablePrefix+"changesets")+" ALTER COLUMN geom TYPE GEOMETRY(Geometry, 4326);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	return ok;
}

bool DbDowngradeTables13To12(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	cout << "DbDowngradeTables13To12" << endl;
	string sql;
	bool ok = true;

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"edit_activity")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"query_activity")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "ALTER TABLE "+c.quote_name(tablePrefix+"changesets")+" ALTER COLUMN geom TYPE GEOMETRY(Polygon, 4326);";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	return ok;
}


bool DbDowngradeTables12To11(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	cout << "DbDowngradeTables12To11" << endl;
	string sql;
	bool ok = true;

	sql = "ALTER TABLE "+c.quote_name(tablePrefix+"liveways")+" DROP COLUMN bbox CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "ALTER TABLE "+c.quote_name(tablePrefix+"liverelations")+" DROP COLUMN bbox CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "DROP VIEW IF EXISTS "+c.quote_name(tablePrefix+"visiblenodes")+" CASCADE";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "DROP VIEW IF EXISTS "+c.quote_name(tablePrefix+"visibleways")+" CASCADE";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	sql = "DROP VIEW IF EXISTS "+c.quote_name(tablePrefix+"visiblerelations")+" CASCADE";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	return ok;
}

bool DbDowngradeTables11To0(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	string sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"oldnodes")+" CASCADE;";
	bool ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"oldways")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"oldrelations")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"livenodes")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"liveways")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"liverelations")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"nodeids")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"wayids")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relationids")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"way_mems")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relation_mems_n")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relation_mems_w")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"relation_mems_r")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	

	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"nextids")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"meta")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;	
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"changesets")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose);
	sql = "DROP TABLE IF EXISTS "+c.quote_name(tablePrefix+"usernames")+" CASCADE;";
	ok = DbExec(work, sql, errStr, nullptr, verbose);
	return ok;	

}

bool DbSetSchemaVersion(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &parentPrefix, 
	const std::string &tablePrefix, 
	int targetVer, bool latest, std::string &errStr)
{
	cout << "DbSetSchemaVersion " << parentPrefix << "," << tablePrefix << "," << targetVer << endl;
	int schemaVersion = 0;
	try
	{
		schemaVersion = std::stoi(DbGetMetaValue(c, work, "schema_version", tablePrefix, errStr));
	}
	catch (runtime_error &err) {}
	cout << "Starting schema version" << schemaVersion << endl;

	if(latest)
		targetVer = 13;
	bool ok = true;

	//Upgrading
	if(schemaVersion == 0 and targetVer>schemaVersion)
	{
		ok = DbUpgradeTables0to11(c, work, 
			verbose, 
			tablePrefix, 
			errStr);
		if(!ok) return false;

		ok = DbSetMetaValue(c, work, "schema_version", to_string(11), tablePrefix, errStr);
		if(!ok) return false;
		schemaVersion = 11;
	}

	if(schemaVersion == 11 and targetVer>schemaVersion)
	{
		ok = DbUpgradeTables11to12(c, work, 
			verbose, 
			parentPrefix, tablePrefix, 
			errStr);
		if(!ok) return false;

		ok = DbSetMetaValue(c, work, "schema_version", to_string(12), tablePrefix, errStr);
		if(!ok) return false;
		schemaVersion = 12;
	}

	if(schemaVersion == 12 and targetVer>schemaVersion)
	{
		ok = DbUpgradeTables12to13(c, work, 
			verbose, 
			parentPrefix, tablePrefix, 
			errStr);
		if(!ok) return false;

		ok = DbSetMetaValue(c, work, "schema_version", to_string(13), tablePrefix, errStr);
		if(!ok) return false;
		schemaVersion = 13;
	}

	//Downgrading
	if(targetVer < 13 and schemaVersion == 13)
	{
		ok = DbDowngradeTables13To12(c, work, 
			verbose, 
			tablePrefix, 
			errStr);
		if(!ok) return false;

		ok = DbSetMetaValue(c, work, "schema_version", to_string(12), tablePrefix, errStr);
		if(!ok) return false;

		schemaVersion = 12;
	}

	if(targetVer < 12 and schemaVersion == 12)
	{
		ok = DbDowngradeTables12To11(c, work, 
			verbose, 
			tablePrefix, 
			errStr);
		if(!ok) return false;

		ok = DbSetMetaValue(c, work, "schema_version", to_string(11), tablePrefix, errStr);
		if(!ok) return false;

		schemaVersion = 11;
	}

	if(targetVer < 11 and schemaVersion == 11)
	{
		ok = DbDowngradeTables11To0(c, work, 
			verbose, 
			tablePrefix, 
			errStr);
		if(!ok) return false;
		schemaVersion = 0;
	}

	return true;
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

bool DbCreateBboxIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	bool ok = true;
	string sql;
	int majorVer=0, minorVer=0;
	DbGetVersion(c, work, majorVer, minorVer);
	string ine = "IF NOT EXISTS ";

	if(!DbCheckIndexExists(c, work, tablePrefix+"livenodes_gix_tags"))
	{
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"livenodes_gix_tags")+" ON "+c.quote_name(tablePrefix+"livenodes")+" USING GIST (geom, to_tsvector('english', tags));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "VACUUM ANALYZE "+c.quote_name(tablePrefix+"livenodes_gix_tags")+"(geom, tags);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	if(!DbCheckIndexExists(c, work, tablePrefix+"liveways_gix_tags"))
	{
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"liveways_gix_tags")+" ON "+c.quote_name(tablePrefix+"liveways")+" USING GIST (bbox, to_tsvector('english', tags));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "VACUUM ANALYZE "+c.quote_name(tablePrefix+"liveways_gix_tags")+"(bbox, tags);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	if(!DbCheckIndexExists(c, work, tablePrefix+"liverelations_gix_tags"))
	{
		sql = "CREATE INDEX "+ine+c.quote_name(tablePrefix+"liverelations_gix_tags")+" ON "+c.quote_name(tablePrefix+"liverelations")+" USING GIST (bbox, to_tsvector('english', tags));";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

		sql = "VACUUM ANALYZE "+c.quote_name(tablePrefix+"liverelations_gix_tags")+"(bbox, tags);";
		ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;
	}

	return ok;
}

bool DbDropBboxIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	bool ok = true;
	string sql;
	int majorVer=0, minorVer=0;
	DbGetVersion(c, work, majorVer, minorVer);
	string ie = "IF EXISTS ";

	sql = "DROP INDEX "+ie+c.quote_name(tablePrefix+"livenodex_gix2")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "DROP INDEX "+ie+c.quote_name(tablePrefix+"liveways_gix")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

	sql = "DROP INDEX "+ie+c.quote_name(tablePrefix+"liverelations_gix")+";";
	ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

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
	class PgCommon *pgCommon,
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
				pgCommon,
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
			LoadFromOsmChangeXml(sb, data.get());

			for(size_t i=0; i<data->blocks.size(); i++)
			{
				cout << data->actions[i] << endl;
				class OsmData &block = data->blocks[i];

				//Set visibility flag depending on action
				bool isCreate = data->actions[i] == "delete";
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

				std::set<int64_t> waysToUpdate, relsToUpdate;
				for(size_t j=0; j<block.ways.size(); j++)
				{
					if (block.ways[j].objId <= 0) throw runtime_error("ID should not be zero or negative");
					waysToUpdate.insert(block.ways[j].objId);
				}
				for(size_t j=0; j<block.relations.size(); j++)
				{
					if (block.relations[j].objId <= 0) throw runtime_error("ID should not be zero or negative");
					relsToUpdate.insert(block.relations[j].objId);
				}

				if(!isCreate)
				{
					//Get affected parent objects
					std::shared_ptr<class OsmData> affectedParents = make_shared<class OsmData>();

					pgCommon->GetAffectedParents2(block, affectedParents);

					//Ensure a copy of affected parents is in the active table
					std::map<int64_t, int64_t> unusedNodeIds, unusedWayIds, unusedRelationIds;
					bool ok = ::StoreObjects(c, work, tableModPrefix, *affectedParents.get(), 
						unusedNodeIds, unusedWayIds, unusedRelationIds, errStr);

					for(size_t j=0; j<affectedParents->ways.size(); j++)
						waysToUpdate.insert(affectedParents->ways[j].objId);
					for(size_t j=0; j<affectedParents->relations.size(); j++)
						relsToUpdate.insert(affectedParents->relations[j].objId);
				}

				//Update bboxes of modified and parent ways
				int ret = ::UpdateWayBboxesById(c, work,
					waysToUpdate,
					0,
					tableModPrefix, 
					errStr);

				//Update relation bboxes
				ret = ::UpdateRelationBboxesById(c, work,
					relsToUpdate,
					0,
					tableModPrefix, 
					errStr);
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
		GetVisibleObjectsById(c, work, dbUsernameLookup,
			nodeActivePrefix, 
			"node", nodeIds, it, 
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

// ***************************************************************************

int DbUpdateWayBboxes(pqxx::connection &c, pqxx::transaction_base *work,
    int verbose,
	const std::string &tablePrefix, 
	class PgCommon *adminObj,
	std::string &errStr)
{
/*
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 Update on planet2_static_liveways  (cost=0.00..11730376118.25 rows=231463328 width=360) (actual time=721416785.010..721416785.010 rows=0 loops=1)
   ->  Seq Scan on planet2_static_liveways  (cost=0.00..11730376118.25 rows=231463328 width=360) (actual time=255.928..623747962.059 rows=123746659 loops=1)
         SubPlan 2
           ->  Index Scan using planet2_static_livenodes_pkey on planet2_static_livenodes  (cost=1.09..50.64 rows=10 width=32) (actual time=1.106..4.865 rows=12 loops=123746659)
                 Index Cond: (id = ANY ((($1)::text[])::bigint[]))
                 InitPlan 1 (returns $1)
                   ->  Result  (cost=0.00..0.51 rows=100 width=0) (actual time=0.011..0.014 rows=13 loops=123746659)
 Planning time: 472.667 ms
 Execution time: 721416787.111 ms
*/

    string sql = "UPDATE "+tablePrefix+"liveways SET bbox=ST_Envelope(ST_Union(ARRAY(SELECT geom FROM "+tablePrefix+"visiblenodes WHERE "+tablePrefix+"visiblenodes.id::bigint = ANY(ARRAY(SELECT jsonb_array_elements("+tablePrefix+"liveways.members))::text[]::bigint[]))));";
	cout << sql << endl;

	work->exec(sql);

	return 1;	
}

int DbUpdateRelationBboxes(pqxx::connection &conn, pqxx::transaction_base *work,
    int verbose,
	const std::string &tablePrefix, 
	class PgCommon *adminObj,
	std::string &errStr)
{
	string objTable = conn.quote_name(tablePrefix + "liverelations");
	std::set<int64_t> relIds;

	for(int co=0; co<10; co++)
	{
		stringstream sql;
		sql << "SELECT " << objTable << ".* FROM ";
		sql << objTable;
		sql << ";";

		//cout << sql.str() << endl;

		int step = 100;
		pqxx::icursorstream cursor( *work, sql.str(), "relcursor", step );	

		while(true)
		{
			pqxx::result rows;
			cursor.get(rows);
			if ( rows.empty() )
				break;

			int idCol = rows.column_number("id");

			for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {
				int64_t objId = c[idCol].as<int64_t>();
				relIds.insert(objId);
			}
		}
	}

	UpdateRelationBboxesById(conn, work,
		relIds,
		verbose,
		tablePrefix, 
		errStr);

	return 1;	
}

