#include "dbadmin.h"
#include "dbids.h"
#include "dbcommon.h"
#include "dbstore.h"
#include "dbdecode.h"
#include "dbquery.h"
#include "dbusername.h"
#include "dbmeta.h"
#include "dbeditactivity.h"
#include "util.h"
#include "cppGzip/DecodeGzip.h"
#include "cppo5m/io.h"
#include <map>
#include <set>
#include <boost/filesystem.hpp>
#include <fstream>
#include <optional>
#include "dbjson.h"
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
	string j = "JSONB";

	string sql = "CREATE TABLE IF NOT EXISTS "+c.quote_name(tablePrefix+"oldnodes")+" (id BIGINT, changeset BIGINT, changeset_index SMALLINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, tags "+j+", geom GEOMETRY(Point, 4326));";
	bool ok = DbExec(work, sql, errStr, nullptr, verbose); if(!ok) return ok;

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

static bool DbDefineVisibleViews(pqxx::connection &c, pqxx::transaction_base *work, int verbose,
	const std::string &parentPrefix, const std::string &tablePrefix, bool distinct, std::string &errStr);

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

	// As tables were then: rows compared to remove duplicates
	return DbDefineVisibleViews(c, work, verbose, parentPrefix, tablePrefix, true, errStr);
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

	string ine = "IF NOT EXISTS ";

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

// Extracts store independent current states, not static/mod overlays.
bool DbCreateExtractTables(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, string &errStr)
{
	string metadata = c.quote_name(tablePrefix + "extracts");
	string sql = "CREATE TABLE IF NOT EXISTS " + metadata +
		" (id BIGSERIAL PRIMARY KEY, name TEXT, bbox GEOMETRY(Polygon, 4326) NOT NULL, "
		"use_bbox_in_query BOOLEAN NOT NULL DEFAULT false, "
		"performed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, "
		"edit_activity_id BIGINT CHECK (edit_activity_id >= 0), "
		"atomic_edit_id BIGINT CHECK (atomic_edit_id >= 0), "
		"auto_update BOOLEAN NOT NULL DEFAULT false, "
		"update_url TEXT NOT NULL DEFAULT '');";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;

	// Columns added to schema 14 after it was first in use: whether the extract
	// is to be kept up to date automatically, and the API to update it from,
	// blank meaning this map. An extracts table made before then gains them here.
	sql = "ALTER TABLE " + metadata +
		" ADD COLUMN IF NOT EXISTS auto_update BOOLEAN NOT NULL DEFAULT false, "
		"ADD COLUMN IF NOT EXISTS update_url TEXT NOT NULL DEFAULT '';";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;

	// Match live map object columns, adding extract_id to scope every object.
	string common = " (extract_id BIGINT NOT NULL REFERENCES " + metadata +
		"(id) ON DELETE CASCADE, id BIGINT NOT NULL, changeset BIGINT, "
		"changeset_index SMALLINT, username TEXT, uid INTEGER, timestamp BIGINT, "
		"version INTEGER NOT NULL, tags JSONB, ";
	for(const char *type : {"node", "way", "relation"})
	{
		string kind(type);
		string table = c.quote_name(tablePrefix + "extract_live" + kind + "s");
		string columns;
		if(kind == "node") columns = "geom GEOMETRY(Point, 4326)";
		else if(kind == "way") columns = "members JSONB, bbox GEOMETRY(Geometry, 4326)";
		else columns = "members JSONB, memberroles JSONB, bbox GEOMETRY(Geometry, 4326)";
		sql = "CREATE TABLE IF NOT EXISTS " + table + common + columns +
			", PRIMARY KEY (extract_id, id));";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		sql = "CREATE INDEX IF NOT EXISTS " + c.quote_name(tablePrefix + "extract_live" + kind + "s_gix") +
			" ON " + table + " USING GIST (" + (kind == "node" ? "geom" : "bbox") + ");";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	}

	// Keep reverse membership lookups within an extract. Do not require every
	// relation member to exist: map queries do not return complete relations.
	for(const char *suffix : {"way_mems", "relation_mems_n", "relation_mems_w", "relation_mems_r"})
	{
		string table = c.quote_name(tablePrefix + "extract_" + suffix);
		string owner = string(suffix) == "way_mems" ? "way" : "relation";
		sql = "CREATE TABLE IF NOT EXISTS " + table +
			" (extract_id BIGINT NOT NULL, id BIGINT NOT NULL, version INTEGER NOT NULL, "
			"index INTEGER NOT NULL, member BIGINT NOT NULL, PRIMARY KEY (extract_id, id, index), "
			"FOREIGN KEY (extract_id, id) REFERENCES " +
			c.quote_name(tablePrefix + "extract_live" + owner + "s") + "(extract_id, id) ON DELETE CASCADE);";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		sql = "CREATE INDEX IF NOT EXISTS " + c.quote_name(tablePrefix + "extract_" + suffix + "_member_idx") +
			" ON " + table + " (extract_id, member);";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	}
	return true;
}

bool DbDropExtractTables(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, string &errStr)
{
	for(const char *name : {"extract_way_mems", "extract_relation_mems_n", "extract_relation_mems_w",
		"extract_relation_mems_r", "extract_livenodes", "extract_liveways", "extract_liverelations", "extracts"})
	{
		string sql = "DROP TABLE IF EXISTS " + c.quote_name(tablePrefix + name) + " CASCADE;";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	}
	return true;
}

// An index on each box column of edit_activity, as version 14 finds the
// records concerning an area with; or with enabled false, none.
static bool DbSetActivityBoxIndexes(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, bool enabled, string &errStr)
{
	for(const char *column : {"existing_bbox", "updated_bbox",
		"affectedparents_before_bbox", "affectedparents_after_bbox", "relation_reach_bbox"})
	{
		string index = c.quote_name(tablePrefix + "activity_gix_" + column);
		string sql = enabled ? "CREATE INDEX IF NOT EXISTS " + index + " ON " +
			c.quote_name(tablePrefix + "edit_activity") + " USING GIST (" + column + ");" :
			"DROP INDEX IF EXISTS " + index + ";";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	}
	return true;
}

// What schema 14 has gained since it was introduced. Every step leaves alone
// whatever is already there, so it both finishes an upgrade to version 14 and
// brings an older version 14 up to date.
bool DbAmendSchema14(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, bool boxIndexes, string &errStr)
{
	// related_parents: the relations holding an edit's related nodes. The four
	// bbox columns: the boxes that go with the lists of existing, updated and
	// affected parent objects, each a collection with one geometry for every
	// object of its list. See EditActivity. Rows from before a column existed
	// keep NULL there, meaning not recorded.
	string box = " GEOMETRY(GeometryCollection, 4326)";
	string sql = "ALTER TABLE " + c.quote_name(tablePrefix + "edit_activity") +
		" ADD COLUMN IF NOT EXISTS related_parents JSONB,"
		" ADD COLUMN IF NOT EXISTS existing_bbox" + box + ","
		" ADD COLUMN IF NOT EXISTS updated_bbox" + box + ","
		" ADD COLUMN IF NOT EXISTS affectedparents_before_bbox" + box + ","
		" ADD COLUMN IF NOT EXISTS affectedparents_after_bbox" + box + ","
		// Where an edited relation matters beyond its own box: the extent of
		// the ways sharing its member nodes. NULL when there is nowhere more.
		" ADD COLUMN IF NOT EXISTS relation_reach_bbox GEOMETRY(Polygon, 4326),"
		// Four columns that schema 14 began with and nothing came to use: the
		// lists of existing objects and these boxes say what they were for.
		// The checks that tied them together go with them.
		" DROP COLUMN IF EXISTS sync_before, DROP COLUMN IF EXISTS bbox_before,"
		" DROP COLUMN IF EXISTS sync_after, DROP COLUMN IF EXISTS bbox_after,"
		// The table's original box of a whole edit, which nothing ever filled in
		" DROP COLUMN IF EXISTS bbox;";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	// Finding the records of a changeset, as its change counts do, without
	// reading the whole table
	sql = "CREATE INDEX IF NOT EXISTS " + c.quote_name(tablePrefix + "activity_changeset") +
		" ON " + c.quote_name(tablePrefix + "edit_activity") + " (changeset);";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	// Finding the records that concern an area, which tests each of these.
	// Version 15 has one index on the footprint in their place, so they are
	// not made again for a map at 15.
	if(boxIndexes && !DbSetActivityBoxIndexes(c, work, verbose, tablePrefix, true, errStr))
		return false;
	return DbCreateExtractTables(c, work, verbose, tablePrefix, errStr);
}

// The member tables, with the kind of object that owns each one's rows
static const char *MEMBER_TABLES[4][2] = {{"way_mems", "way"}, {"relation_mems_n", "relation"},
	{"relation_mems_w", "relation"}, {"relation_mems_r", "relation"}};

// The views of the objects a table set shows: its own live objects, and its
// parent's except those it has taken over, or for a table set with no
// parent, its live objects alone. distinct says whether rows are also
// compared with each other to remove duplicates, as before version 15.
//
// The columns are named, as the table set's live table has them, rather
// than taken as *: a table set and its parent are brought from one schema
// version to the next one after the other, so while one is being changed
// they can have different columns. Defined again, a view shows what columns
// have been added since; a view's * would stand for the columns there were
// when it was made.
static bool DbDefineVisibleViews(pqxx::connection &c, pqxx::transaction_base *work, int verbose,
	const std::string &parentPrefix, const std::string &tablePrefix, bool distinct, std::string &errStr)
{
	for(const char *kind : {"node", "way", "relation"})
	{
		string view = c.quote_name(tablePrefix + "visible" + kind + "s");
		string live = c.quote_name(tablePrefix + "live" + kind + "s");
		string sql;
		if(parentPrefix.empty())
			sql = "CREATE OR REPLACE VIEW " + view + " AS SELECT * FROM " + live + ";";
		else
		{
			string parent = c.quote_name(parentPrefix + "visible" + kind + "s");
			string ids = c.quote_name(tablePrefix + kind + "ids");
			pqxx::result r = work->exec("SELECT attname FROM pg_attribute WHERE attrelid = to_regclass(" +
				work->quote(live) + ") AND attnum > 0 AND NOT attisdropped ORDER BY attnum");
			string ownColumns, parentColumns;
			for(size_t i=0; i<r.size(); i++)
			{
				string column = c.quote_name(r[i][0].as<string>());
				ownColumns += (i ? ", " : "") + column;
				parentColumns += (i ? ", " : "") + parent + "." + column;
			}
			sql = "CREATE OR REPLACE VIEW " + view + " AS SELECT " + parentColumns + " FROM " + parent +
				" LEFT JOIN " + ids + " ON " + ids + ".id = " + parent + ".id WHERE " + ids + ".id IS NULL " +
				(distinct ? "UNION" : "UNION ALL") + " SELECT " + ownColumns + " FROM " + live + ";";
		}
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	}
	return true;
}

// The shapes an extract's boundary may have: a polygon, or from schema 15 a
// multipolygon too, such as a region with islands. Going back to a polygon
// column is refused while any extract has a multipolygon, which it could not
// hold.
static bool DbSetExtractBoundaryShapes(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &tablePrefix, bool multipolygons, std::string &errStr)
{
	string name = tablePrefix + "extracts";
	pqxx::result column = work->exec("SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
		"WHERE a.attrelid = to_regclass(" + work->quote(c.quote_name(name)) + ") AND a.attname = 'bbox'");
	if(column.empty()) return true; // No extract tables here
	string table = c.quote_name(name);
	string check = c.quote_name(tablePrefix + "extracts_bbox_shape");
	string sql;
	if(multipolygons)
	{
		if(column[0][0].as<string>() != "geometry(Geometry,4326)")
		{
			sql = "ALTER TABLE " + table + " ALTER COLUMN bbox TYPE GEOMETRY(Geometry, 4326);";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		}
		sql = "ALTER TABLE " + table + " DROP CONSTRAINT IF EXISTS " + check + ", ADD CONSTRAINT " + check +
			" CHECK (GeometryType(bbox) IN ('POLYGON', 'MULTIPOLYGON'));";
		return DbExec(work, sql, errStr, nullptr, verbose);
	}

	pqxx::result multi = work->exec("SELECT id FROM " + table + " WHERE GeometryType(bbox) <> 'POLYGON' ORDER BY id");
	if(!multi.empty())
	{
		errStr = "Extracts " + multi[0][0].as<string>();
		for(pqxx::result::size_type i = 1; i < multi.size() && i < 10; i++)
			errStr += ", " + multi[i][0].as<string>();
		errStr += string(multi.size() > 10 ? " and more" : "") + " in " + name + " have a multipolygon boundary, "
			"which schema 14 cannot hold; delete them before going back to it";
		return false;
	}
	sql = "ALTER TABLE " + table + " DROP CONSTRAINT IF EXISTS " + check + ";";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	sql = "ALTER TABLE " + table + " ALTER COLUMN bbox TYPE GEOMETRY(Polygon, 4326);";
	return DbExec(work, sql, errStr, nullptr, verbose);
}

// Edits announce themselves: when rows are added to edit_activity, PostgreSQL
// sends a notification on the channel <prefix>edits, with the highest edit
// activity ID and atomic edit ID added, as JSON. It goes out when the
// transaction commits, and not at all if it is rolled back, so whoever hears it
// can read the edits at once. Only connections listening at the time hear it;
// see notify/ in the parent project, which passes it on to clients.
static bool DbSetEditNotifications(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &tablePrefix, bool enabled, std::string &errStr)
{
	string table = tablePrefix + "edit_activity";
	if(work->exec("SELECT to_regclass(" + work->quote(c.quote_name(table)) + ")")[0][0].is_null())
		return true; // No edit activity here
	string function = c.quote_name(tablePrefix + "notify_edits");
	string sql = "DROP TRIGGER IF EXISTS " + c.quote_name(tablePrefix + "notify_edits") + " ON " + c.quote_name(table) + ";";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	if(!enabled)
	{
		sql = "DROP FUNCTION IF EXISTS " + function + "();";
		return DbExec(work, sql, errStr, nullptr, verbose);
	}
	// Once for each statement, however many rows it adds: an upload adds
	// several at a time
	sql = "CREATE OR REPLACE FUNCTION " + function + "() RETURNS trigger LANGUAGE plpgsql AS $$\n"
		"BEGIN\n"
		"  PERFORM pg_notify(" + work->quote(tablePrefix + "edits") + ", (SELECT json_build_object("
		"'edit_activity_id', max(id), 'atomic_edit_id', max(atomic_edit_id))::text FROM added));\n"
		"  RETURN NULL;\n"
		"END\n"
		"$$;";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	sql = "CREATE TRIGGER " + c.quote_name(tablePrefix + "notify_edits") + " AFTER INSERT ON " + c.quote_name(table) +
		" REFERENCING NEW TABLE AS added FOR EACH STATEMENT EXECUTE FUNCTION " + function + "();";
	return DbExec(work, sql, errStr, nullptr, verbose);
}

// The footprint of each edit activity record: the rectangle around all its
// boxes (the four collections and relation_reach_bbox), or the whole world
// when one of the collections was not recorded, as such a record may concern
// anywhere. It is worked out by PostgreSQL as each row is written, and has
// one GiST index together with the ID, so finding the records of a range of
// IDs that may concern an area is one index scan, in place of a test of each
// row against five columns. The exact test of the boxes follows, on the rows
// this finds. The footprint takes the place of the indexes on the separate
// boxes; with enabled false, they come back.
//
// btree_gist provides the ID's part of the index. It is a trusted extension,
// which the owner of the database may create.
static bool DbSetEditFootprint(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &tablePrefix, bool enabled, std::string &errStr)
{
	string table = tablePrefix + "edit_activity";
	if(work->exec("SELECT to_regclass(" + work->quote(c.quote_name(table)) + ")")[0][0].is_null())
		return true; // No edit activity here
	string sql;
	if(!enabled)
	{
		// The index goes with the column
		sql = "ALTER TABLE " + c.quote_name(table) + " DROP COLUMN IF EXISTS footprint;";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		return DbSetActivityBoxIndexes(c, work, verbose, tablePrefix, true, errStr);
	}
	sql = "ALTER TABLE " + c.quote_name(table) + " ADD COLUMN IF NOT EXISTS footprint GEOMETRY(Geometry, 4326) "
		"GENERATED ALWAYS AS (CASE WHEN existing_bbox IS NULL OR updated_bbox IS NULL "
		"OR affectedparents_before_bbox IS NULL OR affectedparents_after_bbox IS NULL "
		"THEN ST_MakeEnvelope(-180, -90, 180, 90, 4326) "
		"ELSE ST_Envelope(ST_Collect(ARRAY[existing_bbox, updated_bbox, affectedparents_before_bbox, "
		"affectedparents_after_bbox, relation_reach_bbox::geometry])) END) STORED;";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	sql = "CREATE EXTENSION IF NOT EXISTS btree_gist;";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	sql = "CREATE INDEX IF NOT EXISTS " + c.quote_name(tablePrefix + "activity_gix_footprint") +
		" ON " + c.quote_name(table) + " USING GIST (id, footprint);";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	return DbSetActivityBoxIndexes(c, work, verbose, tablePrefix, false, errStr);
}

// What schema 15 has gained since it was introduced, as for schema 14: every
// step leaves alone whatever is already there.
bool DbAmendSchema15(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &tablePrefix, std::string &errStr)
{
	if(!DbSetExtractBoundaryShapes(c, work, verbose, tablePrefix, true, errStr)) return false;
	if(!DbSetEditFootprint(c, work, verbose, tablePrefix, true, errStr)) return false;
	return DbSetEditNotifications(c, work, verbose, tablePrefix, true, errStr);
}

// Version 15:
//  - An extract's boundary may be a multipolygon as well as a polygon (see
//    DbSetExtractBoundaryShapes), edits are announced with NOTIFY (see
//    DbSetEditNotifications), and each edit activity record has a footprint,
//    indexed with its ID (see DbSetEditFootprint), in place of an index on
//    each of its boxes. These came after version 15 was first in use,
//    so DbAmendSchema15 brings a map already at 15 up to date.
//  - The member tables (way_mems, relation_mems_*) hold the members of the
//    live version of each way and relation, and nothing else. Until now rows
//    were added for every version stored and never removed, and nothing
//    stopped the same row being added twice. Only the live version's rows
//    are ever looked up; an old version's members remain in its history row.
//    The tables are rebuilt here from the live tables, which are the
//    authority, and given a primary key of (id, version, index).
//  - The visible* views no longer compare whole rows to remove duplicates
//    (UNION ALL in place of UNION): the two halves cannot overlap.
bool DbUpgradeTables14to15(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &parentPrefix, const std::string &tablePrefix, std::string &errStr)
{
	for(const auto &entry : MEMBER_TABLES)
	{
		string suffix = entry[0], owner = entry[1];
		string table = c.quote_name(tablePrefix + suffix);
		string live = c.quote_name(tablePrefix + "live" + owner + "s");
		string memberIndex = tablePrefix + suffix + "_mids";
		bool hadIndex = !work->exec("SELECT to_regclass(" + work->quote(c.quote_name(memberIndex)) + ")")[0][0].is_null();

		// Filling an empty table and indexing it afterwards is far quicker
		// than deleting from a full one
		string sql = "DROP INDEX IF EXISTS " + c.quote_name(memberIndex) + ";";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		sql = "TRUNCATE " + table + ";";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		if(owner == "way")
			sql = "INSERT INTO " + table + " (id, version, index, member) SELECT o.id, o.version, "
				"(m.ordinality-1)::integer, m.value::text::bigint FROM " + live +
				" o CROSS JOIN LATERAL jsonb_array_elements(o.members) WITH ORDINALITY m;";
		else
			sql = "INSERT INTO " + table + " (id, version, index, member) SELECT o.id, o.version, "
				"(m.ordinality-1)::integer, (m.value->>1)::bigint FROM " + live +
				" o CROSS JOIN LATERAL jsonb_array_elements(o.members) WITH ORDINALITY m "
				"WHERE m.value->>0 = " + work->quote(string(1, suffix.back()) == "n" ? string("node") :
					(suffix.back() == 'w' ? string("way") : string("relation"))) + ";";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		sql = "ALTER TABLE " + table + " ADD CONSTRAINT " + c.quote_name(tablePrefix + suffix + "_key") +
			" PRIMARY KEY (id, version, index);";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		if(hadIndex)
		{
			sql = "CREATE INDEX " + c.quote_name(memberIndex) + " ON " + table + " (member);";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		}
	}
	return DbDefineVisibleViews(c, work, verbose, parentPrefix, tablePrefix, false, errStr);
}

// The member tables keep what they hold, which the earlier version reads
// just as well; only the key and the form of the views go back.
bool DbDowngradeTables15To14(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &parentPrefix, const std::string &tablePrefix, std::string &errStr)
{
	if(!DbSetExtractBoundaryShapes(c, work, verbose, tablePrefix, false, errStr)) return false;
	if(!DbSetEditNotifications(c, work, verbose, tablePrefix, false, errStr)) return false;
	if(!DbSetEditFootprint(c, work, verbose, tablePrefix, false, errStr)) return false;
	for(const auto &entry : MEMBER_TABLES)
	{
		string suffix = entry[0];
		string sql = "ALTER TABLE " + c.quote_name(tablePrefix + suffix) + " DROP CONSTRAINT IF EXISTS " +
			c.quote_name(tablePrefix + suffix + "_key") + ";";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		for(const char *column : {"id", "version", "index"})
		{
			sql = "ALTER TABLE " + c.quote_name(tablePrefix + suffix) + " ALTER COLUMN " + column + " DROP NOT NULL;";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		}
	}
	return DbDefineVisibleViews(c, work, verbose, parentPrefix, tablePrefix, true, errStr);
}

// Version 16: every version of every object records the atomic edit that
// made it, as atomic_edit_id in the live and history tables, the same ID as
// the edit's activity records. Versions from before this, the imported map
// in the static tables, and copies of versions the map already had (as an
// edit makes of the ways and relations holding what it changes) have NULL.
// The views of visible objects are defined again to show the column.
bool DbUpgradeTables15to16(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &parentPrefix, const std::string &tablePrefix, std::string &errStr)
{
	for(const char *kind : {"node", "way", "relation"})
		for(const char *table : {"live", "old"})
		{
			string sql = "ALTER TABLE " + c.quote_name(tablePrefix + table + kind + "s") +
				" ADD COLUMN IF NOT EXISTS atomic_edit_id BIGINT;";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		}
	return DbDefineVisibleViews(c, work, verbose, parentPrefix, tablePrefix, false, errStr);
}

// The column goes, and with it the views showing it: this table set's, and
// those of any table set above it, whose own step defines them again (the
// table sets are taken from the static ones up).
bool DbDowngradeTables16To15(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &parentPrefix, const std::string &tablePrefix, std::string &errStr)
{
	for(const char *kind : {"node", "way", "relation"})
		for(const char *table : {"live", "old"})
		{
			string sql = "ALTER TABLE " + c.quote_name(tablePrefix + table + kind + "s") +
				" DROP COLUMN IF EXISTS atomic_edit_id CASCADE;";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		}
	return DbDefineVisibleViews(c, work, verbose, parentPrefix, tablePrefix, false, errStr);
}

// Legacy rows remain ungrouped: timestamps cannot recover upload boundaries.
bool DbUpgradeTables13to14(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, string &errStr)
{
	string table = c.quote_name(tablePrefix + "edit_activity");
	string sql = "CREATE SEQUENCE " + c.quote_name(tablePrefix + "atomic_edit_id_seq") + ";";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	sql = "ALTER TABLE " + table + " ADD COLUMN atomic_edit_id BIGINT, ADD COLUMN block_index BIGINT, "
		"ADD CHECK ((atomic_edit_id IS NULL AND block_index IS NULL) OR "
		"(atomic_edit_id IS NOT NULL AND atomic_edit_id > 0 AND block_index IS NOT NULL AND block_index >= 0));";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	sql = "CREATE UNIQUE INDEX " + c.quote_name(tablePrefix + "activity_atomic_block") +
		" ON " + table + " (atomic_edit_id, block_index);";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	return DbAmendSchema14(c, work, verbose, tablePrefix, true, errStr);
}

bool DbDowngradeTables14To13(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, string &errStr)
{
	if(!DbDropExtractTables(c, work, verbose, tablePrefix, errStr)) return false;
	string sql = "DROP INDEX IF EXISTS " + c.quote_name(tablePrefix + "activity_changeset") + ";";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	// The box indices go with their columns, below
	sql = "ALTER TABLE " + c.quote_name(tablePrefix + "edit_activity") +
		" DROP COLUMN IF EXISTS related_parents, DROP COLUMN IF EXISTS existing_bbox,"
		" DROP COLUMN IF EXISTS updated_bbox, DROP COLUMN IF EXISTS affectedparents_before_bbox,"
		" DROP COLUMN IF EXISTS affectedparents_after_bbox,"
		" DROP COLUMN IF EXISTS relation_reach_bbox,"
		" DROP COLUMN atomic_edit_id, DROP COLUMN block_index,"
		" DROP COLUMN IF EXISTS sync_before, DROP COLUMN IF EXISTS bbox_before,"
		" DROP COLUMN IF EXISTS sync_after, DROP COLUMN IF EXISTS bbox_after,"
		// As the table had it before version 14
		" ADD COLUMN IF NOT EXISTS bbox GEOMETRY(Geometry, 4326);";
	if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	sql = "DROP SEQUENCE " + c.quote_name(tablePrefix + "atomic_edit_id_seq") + ";";
	return DbExec(work, sql, errStr, nullptr, verbose);
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

bool DbDropMapTables(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, string &errStr)
{
	if(!DbDropExtractTables(c, work, verbose, tablePrefix, errStr)) return false;
	// Use exact known names rather than prefix matching, which could remove
	// unrelated tables. IF EXISTS supports partial or mismatched schemas.
	for(const char *name : {"visiblenodes", "visibleways", "visiblerelations"})
	{
		string sql = "DROP VIEW IF EXISTS " + c.quote_name(tablePrefix + name) + " CASCADE;";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	}
	for(const char *name : {"edit_activity", "query_activity", "oldnodes", "oldways",
		"oldrelations", "livenodes", "liveways", "liverelations", "nodeids", "wayids",
		"relationids", "way_mems", "relation_mems_n", "relation_mems_w",
		"relation_mems_r", "nextids", "changesets", "usernames", "meta"})
	{
		string sql = "DROP TABLE IF EXISTS " + c.quote_name(tablePrefix + name) + " CASCADE;";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	}
	string sql = "DROP SEQUENCE IF EXISTS " + c.quote_name(tablePrefix + "atomic_edit_id_seq") + ";";
	return DbExec(work, sql, errStr, nullptr, verbose);
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
		targetVer = MAP_SCHEMA_VERSION;
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

	if(schemaVersion == 13 and targetVer > schemaVersion)
	{
		if(!DbUpgradeTables13to14(c, work, verbose, tablePrefix, errStr)) return false;
		if(!DbSetMetaValue(c, work, "schema_version", "14", tablePrefix, errStr)) return false;
		schemaVersion = 14;
	}

	if(schemaVersion >= 14 and targetVer >= schemaVersion)
	{
		// Schema 14 has gained columns since it was introduced. Every step of
		// creating its extract tables leaves alone whatever is already there,
		// so asking for the version a map already has, or a later one, brings
		// an older version 14 up to date.
		if(!DbAmendSchema14(c, work, verbose, tablePrefix, schemaVersion < 15, errStr)) return false;
	}

	if(schemaVersion == 14 and targetVer > schemaVersion)
	{
		if(!DbUpgradeTables14to15(c, work, verbose, parentPrefix, tablePrefix, errStr)) return false;
		if(!DbSetMetaValue(c, work, "schema_version", "15", tablePrefix, errStr)) return false;
		schemaVersion = 15;
	}

	if(schemaVersion >= 15 and targetVer >= schemaVersion)
	{
		// As for schema 14: what version 15 has gained since it was introduced
		if(!DbAmendSchema15(c, work, verbose, tablePrefix, errStr)) return false;
	}

	if(schemaVersion == 15 and targetVer > schemaVersion)
	{
		if(!DbUpgradeTables15to16(c, work, verbose, parentPrefix, tablePrefix, errStr)) return false;
		if(!DbSetMetaValue(c, work, "schema_version", "16", tablePrefix, errStr)) return false;
		schemaVersion = 16;
	}

	//Downgrading
	if(targetVer < 16 and schemaVersion == 16)
	{
		if(!DbDowngradeTables16To15(c, work, verbose, parentPrefix, tablePrefix, errStr)) return false;
		if(!DbSetMetaValue(c, work, "schema_version", "15", tablePrefix, errStr)) return false;
		schemaVersion = 15;
	}
	if(targetVer < 15 and schemaVersion == 15)
	{
		if(!DbDowngradeTables15To14(c, work, verbose, parentPrefix, tablePrefix, errStr)) return false;
		if(!DbSetMetaValue(c, work, "schema_version", "14", tablePrefix, errStr)) return false;
		schemaVersion = 14;
	}
	if(targetVer < 14 and schemaVersion == 14)
	{
		if(!DbDowngradeTables14To13(c, work, verbose, tablePrefix, errStr)) return false;
		if(!DbSetMetaValue(c, work, "schema_version", "13", tablePrefix, errStr)) return false;
		schemaVersion = 13;
	}

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

// ************* Importing a map file straight into the tables *************


// Receives the objects of a map file and writes them to one set of map
// tables with COPY, which is the fast way into PostgreSQL. The data goes
// through the connection, so no files are written first and the database
// user need not be a superuser. The tables must have been created and
// should be empty.
//
// A connection carries one COPY at a time, while each object has rows for
// several tables, so rows are collected and written out a table at a time.
class MapTableImport : public IDataStreamHandler
{
public:
	MapTableImport(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, int verbose):
		c(c), work(work), tablePrefix(tablePrefix), verbose(verbose), buffered(0), total(0)
	{
		for(int i=0; i<3; i++) lastId[i] = 0;
	}

	void StoreNode(const OsmNode &node) override
	{
		if(node.lat < -90.0 || node.lat > 90.0 || node.lon < -180.0 || node.lon > 180.0)
			cerr << "Warning: node " << node.objId << " has invalid lat/lon: " << node.lat << "," << node.lon << endl;
		ObjectRow row;
		Common(node, 0, row);
		stringstream ss;
		ss.precision(9);
		ss << "SRID=4326;POINT(" << fixed << node.lon << " " << node.lat << ")";
		row.extra = ss.str();
		Add(node, 0, row);
	}

	void StoreWay(const OsmWay &way) override
	{
		ObjectRow row;
		Common(way, 1, row);
		EncodeInt64Vec(way.refs, row.extra);
		// Only the live version of an object has rows in the member tables
		if(IsLive(way))
		{
			for(size_t i=0; i<way.refs.size(); i++)
				members[0].push_back({way.objId, (int64_t)way.metaData.version, (int64_t)i, way.refs[i]});
			buffered += way.refs.size();
		}
		Add(way, 1, row);
	}

	void StoreRelation(const OsmRelation &relation) override
	{
		ObjectRow row;
		Common(relation, 2, row);
		std::vector<std::string> refTypeStrs, refRoles;
		std::vector<int64_t> refIds;
		for(size_t i=0; i<relation.members.size(); i++)
		{
			const RelationMember &member = relation.members[i];
			refTypeStrs.push_back(ObjectTypeName(member.type));
			refIds.push_back(member.ref);
			refRoles.push_back(member.role);
			if(IsLive(relation))
				members[1 + (int)member.type].push_back({relation.objId, (int64_t)relation.metaData.version, (int64_t)i, member.ref});
		}
		buffered += relation.members.size();
		EncodeRelationMems(refTypeStrs, refIds, row.extra);
		EncodeStringVec(refRoles, row.roles);
		Add(relation, 2, row);
	}

	void Finish() override
	{
		Flush();
	}

	uint64_t Total() const {return total;}

private:
	struct ObjectRow
	{
		int64_t objId, version;
		std::optional<int64_t> changeset, uid, timestamp;
		std::optional<string> username;
		bool visible;
		string tags, extra, roles; // extra: geometry for a node, members otherwise
	};
	struct MemberRow
	{
		int64_t objId, version, index, member;
	};

	pqxx::connection &c;
	pqxx::transaction_base *work;
	string tablePrefix;
	int verbose;
	size_t buffered;
	uint64_t total;
	int64_t lastId[3];
	// By type (node, way, relation): rows for the live and history tables, and IDs
	std::vector<ObjectRow> live[3], old[3];
	std::vector<int64_t> ids[3];
	// way_mems, then relation_mems_n, _w and _r
	std::vector<MemberRow> members[4];

	// The current, visible version of an object is live; the rest is history
	static bool IsLive(const OsmObject &obj)
	{
		return obj.metaData.current && obj.metaData.visible;
	}

	// Something left out of a file is stored as NULL
	void Common(const OsmObject &obj, int type, ObjectRow &row)
	{
		const MetaData &m = obj.metaData;
		row.objId = obj.objId;
		row.version = m.version;
		if(m.changeset != 0) row.changeset = m.changeset;
		if(m.uid != 0) row.uid = m.uid;
		if(m.timestamp != 0) row.timestamp = m.timestamp;
		if(!m.username.empty()) row.username = m.username;
		row.visible = m.visible;
		EncodeTags(obj.tags, row.tags);
	}

	void Add(const OsmObject &obj, int type, const ObjectRow &row)
	{
		if(IsLive(obj))
			live[type].push_back(row);
		else
			old[type].push_back(row);
		// One entry for each object, however many of its versions the file holds
		if(obj.objId != lastId[type] || ids[type].empty())
		{
			ids[type].push_back(obj.objId);
			lastId[type] = obj.objId;
		}
		buffered += 2;
		total ++;
		if(verbose >= 1 && total % 1000000 == 0)
			cout << total << " objects" << endl;
		if(buffered >= 100000)
			Flush();
	}

	void Flush()
	{
		static const char *typeNames[3] = {"node", "way", "relation"};
		static const char *memberTables[4] = {"way_mems", "relation_mems_n", "relation_mems_w", "relation_mems_r"};
		for(int type=0; type<3; type++)
		{
			string extraColumns = type == 0 ? ",geom" : (type == 1 ? ",members" : ",members,memberroles");
			for(int history=0; history<2; history++)
			{
				std::vector<ObjectRow> &rows = history ? old[type] : live[type];
				if(rows.empty()) continue;
				string table = c.quote_name(tablePrefix + (history ? "old" : "live") + typeNames[type] + "s");
				string columns = string("id,changeset,username,uid,timestamp,version,tags") +
					(history ? ",visible" : "") + extraColumns;
				pqxx::stream_to stream = pqxx::stream_to::raw_table(*work, table, columns);
				for(const ObjectRow &row : rows)
				{
					// The columns that differ between the tables come last
					if(type == 2 && history)
						stream.write_values(row.objId, row.changeset, row.username, row.uid, row.timestamp,
							row.version, row.tags, row.visible, row.extra, row.roles);
					else if(type == 2)
						stream.write_values(row.objId, row.changeset, row.username, row.uid, row.timestamp,
							row.version, row.tags, row.extra, row.roles);
					else if(history)
						stream.write_values(row.objId, row.changeset, row.username, row.uid, row.timestamp,
							row.version, row.tags, row.visible, row.extra);
					else
						stream.write_values(row.objId, row.changeset, row.username, row.uid, row.timestamp,
							row.version, row.tags, row.extra);
				}
				stream.complete();
				rows.clear();
			}
			if(!ids[type].empty())
			{
				pqxx::stream_to stream = pqxx::stream_to::raw_table(*work,
					c.quote_name(tablePrefix + typeNames[type] + "ids"), "id");
				for(int64_t objId : ids[type])
					stream.write_values(objId);
				stream.complete();
				ids[type].clear();
			}
		}
		for(int table=0; table<4; table++)
		{
			if(members[table].empty()) continue;
			pqxx::stream_to stream = pqxx::stream_to::raw_table(*work,
				c.quote_name(tablePrefix + memberTables[table]), "id,version,index,member");
			for(const MemberRow &row : members[table])
				stream.write_values(row.objId, row.version, row.index, row.member);
			stream.complete();
			members[table].clear();
		}
		buffered = 0;
	}
};


bool DbImportMapFile(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &filename, const std::string &tablePrefix,
	std::string &errStr)
{
	try
	{
		// PostgreSQL does not have to write a table's rows to its write-ahead
		// log as well as to the table when the table was emptied in the
		// transaction that fills it (and wal_level is minimal): if the
		// transaction fails, the table simply goes back to being empty. So each
		// table that is empty is truncated here, which changes nothing else.
		// A table that already has rows is left alone and added to. This needs
		// the caller to import within one transaction to have any effect.
		for(const char *suffix : {"livenodes", "liveways", "liverelations", "oldnodes", "oldways",
			"oldrelations", "nodeids", "wayids", "relationids", "way_mems", "relation_mems_n",
			"relation_mems_w", "relation_mems_r"})
		{
			string table = c.quote_name(tablePrefix + suffix);
			if(work->exec("SELECT 1 FROM " + table + " LIMIT 1").empty())
				work->exec("TRUNCATE " + table);
		}

		if(verbose >= 1)
		{
			string walLevel = work->exec("SHOW wal_level")[0][0].as<string>();
			if(walLevel != "minimal")
				cout << "Note: the database has wal_level=" << walLevel << ", so everything imported is also "
					"written to its write-ahead log. For a large import, setting wal_level=minimal and "
					"max_wal_senders=0 in postgresql.conf and restarting PostgreSQL avoids that; set them "
					"back afterwards if the database is replicated or archived." << endl;
		}

		// A member table that starts empty is loaded without its key and its
		// index of members, which are put back once it is full: building them
		// in one go is much quicker than keeping them up row by row.
		std::vector<string> keysToRestore, indicesToRestore;
		for(const auto &entry : MEMBER_TABLES)
		{
			string suffix = entry[0];
			string table = c.quote_name(tablePrefix + suffix);
			if(!work->exec("SELECT 1 FROM " + table + " LIMIT 1").empty()) continue;
			auto exists = [&](const string &name) {
				return !work->exec("SELECT to_regclass(" + work->quote(c.quote_name(name)) + ")")[0][0].is_null();
			};
			if(exists(tablePrefix + suffix + "_key"))
			{
				work->exec("ALTER TABLE " + table + " DROP CONSTRAINT " + c.quote_name(tablePrefix + suffix + "_key"));
				keysToRestore.push_back(suffix);
			}
			if(exists(tablePrefix + suffix + "_mids"))
			{
				work->exec("DROP INDEX " + c.quote_name(tablePrefix + suffix + "_mids"));
				indicesToRestore.push_back(suffix);
			}
		}

		std::shared_ptr<MapTableImport> import(new MapTableImport(c, work, tablePrefix, verbose));
		LoadOsmFromFile(filename, import);
		// A decoder finishes its output itself; doing so again writes nothing
		import->Finish();
		if(verbose >= 1)
			cout << import->Total() << " objects imported" << endl;

		for(const string &suffix : keysToRestore)
			work->exec("ALTER TABLE " + c.quote_name(tablePrefix + suffix) + " ADD CONSTRAINT " +
				c.quote_name(tablePrefix + suffix + "_key") + " PRIMARY KEY (id, version, index)");
		for(const string &suffix : indicesToRestore)
			work->exec("CREATE INDEX " + c.quote_name(tablePrefix + suffix + "_mids") + " ON " +
				c.quote_name(tablePrefix + suffix) + " (member)");
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}
	return true;
}

// ************* The indices of the map tables, and their version *************
//
// The indices of the main map tables are not part of the schema version.
// Tables are created before a bulk import and indexed after it, and building
// the indices of a large map takes long enough that it has to be something
// that can be stopped and run again. So "Create indicies" is a step of its
// own, which adds whatever is missing.
//
// Adding what is missing cannot take away an index that is no longer wanted,
// or rebuild one whose definition has changed while its name has not. Those
// changes are made once each, by index version: a table set records, as
// index_version in its meta table, the version its indices were last brought
// up to, and DbCreateIndices makes the changes of every later version before
// adding what is missing.
//
// Index versions are numbered as schema versions are: index version 15 is
// the set of indices for schema version 15. The two are recorded separately,
// as tables are upgraded or created by one step and indexed by another, often
// days later. MAP_INDEX_VERSION follows MAP_SCHEMA_VERSION (dbadmin.h). To
// change the indices along with a new schema version:
//   1. raise MAP_SCHEMA_VERSION, which raises MAP_INDEX_VERSION,
//   2. for an index to be dropped or rebuilt, add the DROP to
//      DbChangeIndicesFrom, under the version being left behind,
//   3. for an index to be added or rebuilt, add or alter its CREATE in
//      DbCreateCurrentIndices.
// The indices that a schema step creates for the tables it adds (edit
// activity, stored extracts, the key of the member tables) stay with that
// schema step and are not counted here.

int DbGetIndexVersion(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix)
{
	string errStr;
	try
	{
		int version = std::stoi(DbGetMetaValue(c, work, "index_version", tablePrefix, errStr));
		// Index versions were first counted from 1, for the indices of schema
		// version 15, before they were numbered with the schema
		if(version > 0 && version < 11)
			return 15;
		return version;
	}
	catch (const std::exception &) {}
	return 0; // Never recorded: nothing is known about the indices
}

// What to tell someone about a table set whose indices are behind, or
// nothing if they are up to date
std::string DbIndexVersionNote(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix)
{
	int version = DbGetIndexVersion(c, work, tablePrefix);
	if(version >= MAP_INDEX_VERSION) return "";
	stringstream ss;
	ss << "The indices of the " << tablePrefix << " tables ";
	if(version == 0)
		ss << "are not recorded as built";
	else
		ss << "are those of schema version " << version;
	ss << " (the current version is " << MAP_INDEX_VERSION << "): run \"Create indicies\".";
	return ss.str();
}

// The changes that adding missing indices cannot make, for a table set whose
// indices are at fromVersion. Zero is a table set of unknown state.
static bool DbChangeIndicesFrom(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, int fromVersion, std::string &errStr)
{
	// Version 15 is the set of indices as it stood when versions began: there
	// is nothing to undo on the way to it. Later versions go here, as
	//   if(fromVersion < 16) { DROP INDEX IF EXISTS ... }
	return true;
}

static bool DbCreateCurrentIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	bool ok = true;
	string sql;
	string ine = "IF NOT EXISTS ";

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

// The indices and keys DbCreateCurrentIndices builds, for taking them away
// again. Keys are found by table, as their names depend on how they were made.
static const char *INDEXED_KEY_TABLES[] = {"oldnodes", "oldways", "oldrelations", "livenodes", "liveways",
	"liverelations", "nodeids", "wayids", "relationids", "usernames"};
static const char *INDEX_SUFFIXES[] = {"livenodes_gix", "oldnodes_gix",
	"way_mems_mids", "relation_mems_n_mids", "relation_mems_w_mids", "relation_mems_r_mids",
	"oldnodes_ts2", "oldways_ts2", "oldrelations_ts2", "livenodes_ts", "liveways_ts", "liverelations_ts",
	"oldnodes_uid2", "oldways_uid2", "oldrelations_uid2", "livenodes_uid", "liveways_uid", "liverelations_uid",
	"oldnodes_cs2", "oldways_cs2", "oldrelations_cs2", "livenodes_cs", "liveways_cs", "liverelations_cs",
	"changesets_uidx", "changesets_open_timestampx", "changesets_close_timestampx", "changesets_is_openx",
	"changesets_gix"};

// Index version 0: none of the indices or keys "Create indicies" builds, as
// a table set is after its tables are created. Dropping them before loading a
// large amount of data, and building them again afterwards, is much quicker
// than keeping them up as the rows go in.
static bool DbDropCurrentIndices(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, std::string &errStr)
{
	for(const char *suffix : INDEX_SUFFIXES)
	{
		string sql = "DROP INDEX IF EXISTS " + c.quote_name(tablePrefix + suffix) + ";";
		if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
	}
	for(const char *suffix : INDEXED_KEY_TABLES)
	{
		string table = tablePrefix + suffix;
		pqxx::result r = work->exec("SELECT conname FROM pg_constraint WHERE contype = 'p' AND conrelid = to_regclass(" +
			work->quote(c.quote_name(table)) + ")");
		for(const auto &row : r)
		{
			string sql = "ALTER TABLE " + c.quote_name(table) + " DROP CONSTRAINT " + c.quote_name(row[0].as<string>()) + ";";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		}
	}
	return true;
}

bool DbSetIndexVersion(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const string &tablePrefix, int targetVersion, std::string &errStr)
{
	if(targetVersion != 0 && (targetVersion < 11 || targetVersion > MAP_INDEX_VERSION))
	{
		stringstream ss;
		ss << "Index version " << targetVersion << " is not known: give 0 for none, or a version from 11 to "
			<< MAP_INDEX_VERSION << ".";
		errStr = ss.str();
		return false;
	}
	int version = DbGetIndexVersion(c, work, tablePrefix);
	if(verbose >= 1)
		cout << "Indices of " << tablePrefix << ": version " << version << ", bringing to " << targetVersion << endl;

	if(targetVersion == 0)
	{
		if(!DbDropCurrentIndices(c, work, verbose, tablePrefix, errStr)) return false;
	}
	else
	{
		// Versions 11 to 15 have one set of indices: the set as it stood when
		// index versions began had not changed since before schema versions
		// did. When a later version differs, the changes up to it go in
		// DbChangeIndicesFrom, and those back down from it go here, before the
		// set of the version asked for is built.
		if(version < targetVersion)
			if(!DbChangeIndicesFrom(c, work, verbose, tablePrefix, version, errStr)) return false;
		// Every index is checked each time, so a run that was stopped, or an
		// index that has gone missing, is put right by running this again
		if(!DbCreateCurrentIndices(c, work, verbose, tablePrefix, errStr)) return false;
	}
	// Recorded only once all of it has succeeded, in the current numbering
	return DbSetMetaValue(c, work, "index_version", to_string(targetVersion), tablePrefix, errStr);
}

bool DbCreateIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	return DbSetIndexVersion(c, work, verbose, tablePrefix, MAP_INDEX_VERSION, errStr);
}

// ************* The optional indices for finding objects by box and by tag *************
//
// Ways and relations get a spatial index on their stored box, which a map
// that uses stored boxes (useBboxInQuery) needs to find them by area, and
// every kind of object gets a text index on its tags, which the tag searches
// of the Overpass and XAPI queries use. A search by tag within an area uses
// the two together.
//
// Until now this was one index for each kind of object, on the position or
// box and the tags together. That is slow to build, as it can only be built
// a row at a time, and large. A separate index of each kind is several times
// quicker to build and smaller, and serves the same searches at least as
// well, a search by tag alone much better. Measured on a copy of a small map
// made eight times larger (542,000 nodes): 3.6 s and 37 MB for the nodes
// before, 0.7 s and 2 MB now.
//
// A table that still has an index of the earlier kind is left as it is: that
// index goes on serving, and replacing it is a matter of dropping the
// indices and creating them again.

static const char *TAG_TEXT_EXPRESSION = "jsonb_to_tsvector('english', tags, '\"all\"')";

bool DbCreateBboxIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	for(const char *kind : {"node", "way", "relation"})
	{
		string tableName = tablePrefix + "live" + kind + "s";
		string table = c.quote_name(tableName);
		if(DbCheckIndexExists(c, work, tableName + "_gix_tags"))
		{
			if(verbose >= 1)
				cout << tableName << " has an index of the earlier, combined kind, which is left in place. "
					"To have the newer indices instead, drop the bbox indices and create them again." << endl;
			continue;
		}

		bool created = false;
		// Nodes have their spatial index from "Create indicies"
		if(string(kind) != "node" && !DbCheckIndexExists(c, work, tableName + "_gix_bbox"))
		{
			string sql = "CREATE INDEX IF NOT EXISTS " + c.quote_name(tableName + "_gix_bbox") + " ON " + table +
				" USING GIST (bbox);";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
			created = true;
		}
		if(!DbCheckIndexExists(c, work, tableName + "_gin_tags"))
		{
			string sql = "CREATE INDEX IF NOT EXISTS " + c.quote_name(tableName + "_gin_tags") + " ON " + table +
				" USING GIN (" + TAG_TEXT_EXPRESSION + ");";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
			created = true;
		}
		if(created)
		{
			// So that the planner knows what the new indices hold, the text
			// index in particular: without this it cannot tell a common tag
			// from a rare one
			string sql = "ANALYZE " + table + ";";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		}
	}
	return true;
}

bool DbDropBboxIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const string &tablePrefix, 
	std::string &errStr)
{
	// Both kinds: the separate indices, and the combined one of earlier versions
	for(const char *kind : {"node", "way", "relation"})
		for(const char *suffix : {"_gin_tags", "_gix_bbox", "_gix_tags"})
		{
			string sql = "DROP INDEX IF EXISTS " + c.quote_name(tablePrefix + "live" + kind + "s" + suffix) + ";";
			if(!DbExec(work, sql, errStr, nullptr, verbose)) return false;
		}
	return true;
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
		if (boost::filesystem::path(diffPath).extension() == ".gz")
		{
			cout << "   " << diffPath << endl;
			std::string xmlData;
			DecodeGzipQuickFromFilename(diffPath, xmlData);
			
			shared_ptr<class OsmChange> data(new class OsmChange());
			std::stringbuf sb(xmlData);
			LoadFromOsmChangeXml(sb, *data);

			// The edits are recorded as an upload's are, so that whatever follows
			// the edit activity, such as a stored extract, sees them. One diff
			// file is one atomic edit.
			class EditActivityRecorder recorder(c, work, tableModPrefix, pgCommon);

			for(size_t i=0; i<data->blocks.size(); i++)
			{
				const std::string &action = data->blocks[i].action;
				cout << action << endl;
				class OsmData &block = data->blocks[i].data;

				//Set visibility flag depending on action
				bool isCreate = action == "create";
				bool isDelete = action == "delete";
				for(size_t j=0; j<block.nodes.size(); j++)
					block.nodes[j].metaData.visible = !isDelete;
				for(size_t j=0; j<block.ways.size(); j++)
					block.ways[j].metaData.visible = !isDelete;
				for(size_t j=0; j<block.relations.size(); j++)
					block.relations[j].metaData.visible = !isDelete;

				//Note how things are before the block changes them
				recorder.BeforeStore(action, block);

				//Store objects
				std::map<int64_t, int64_t> createdNodeIds, createdWayIds, createdRelationIds;

				// New versions, made by the diff's atomic edit; the copies of
				// affected parents below are not
				bool ok = ::StoreObjects(c, work, tableModPrefix, block, 
					createdNodeIds, createdWayIds, createdRelationIds, errStr, recorder.AtomicEditId());
				if(!ok)
					cout << "Warning: " << errStr << endl;
				bool stored = ok;

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
					//Get affected parent objects, as they were found before the
					//block was stored
					std::shared_ptr<class OsmData> affectedParents = make_shared<class OsmData>();
					if(recorder.Enabled())
						*affectedParents = recorder.AffectedParents();
					else
						pgCommon->GetAffectedParents2(block, affectedParents);

					//Ensure a copy of affected parents is in the active table
					std::map<int64_t, int64_t> unusedNodeIds, unusedWayIds, unusedRelationIds;
					::StoreObjects(c, work, tableModPrefix, *affectedParents.get(),
						unusedNodeIds, unusedWayIds, unusedRelationIds, errStr);

					for(size_t j=0; j<affectedParents->ways.size(); j++)
						waysToUpdate.insert(affectedParents->ways[j].objId);
					for(size_t j=0; j<affectedParents->relations.size(); j++)
						relsToUpdate.insert(affectedParents->relations[j].objId);
				}

				//Update bboxes of modified and parent ways
				::UpdateWayBboxesById(c, work,
					waysToUpdate,
					0,
					tableModPrefix,
					errStr);

				//Update relation bboxes
				::UpdateRelationBboxesById(c, work,
					relsToUpdate,
					0,
					tableModPrefix,
					errStr);

				//Record the edit, now that the boxes say where things have got to
				if(stored && !recorder.AfterStore(action, block, errStr))
					return false;
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

// ************* Working out the box of every way and relation *************
//
// "Update way/relation bboxes" gives every way and relation of a table set
// its stored box. On a whole planet the first version of this took over
// eight days for the ways alone: it was one UPDATE that looked up the nodes
// of each way separately, united them as geometry, and did all of it in a
// single transaction. Now:
//   - ways are done a batch of IDs at a time, each batch one statement that
//     joins the ways to their nodes and takes the extent, and, when the
//     caller is not inside a transaction, each batch is committed as it
//     finishes, so progress is kept and shown;
//   - with onlyMissing, ways that already have a box are passed over, which
//     is how a run that was stopped is taken up again;
//   - relations are worked out in the database a level at a time, where
//     before each relation cost several queries from here.
//
// Everything is known when this runs, so a member with no extent (a way none
// of whose nodes exist, a relation with nothing in it) is simply left out of
// what holds it. That differs from keeping a box up to date as an edit
// arrives (UpdateSingleRelation), where a member relation without a stored
// box may only not have been worked out yet.

// The most ways whose boxes are worked out by one statement
static const int64_t BBOX_WAYS_PER_BATCH = 50000;

int DbUpdateWayBboxes(pqxx::connection &c, pqxx::transaction_base *work,
    int verbose,
	const std::string &tablePrefix, 
	class PgCommon *adminObj,
	std::string &errStr,
	bool onlyMissing)
{
	string ways = c.quote_name(tablePrefix + "liveways");
	string nodes = c.quote_name(tablePrefix + "visiblenodes");

	// The extent of each way's nodes, for the ways the condition selects. A
	// way with none of its nodes to be found gets no box.
	auto updateSql = [&](const string &condition) {
		return "UPDATE " + ways + " w SET bbox = b.box FROM ("
			"SELECT w2.id, ST_SetSRID(ST_Extent(n.geom)::geometry, 4326) AS box FROM " + ways + " w2 "
			"LEFT JOIN LATERAL jsonb_array_elements_text(w2.members) AS e(ref) ON true "
			"LEFT JOIN " + nodes + " n ON n.id = e.ref::bigint "
			"WHERE " + condition + " GROUP BY w2.id) b WHERE w.id = b.id;";
	};
	string missing = onlyMissing ? " AND w2.bbox IS NULL" : "";

	// Batches are ranges of ID, which need the primary key to be found by.
	// Without it, as before "Create indicies" has been run, it is one statement.
	if(DbCountPrimaryKeyCols(c, work, tablePrefix + "liveways") == 0)
	{
		string sql = updateSql("true" + missing);
		if(verbose >= 1) cout << sql << endl;
		work->exec(sql);
		return 1;
	}

	int64_t total = work->exec("SELECT reltuples::bigint FROM pg_class WHERE oid = " +
		work->quote(ways) + "::regclass")[0][0].as<int64_t>();
	int64_t done = 0;
	bool started = false;
	int64_t last = 0;
	while(true)
	{
		// The ID that ends the next batch
		pqxx::result r = work->exec("SELECT max(id), count(*) FROM (SELECT id FROM " + ways +
			(started ? " WHERE id > " + to_string(last) : string()) + " ORDER BY id LIMIT " +
			to_string(BBOX_WAYS_PER_BATCH) + ") batch;");
		int64_t count = r[0][1].as<int64_t>();
		if(count == 0) break;
		int64_t upTo = r[0][0].as<int64_t>();
		string condition = (started ? "w2.id > " + to_string(last) + " AND " : string()) +
			"w2.id <= " + to_string(upTo) + missing;
		work->exec(updateSql(condition));
		started = true;
		last = upTo;
		done += count;
		if(verbose >= 1)
			cout << tablePrefix << "liveways: " << done << " of about " << std::max(total, done) << " ways" << endl;
		if(count < BBOX_WAYS_PER_BATCH) break;
	}
	return 1;
}

int DbUpdateRelationBboxes(pqxx::connection &conn, pqxx::transaction_base *work,
    int verbose,
	const std::string &tablePrefix, 
	class PgCommon *adminObj,
	std::string &errStr)
{
	auto table = [&](const string &suffix) { return conn.quote_name(tablePrefix + suffix); };
	string relations = table("liverelations");
	// Temporary tables: the box of each relation as it is worked out, and
	// which relations hold which
	const string boxes = "pgmap_relation_boxes", edges = "pgmap_relation_edges", outside = "pgmap_relation_outside";
	for(const string &name : {boxes, edges, outside})
		work->exec("DROP TABLE IF EXISTS " + name + ";");

	// A member belongs to the live version of its relation
	auto members = [&](const string &suffix) {
		return table(suffix) + " m JOIN " + relations + " r ON r.id = m.id AND r.version = m.version";
	};

	// First what each relation holds directly: its node members and the
	// stored boxes of its way members
	if(verbose >= 1) cout << tablePrefix << "liverelations: node and way members" << endl;
	work->exec("CREATE TEMP TABLE " + boxes + " AS SELECT id, min(x1) AS x1, min(y1) AS y1, max(x2) AS x2, max(y2) AS y2 FROM ("
		"SELECT m.id, ST_X(n.geom) AS x1, ST_Y(n.geom) AS y1, ST_X(n.geom) AS x2, ST_Y(n.geom) AS y2 FROM " +
		members("relation_mems_n") + " JOIN " + table("visiblenodes") + " n ON n.id = m.member "
		"UNION ALL "
		"SELECT m.id, ST_XMin(w.bbox), ST_YMin(w.bbox), ST_XMax(w.bbox), ST_YMax(w.bbox) FROM " +
		members("relation_mems_w") + " JOIN " + table("visibleways") + " w ON w.id = m.member WHERE w.bbox IS NOT NULL"
		") direct GROUP BY id;");
	work->exec("ALTER TABLE " + boxes + " ADD PRIMARY KEY (id);");

	// Then what it holds through its relation members, a level at a time,
	// until nothing grows. A member relation that belongs to another table
	// set, as when the active tables refer to the base ones, has its stored box.
	work->exec("CREATE TEMP TABLE " + edges + " AS SELECT DISTINCT m.id AS parent, m.member AS child FROM " +
		members("relation_mems_r") + " WHERE m.member <> m.id;");
	work->exec("CREATE TEMP TABLE " + outside + " AS SELECT v.id, ST_XMin(v.bbox) AS x1, ST_YMin(v.bbox) AS y1, "
		"ST_XMax(v.bbox) AS x2, ST_YMax(v.bbox) AS y2 FROM " + table("visiblerelations") + " v "
		"WHERE v.bbox IS NOT NULL AND v.id IN (SELECT child FROM " + edges + ") "
		"AND NOT EXISTS (SELECT 1 FROM " + relations + " own WHERE own.id = v.id);");
	work->exec("ANALYZE " + boxes + "; ANALYZE " + edges + ";");
	for(int level = 1; level <= 100; level++)
	{
		pqxx::result r = work->exec("INSERT INTO " + boxes + " AS t (id, x1, y1, x2, y2) "
			"SELECT e.parent, min(c.x1), min(c.y1), max(c.x2), max(c.y2) FROM " + edges + " e JOIN ("
			"SELECT id, x1, y1, x2, y2 FROM " + boxes + " UNION ALL SELECT id, x1, y1, x2, y2 FROM " + outside +
			") c ON c.id = e.child GROUP BY e.parent "
			"ON CONFLICT (id) DO UPDATE SET x1 = LEAST(t.x1, EXCLUDED.x1), y1 = LEAST(t.y1, EXCLUDED.y1), "
			"x2 = GREATEST(t.x2, EXCLUDED.x2), y2 = GREATEST(t.y2, EXCLUDED.y2) "
			"WHERE EXCLUDED.x1 < t.x1 OR EXCLUDED.y1 < t.y1 OR EXCLUDED.x2 > t.x2 OR EXCLUDED.y2 > t.y2;");
		if(verbose >= 1)
			cout << tablePrefix << "liverelations: level " << level << ", " << r.affected_rows() << " changed" << endl;
		if(r.affected_rows() == 0) break;
	}

	// Store them: a relation with no extent has no box
	work->exec("UPDATE " + relations + " r SET bbox = ST_MakeEnvelope(t.x1, t.y1, t.x2, t.y2, 4326) FROM " + boxes +
		" t WHERE r.id = t.id;");
	work->exec("UPDATE " + relations + " r SET bbox = NULL WHERE r.bbox IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " +
		boxes + " t WHERE t.id = r.id);");
	for(const string &name : {boxes, edges, outside})
		work->exec("DROP TABLE " + name + ";");
	return 1;
}
