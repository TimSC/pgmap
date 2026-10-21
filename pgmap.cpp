#include "pgmap.h"
#include <functional>
#include "dbquery.h"
#include "dbextract.h"
#include "dbprepared.h"
#include "dbids.h"
#include "dbadmin.h"
#include "dbdecode.h"
#include "dbreplicate.h"
#include "dbstore.h"
#include "dbdump.h"
#include "dbfilters.h"
#include "dbchangeset.h"
#include "dbmeta.h"
#include "osmfile.h"
#include "dbcommon.h"
#include "dboverpass.h"
#include "util.h"
#include "cppo5m/model.h"
#include <algorithm>
#include <cmath>
using namespace std;

PgMapError::PgMapError()
{

}

PgMapError::PgMapError(const string &connection)
{
	this->errStr = connection;
}

PgMapError::PgMapError(const PgMapError &obj)
{
	this->errStr = obj.errStr;
}

PgMapError::~PgMapError()
{

}

// **********************************************

bool LockMap(std::shared_ptr<pqxx::transaction_base> work, const std::string &prefix, const std::string &accessMode, std::string &errStr)
{
	try
	{
		//It is important resources are locked in a consistent order to avoid deadlock
		//Also, lock everything in one command to get a consistent view of the data.
		string sql = "LOCK TABLE "+prefix+ "oldnodes";
		sql += ","+prefix+ "oldways";
		sql += ","+prefix+ "oldrelations";
		sql += ","+prefix+ "livenodes";
		sql += ","+prefix+ "liveways";
		sql += ","+prefix+ "liverelations";

		sql += ","+prefix+ "nodeids";
		sql += ","+prefix+ "wayids";
		sql += ","+prefix+ "relationids";

		sql += ","+prefix+ "way_mems";
		sql += ","+prefix+ "relation_mems_n";
		sql += ","+prefix+ "relation_mems_w";
		sql += ","+prefix+ "relation_mems_r";
		sql += ","+prefix+ "nextids";
		sql += ","+prefix+ "changesets";
		sql += ","+prefix+ "meta";
		sql += ","+prefix+ "usernames";
		sql += ","+prefix+ "query_activity";
		sql += ","+prefix+ "edit_activity";
		sql += " IN "+accessMode+" MODE;";

		work->exec(sql);

	}
	catch (const pqxx::sql_error &e)
	{
		stringstream ss;
		ss << e.what() << " (" << e.query() << ")";
		errStr = ss.str();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}
	return true;
}

// **********************************************

PgChangeset::PgChangeset()
{
	objId = 0;
	uid = 0;
	open_timestamp = 0;
	close_timestamp = 0;
	is_open = true; bbox_set = false;
	x1 = 0.0; y1 = 0.0; x2 = 0.0; y2 = 0.0;
	created_count = 0; modified_count = 0; deleted_count = 0;
}

PgChangeset::PgChangeset(const PgChangeset &obj)
{
	*this = obj;
}

PgChangeset::~PgChangeset()
{

}

PgChangeset& PgChangeset::operator=(const PgChangeset &obj)
{
	objId = obj.objId;
	uid = obj.uid;
	open_timestamp = obj.open_timestamp;
	close_timestamp = obj.close_timestamp;
	username = obj.username;
	tags = obj.tags;
	is_open = obj.is_open;
	bbox_set = obj.bbox_set;
	x1 = obj.x1; y1 = obj.y1; x2 = obj.x2; y2 = obj.y2;
	created_count = obj.created_count;
	modified_count = obj.modified_count;
	deleted_count = obj.deleted_count;
	return *this;
}

// **********************************************

PgMapQuery::PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn,
		shared_ptr<pqxx::connection> &db,
		std::shared_ptr<class PgWork> sharedWorkIn,
		class DbUsernameLookup &dbUsernameLookupIn):
	sharedWork(sharedWorkIn),
	dbUsernameLookup(dbUsernameLookupIn)
{
	mapQueryActive = false;
	dbconn = db;

	tableStaticPrefix = tableStaticPrefixIn;
	tableActivePrefix = tableActivePrefixIn;
	useBboxInQuery = 0;
}

PgMapQuery::~PgMapQuery()
{
	this->Reset();
}

PgMapQuery& PgMapQuery::operator=(const PgMapQuery&)
{
	return *this;
}

int PgMapQuery::StartCommon(const vector<double> &bbox, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc)
{
	/**
	\param area being queried
	\param timestamp the timestamp right now (0 if we don't care about logging accurate query times)
	\param enc output object to receive the query result
	*/
	this->mapQueryEnc = enc;

	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Check if bbox data has been enabled for ways and relations
	string errStrNative;
	string useBboxInQueryStr;
	try
	{
		useBboxInQueryStr = DbGetMetaValue(*dbconn, work.get(),
			"useBboxInQuery", 
			this->tableActivePrefix,
			errStrNative);
	}
	catch(runtime_error &err)
	{		
	}
	this->useBboxInQuery = atoi(useBboxInQueryStr.c_str()) == 1;

	string errStr;
	bool ok = DbInsertQueryActivity(*dbconn, work.get(), this->tableActivePrefix,
		timestamp,
		bbox,
		errStr,
		0);
	if (!ok)
		cout << errStr << endl;

	return 0;
}

void PgMapQuery::SetAtomicEditId(int64_t atomicEditIdIn)
{
	if(mapQueryActive)
		throw runtime_error("Query already active");
	if(atomicEditIdIn < 0)
		throw invalid_argument("Atomic edit ID must not be negative");
	this->atomicEditId = atomicEditIdIn;
}

int PgMapQuery::Start(const vector<double> &bbox, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc)
{
	if(mapQueryActive)
		throw runtime_error("Query already active");
	if(dbconn.get() == NULL)
		throw runtime_error("DB pointer not set for PgMapQuery");
	if(bbox.size() != 4)
		throw invalid_argument("bbox must have four values");

	mapQueryActive = true;
	this->mapQueryPhase = 0;
	this->mapQueryBbox = bbox;

	return this->StartCommon(bbox, timestamp, enc);
}

int PgMapQuery::Start(const std::string &wkt, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc)
{
	if(mapQueryActive)
		throw runtime_error("Query already active");
	if(dbconn.get() == NULL)
		throw runtime_error("DB pointer not set for PgMapQuery");
	mapQueryActive = true;
	this->mapQueryPhase = 0;
	this->mapQueryWkt = wkt;
	// The rectangle around it, for the bounds of what is written
	this->mapQueryWktBounds.clear();
	{
		std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
		if(!work) throw runtime_error("Transaction has been deleted");
		pqxx::result r = work->exec("SELECT ST_XMin(g), ST_YMin(g), ST_XMax(g), ST_YMax(g) FROM "
			"(SELECT ST_GeomFromText(" + work->quote(wkt) + ", 4326) AS g) area");
		if(!r[0][0].is_null())
			for(int i=0; i<4; i++) this->mapQueryWktBounds.push_back(r[0][i].as<double>());
	}

	std::vector<double> emptyBbox;
	return this->StartCommon(emptyBbox, timestamp, enc);
}

// The temporary tables one map query keeps its selection in. They belong to
// the session and go when its transaction ends.
static const char *MAP_QUERY_SEEDS = "pgmap_mapquery_seeds";
static const char *MAP_QUERY_WAYS = "pgmap_mapquery_ways";
static const char *MAP_QUERY_EXTRA = "pgmap_mapquery_extra";
static const char *MAP_QUERY_CHANGED = "pgmap_mapquery_changed";

static void DropMapQueryTables(pqxx::transaction_base *work)
{
	for(const char *name : {MAP_QUERY_SEEDS, MAP_QUERY_WAYS, MAP_QUERY_EXTRA, MAP_QUERY_CHANGED})
		work->exec(string("DROP TABLE IF EXISTS ") + name);
}

void PgMapQuery::SelectObjects()
{
	/*
	 * The selection is worked out in the database, which then only has to be
	 * read from. The rule is:
	 *   1. the nodes inside the area,
	 *   2. the ways having one of those nodes or, on a map that uses stored
	 *      boxes (useBboxInQuery), the ways whose box meets the area,
	 *   3. the other nodes of those ways,
	 *   4. the relations having any of those nodes or ways as a member or,
	 *      with stored boxes, the relations whose box meets the area.
	 * DbUpdateExtract follows the same rule for a stored extract.
	 *
	 * Each kind of object is read from the static tables and from the active
	 * ones, a static object being passed over when the active tables have
	 * taken it over (its ID is then in their *ids table). That is what the
	 * visible* views do, without their removal of duplicates, which has
	 * nothing to remove.
	 *
	 * The map as it was after an atomic edit (SetAtomicEditId) is the map as
	 * it is with the edits since undone. The edit activity records since say
	 * which objects those edits changed, and in their lists of existing
	 * objects, the version each had before: the version before the first
	 * edit since that changed an object is its version then, and an object
	 * with none was made since. Every other object is as it was then, and is
	 * found as now; the changed ones are found among their versions then, by
	 * ID and version, in the live and history tables of every layer. So the
	 * work grows with the edits since, not with the history of the map.
	 * Stored boxes are of current shapes, so ways and relations are found
	 * through their members, as without useBboxInQuery.
	 */
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	pqxx::connection &c = *dbconn;

	// Static baseline, then the active overlay
	vector<pair<string,string> > layers;
	if(!tableStaticPrefix.empty() && tableStaticPrefix != tableActivePrefix)
		layers.push_back({tableStaticPrefix, tableActivePrefix});
	layers.push_back({tableActivePrefix, ""});
	auto suppressed = [&](const string &exclude, const string &kind) {
		if(exclude.empty()) return string("true");
		return "NOT EXISTS (SELECT 1 FROM " + c.quote_name(exclude + kind + "ids") + " x WHERE x.id=o.id)";
	};
	// Rows of one kind of object from every layer, each layer's chosen by a
	// FROM clause naming its live table as o
	auto fromLayers = [&](const string &kind, const string &columns,
		std::function<string(const string &)> choose) {
		string sql;
		for(const auto &layer : layers)
		{
			if(!sql.empty()) sql += " UNION ALL ";
			sql += "SELECT " + columns + " FROM " + c.quote_name(layer.first + "live" + kind + "s") + " o " +
				choose(layer.first) + " AND " + suppressed(layer.second, kind);
		}
		return sql;
	};

	string area;
	if(this->mapQueryBbox.size() == 4)
	{
		stringstream ss;
		ss.precision(9);
		ss << fixed << "ST_MakeEnvelope(" << mapQueryBbox[0] << "," << mapQueryBbox[1] << ","
			<< mapQueryBbox[2] << "," << mapQueryBbox[3] << ", 4326)";
		area = ss.str();
	}
	else
		area = "ST_GeomFromText(" + c.quote(this->mapQueryWkt) + ", 4326)";
	// What is in a polygon, not just in the rectangle around it. For a
	// rectangle the two are the same, and && alone is quicker.
	bool polygon = this->mapQueryBbox.size() != 4;
	auto within = [&](const string &column) {
		return polygon ? "ST_Intersects(" + column + ", " + area + ")" : column + " && " + area;
	};

	DropMapQueryTables(work.get());

	const bool past = this->atomicEditId > 0;
	const bool byBoxes = this->useBboxInQuery && !past;
	// What is read of an object of each kind, which the live and history
	// tables both have
	auto objectColumns = [](const string &kind) {
		string columns = "o.id, o.changeset, o.username, o.uid, o.timestamp, o.version, o.tags";
		if(kind == "node") return columns + ", o.geom";
		if(kind == "way") return columns + ", o.members";
		return columns + ", o.members, o.memberroles";
	};
	auto readColumns = [&](const string &kind) {
		return kind == "node" ? objectColumns(kind) + ", ST_X(o.geom) AS lon, ST_Y(o.geom) AS lat" : objectColumns(kind);
	};
	if(past)
	{
		// The objects changed since, each with its version then, or NULL if made since
		string activity = c.quote_name(tableActivePrefix + "edit_activity");
		work->exec(string("CREATE TEMP TABLE ") + MAP_QUERY_CHANGED + " ON COMMIT DROP AS "
			"SELECT DISTINCT ON (u.value->>0, (u.value->>1)::bigint) u.value->>0 AS kind, (u.value->>1)::bigint AS id, "
			"(SELECT (x.value->>2)::bigint FROM jsonb_array_elements(a.existing) x "
			"WHERE x.value->>0 = u.value->>0 AND x.value->>1 = u.value->>1 LIMIT 1) AS version "
			"FROM " + activity + " a CROSS JOIN LATERAL jsonb_array_elements(a.updated) u "
			"WHERE a.atomic_edit_id > " + to_string(this->atomicEditId) + " "
			"ORDER BY u.value->>0, (u.value->>1)::bigint, a.atomic_edit_id, a.block_index");
	}
	// Rows of one kind of object as the map has them, or had them after the
	// atomic edit: those unchanged since, chosen from each layer by choose as
	// fromLayers does, and the versions then of those changed since, chosen
	// by changed, a condition on o. A version can be in more than one layer
	// (a static one the active tables copied), which is read once.
	auto rowsOf = [&](const string &kind, std::function<string(const string &)> choose, const string &changed) {
		// Columns named, not *, so that what is not sent, such as a shape kept
		// for map renderers, is not read
		if(!past)
			return fromLayers(kind, readColumns(kind), choose);
		string kindValue = c.quote(kind);
		string sql = fromLayers(kind, readColumns(kind), [&](const string &prefix) {
			return choose(prefix) + " AND NOT EXISTS (SELECT 1 FROM " + MAP_QUERY_CHANGED +
				" ch WHERE ch.kind=" + kindValue + " AND ch.id=o.id)"; });
		string versions;
		int rank = 0;
		for(auto layer = layers.rbegin(); layer != layers.rend(); ++layer)
			for(const char *table : {"live", "old"})
			{
				if(!versions.empty()) versions += " UNION ALL ";
				versions += "SELECT " + to_string(rank++) + " AS rank, " + objectColumns(kind) + " FROM " +
					c.quote_name(layer->first + table + kind + "s") + " o JOIN " + MAP_QUERY_CHANGED +
					" ch ON ch.kind=" + kindValue + " AND ch.id=o.id AND ch.version=o.version" +
					(string(table) == "old" ? " WHERE o.visible" : "");
			}
		return sql + " UNION ALL (SELECT DISTINCT ON (o.id) " + readColumns(kind) + " FROM (" + versions +
			") o WHERE " + changed + " ORDER BY o.id, o.rank)";
	};

	// The tables of nodes and ways hold whole rows, which are then written
	// out from them. Holding only IDs and reading the objects again by ID was
	// tried and measured: it was 10 to 15% slower on large areas, and more on
	// small ones, since each object is then looked up twice.

	// 1. Nodes in the area
	work->exec(string("CREATE TEMP TABLE ") + MAP_QUERY_SEEDS + " ON COMMIT DROP AS " +
		rowsOf("node", [&](const string &) { return "WHERE " + within("o.geom"); }, within("o.geom")));

	// 2. Ways. The member tables are of live versions, which are the versions
	// then of the ways unchanged since; a way's own list of members is read
	// for its version then.
	if(!byBoxes)
		work->exec(string("CREATE TEMP TABLE ") + MAP_QUERY_WAYS + " ON COMMIT DROP AS " +
			rowsOf("way", [&](const string &prefix) {
				return "JOIN (SELECT DISTINCT m.id, m.version FROM " + c.quote_name(prefix + "way_mems") +
					" m JOIN " + MAP_QUERY_SEEDS + " s ON m.member=s.id) w ON o.id=w.id AND o.version=w.version WHERE true"; },
				string("EXISTS (SELECT 1 FROM jsonb_array_elements(o.members) m JOIN ") + MAP_QUERY_SEEDS +
					" s ON s.id = m.value::text::bigint)"));
	else
		work->exec(string("CREATE TEMP TABLE ") + MAP_QUERY_WAYS + " ON COMMIT DROP AS " +
			fromLayers("way", readColumns("way"), [&](const string &) { return "WHERE " + within("o.bbox"); }));

	// 3. Nodes of those ways that are outside the area
	work->exec(string("CREATE TEMP TABLE ") + MAP_QUERY_EXTRA + " ON COMMIT DROP AS "
		"SELECT DISTINCT member.value::text::bigint AS id FROM " + MAP_QUERY_WAYS +
		" w CROSS JOIN LATERAL jsonb_array_elements(w.members) member"
		" EXCEPT SELECT id FROM " + MAP_QUERY_SEEDS);
	this->extraNodesSql = rowsOf("node", [&](const string &) {
		return string("JOIN ") + MAP_QUERY_EXTRA + " s ON o.id=s.id WHERE true"; },
		string("o.id IN (SELECT id FROM ") + MAP_QUERY_EXTRA + ")");

	// 4. Relations
	if(!byBoxes)
		this->relationsSql = rowsOf("relation", [&](const string &prefix) {
			string holders;
			for(const char *selected : {MAP_QUERY_SEEDS, MAP_QUERY_EXTRA})
				holders += "SELECT m.id, m.version FROM " + c.quote_name(prefix + "relation_mems_n") +
					" m JOIN " + selected + " s ON m.member=s.id UNION ";
			holders += "SELECT m.id, m.version FROM " + c.quote_name(prefix + "relation_mems_w") +
				" m JOIN " + MAP_QUERY_WAYS + " s ON m.member=s.id";
			return "JOIN (" + holders + ") r ON o.id=r.id AND o.version=r.version WHERE true"; },
			string("EXISTS (SELECT 1 FROM jsonb_array_elements(o.members) m WHERE "
				"(m.value->>0 = 'node' AND (m.value->>1)::bigint IN (SELECT id FROM ") + MAP_QUERY_SEEDS +
				" UNION ALL SELECT id FROM " + MAP_QUERY_EXTRA + ")) OR "
				"(m.value->>0 = 'way' AND (m.value->>1)::bigint IN (SELECT id FROM " + MAP_QUERY_WAYS + ")))");
	else
		// A relation's box may be two, either side of the antimeridian, which
		// the index sees as one round the world: what && finds is checked
		this->relationsSql = fromLayers("relation", readColumns("relation"), [&](const string &) {
			return "WHERE " + within("o.bbox") + (polygon ? "" : " AND ST_Intersects(o.bbox, " + area + ")"); });
}

int PgMapQuery::Continue()
{
	if(!mapQueryActive)
		throw runtime_error("Query not active");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	// Each call does one step, so that the caller can pass on what has been
	// written so far: the header, the selection, then a batch of objects at a time.
	if(this->mapQueryPhase == 0)
	{
		this->mapQueryEnc->StoreIsDiff(false);
		if(this->mapQueryBbox.size() == 4)
			this->mapQueryEnc->StoreBounds(Bounds(this->mapQueryBbox[0], this->mapQueryBbox[1], this->mapQueryBbox[2], this->mapQueryBbox[3]));
		else if(this->mapQueryWktBounds.size() == 4)
			this->mapQueryEnc->StoreBounds(Bounds(this->mapQueryWktBounds[0], this->mapQueryWktBounds[1],
				this->mapQueryWktBounds[2], this->mapQueryWktBounds[3]));
		this->mapQueryPhase = 1;
		return 0;
	}

	if(this->mapQueryPhase == 1)
	{
		this->SelectObjects();
		cursor.reset(new pqxx::icursorstream(*work, string("SELECT * FROM ") + MAP_QUERY_SEEDS, "mapquerynodes", 1000));
		this->mapQueryPhase = 2;
		return 0;
	}

	if(this->mapQueryPhase == 2)
	{
		//Nodes in the area
		int ret = NodeResultsToEncoder(*cursor, this->dbUsernameLookup, this->mapQueryEnc);
		if(ret > 0) return 0;
		if(ret < 0) return -1;
		cursor.reset(new pqxx::icursorstream(*work, this->extraNodesSql, "mapqueryextranodes", 1000));
		this->mapQueryPhase = 3;
		return 0;
	}

	if(this->mapQueryPhase == 3)
	{
		//Nodes that complete the ways
		int ret = NodeResultsToEncoder(*cursor, this->dbUsernameLookup, this->mapQueryEnc);
		if(ret > 0) return 0;
		if(ret < 0) return -1;
		this->mapQueryEnc->Reset();
		cursor.reset(new pqxx::icursorstream(*work, string("SELECT * FROM ") + MAP_QUERY_WAYS, "mapqueryways", 1000));
		this->mapQueryPhase = 4;
		return 0;
	}

	if(this->mapQueryPhase == 4)
	{
		int ret = WayResultsToEncoder(*cursor, this->dbUsernameLookup, this->mapQueryEnc);
		if(ret > 0) return 0;
		if(ret < 0) return -1;
		this->mapQueryEnc->Reset();
		cursor.reset(new pqxx::icursorstream(*work, this->relationsSql, "mapqueryrelations", 1000));
		this->mapQueryPhase = 5;
		return 0;
	}

	if(this->mapQueryPhase == 5)
	{
		std::set<int64_t> skipIds;
		RelationResultsToEncoder(*cursor, this->dbUsernameLookup, skipIds, this->mapQueryEnc);
		cursor.reset();
		DropMapQueryTables(work.get());
		this->mapQueryPhase = 6;
		return 0;
	}

	if(this->mapQueryPhase == 6)
	{
		this->mapQueryEnc->Finish();

		this->Reset();
		return 1; // All done!
	}

	return -1;
}

void PgMapQuery::Reset()
{
	this->mapQueryPhase = 0;
	this->mapQueryActive = false;
	this->mapQueryEnc.reset();
	this->mapQueryBbox.clear();
	this->mapQueryWkt.clear();
	this->mapQueryWktBounds.clear();
	this->cursor.reset();
	this->extraNodesSql.clear();
	this->relationsSql.clear();
	this->atomicEditId = 0;
}

// *********************************************


PgTransaction::PgTransaction(shared_ptr<pqxx::connection> dbconnIn,
	const string &tableStaticPrefixIn, 
	const string &tableActivePrefixIn,
	std::shared_ptr<class PgWork> sharedWorkIn,
	const std::string &shareMode):

	PgCommon(dbconnIn, tableStaticPrefixIn, tableActivePrefixIn, sharedWorkIn, shareMode)
{
	string errStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = LockMap(work, this->tableStaticPrefix, this->shareMode, errStr);
	if(!ok)
		throw runtime_error(errStr);
	ok = LockMap(work, this->tableActivePrefix, this->shareMode, errStr);
	if(!ok)
		throw runtime_error(errStr);
}

PgTransaction::~PgTransaction()
{
	this->sharedWork->work.reset();
	this->sharedWork.reset();
}

shared_ptr<class PgMapQuery> PgTransaction::GetQueryMgr()
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	shared_ptr<class PgMapQuery> out(new class PgMapQuery(tableStaticPrefix, tableActivePrefix, 
		this->dbconn, this->sharedWork, this->dbUsernameLookup));
	return out;
}

void PgTransaction::GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, 
	std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(objectIds.size()==0)
		return;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	std::set<int64_t>::const_iterator it = objectIds.begin();
	while(it != objectIds.end())
		GetVisibleObjectsById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, objectIds, 
			it, 1000, out);

}

void PgTransaction::GetFullObjectById(const std::string &type, int64_t objectId, 
	std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(type == "node")
		throw invalid_argument("Cannot get full object for nodes");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Get main object
	std::shared_ptr<class OsmData> outData(new class OsmData());
	std::set<int64_t> objectIds;
	objectIds.insert(objectId);
	this->GetObjectsById(type, objectIds, outData);

	//Get members of main object
	if(type == "way")
	{
		if (outData->ways.size() != 1)
			return; //Unexpected number of objects in intermediate result
		class OsmWay &mainWay = outData->ways[0];

		std::set<int64_t> memberNodes;
		for(size_t i=0; i<mainWay.refs.size(); i++)
			memberNodes.insert(mainWay.refs[i]);
		this->GetObjectsById("node", memberNodes, outData);
 	}
	else if(type == "relation")
	{
		if (outData->relations.size() != 1)
			return; //Unexpected number of objects in intermediate result
		class OsmRelation &mainRelation = outData->relations[0];

		std::set<int64_t> memberNodes, memberWays, memberRelations;
		for(const RelationMember &member : mainRelation.members)
		{
			if(member.type == ObjectType::Node)
				memberNodes.insert(member.ref);
			else if(member.type == ObjectType::Way)
				memberWays.insert(member.ref);
			else if(member.type == ObjectType::Relation)
				memberRelations.insert(member.ref);
		}

		std::shared_ptr<class OsmData> memberWayObjs(new class OsmData());
		this->GetObjectsById("node", memberNodes, outData);
		this->GetObjectsById("way", memberWays, memberWayObjs);
		this->GetObjectsById("relation", memberRelations, outData);
		memberWayObjs->StreamTo(*outData.get());

		std::set<int64_t> memberNodes2;
		for(size_t i=0; i<memberWayObjs->ways.size(); i++)
			for(size_t j=0; j<memberWayObjs->ways[i].refs.size(); j++)
				memberNodes2.insert(memberWayObjs->ways[i].refs[j]);
		this->GetObjectsById("node", memberNodes2, outData);
	}
	else
		throw invalid_argument("Known object type");
	
	outData->StreamTo(*out.get());
}

void PgTransaction::GetObjectsByIdVer(const std::string &type, const std::set<std::pair<int64_t, int64_t> > &objectIdVers, 
		std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(objectIdVers.size()==0)
		return;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Query all tables that might contain object of this id and version
	std::set<std::pair<int64_t, int64_t> >::const_iterator it = objectIdVers.begin();
	while(it != objectIdVers.end())
		DbGetObjectsByIdVer(*dbconn, work.get(), this->dbUsernameLookup, 
			this->tableStaticPrefix, type, "old", objectIdVers, 
			it, 1000, out);
	it = objectIdVers.begin();
	while(it != objectIdVers.end())
		DbGetObjectsByIdVer(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableStaticPrefix, type, "live", objectIdVers, 
			it, 1000, out);
	it = objectIdVers.begin();
	while(it != objectIdVers.end())
		DbGetObjectsByIdVer(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, "old", objectIdVers, 
			it, 1000, out);
	it = objectIdVers.begin();
	while(it != objectIdVers.end())
		DbGetObjectsByIdVer(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, "live", objectIdVers, 
			it, 1000, out);
}

void PgTransaction::GetObjectsHistoryById(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	if(objectIds.size()==0)
		return;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Query all tables that might contain object of this id and version
	std::set<int64_t>::const_iterator it = objectIds.begin();
	while(it != objectIds.end())
		DbGetObjectsHistoryById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableStaticPrefix, type, "old", objectIds, 
			it, 1000, out);
	it = objectIds.begin();
	while(it != objectIds.end())
		DbGetObjectsHistoryById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableStaticPrefix, type, "live", objectIds, 
			it, 1000, out);
	it = objectIds.begin();
	while(it != objectIds.end())
		DbGetObjectsHistoryById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, "old", objectIds, 
			it, 1000, out);
	it = objectIds.begin();
	while(it != objectIds.end())
		DbGetObjectsHistoryById(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, type, "live", objectIds, 
			it, 1000, out);
}

bool PgTransaction::StoreObjects(class OsmData &data, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	bool saveToStaticTables,
	class PgMapError &errStr,
	bool asEdit)
{
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	string tablePrefix = this->tableActivePrefix;
	if(saveToStaticTables)
		tablePrefix = this->tableStaticPrefix;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	// The static tables hold the imported map, which no atomic edit made
	int64_t atomicEditId = asEdit && !saveToStaticTables ? this->Recorder(work.get()).AtomicEditId() : 0;
	bool ok = ::StoreObjects(*dbconn, work.get(), tablePrefix, data, createdNodeIds, createdWayIds, createdRelationIds,
		nativeErrStr, atomicEditId);
	errStr.errStr = nativeErrStr;

	return ok;
}

int PgTransaction::UpdateObjectBboxesById(
	const std::string &objType,
	const std::set<int64_t> &objectIds, int verbose, 
	bool saveToStaticTables,
	class PgMapError &errStr)
{
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	string tablePrefix = this->tableActivePrefix;
	if(saveToStaticTables)
		tablePrefix = this->tableStaticPrefix;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	int ok = 1;
	if(objType == "way")
		ok = ::UpdateWayBboxesById(*dbconn, work.get(),
			objectIds,
			verbose,
			tablePrefix, 
			nativeErrStr);
	else if (objType == "relation")
	{
		ok = ::UpdateRelationBboxesById(*dbconn, work.get(),
			objectIds,
			verbose,
			tablePrefix, 
			nativeErrStr);
	}

	errStr.errStr = nativeErrStr;

	return ok;
}

// A streaming database sink: object contents remain in PostgreSQL, not OsmData.
// The query and destination writes use the same repeatable-read transaction.
class ExtractDatabaseWriter : public IDataStreamHandler
{
	pqxx::connection &connection;
	pqxx::transaction_base &work;
	string prefix;
	int64_t extractId;

	void CopyObject(const string &kind, int64_t id)
	{
		string columns = "id, changeset, changeset_index, username, uid, timestamp, version, tags";
		if(kind == "node") columns += ", geom";
		else if(kind == "way") columns += ", members, bbox";
		else columns += ", members, memberroles, bbox";
		string destination = connection.quote_name(prefix + "extract_live" + kind + "s");
		string source = connection.quote_name(prefix + "visible" + kind + "s");
		work.exec("INSERT INTO " + destination + " (extract_id, " + columns + ") SELECT " +
			to_string(extractId) + ", " + columns + " FROM " + source + " WHERE id=" +
			to_string(id) + " ON CONFLICT (extract_id, id) DO NOTHING");
	}

	// types, when given, selects the members whose type starts with the
	// letter ending the table suffix.
	void Membership(const string &suffix, int64_t id, int64_t version,
		const vector<int64_t> &refs, const vector<ObjectType> *types = nullptr)
	{
		// Bound each SQL batch even for unusually large ways or relations.
		for(size_t start = 0; start < refs.size(); start += 1000)
		{
			string sql = "INSERT INTO " + connection.quote_name(prefix + "extract_" + suffix) +
				" (extract_id, id, version, index, member) VALUES ";
			bool any = false;
			for(size_t i = start; i < refs.size() && i < start + 1000; ++i)
			{
				if(types && suffix.back() != ObjectTypeName((*types)[i])[0]) continue;
				if(any) sql += ",";
				any = true;
				sql += "(" + to_string(extractId) + "," + to_string(id) + "," +
					to_string(version) + "," + to_string(i) + "," + to_string(refs[i]) + ")";
			}
			if(any) work.exec(sql + " ON CONFLICT (extract_id, id, index) DO NOTHING");
		}
	}
public:
	ExtractDatabaseWriter(pqxx::connection &c, pqxx::transaction_base &w,
		const string &p, int64_t id): connection(c), work(w), prefix(p), extractId(id) {}
	void StoreNode(const OsmNode &node) override
	{
		CopyObject("node", node.objId);
	}
	void StoreWay(const OsmWay &way) override
	{
		CopyObject("way", way.objId);
		Membership("way_mems", way.objId, way.metaData.version, way.refs);
	}
	void StoreRelation(const OsmRelation &relation) override
	{
		vector<int64_t> refs;
		vector<ObjectType> types;
		for(const RelationMember &member : relation.members)
		{
			refs.push_back(member.ref);
			types.push_back(member.type);
		}
		CopyObject("relation", relation.objId);
		for(const char *suffix : {"relation_mems_n", "relation_mems_w", "relation_mems_r"})
			Membership(suffix, relation.objId, relation.metaData.version, refs, &types);
	}
};

void PgTransaction::LockExtractTables(const string &accessMode)
{
	if(accessMode != "ACCESS SHARE" && accessMode != "EXCLUSIVE")
		throw invalid_argument("Unsupported extract lock mode");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	// The constructor acquires static then active main-map locks first.
	// Every extract operation must use this same table order, in one statement,
	// before accessing any extract metadata, objects or membership rows.
	string sql = "LOCK TABLE ";
	bool first = true;
	for(const char *name : {"extracts", "extract_livenodes", "extract_liveways",
		"extract_liverelations", "extract_way_mems", "extract_relation_mems_n",
		"extract_relation_mems_w", "extract_relation_mems_r"})
	{
		if(!first) sql += ",";
		first = false;
		sql += dbconn->quote_name(tableActivePrefix + name);
	}
	work->exec(sql + " IN " + accessMode + " MODE;");
	// Locks are owned by the transaction and released at commit or abort.
}

int64_t PgTransaction::SaveExtract(const vector<double> &bbox, const string &name)
{
	if(bbox.size() != 4) throw invalid_argument("Bbox must have four coordinates");
	for(double value : bbox)
		if(!std::isfinite(value)) throw invalid_argument("Bbox coordinates must be finite");
	if(bbox[0] < -180 || bbox[2] > 180 || bbox[1] < -90 || bbox[3] > 90 ||
		bbox[0] >= bbox[2] || bbox[1] >= bbox[3])
		throw invalid_argument("Bbox must be a nonempty longitude/latitude rectangle");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	string envelope = "ST_MakeEnvelope(";
	for(size_t i=0; i<4; ++i) envelope += work->quote(bbox[i]) + ",";
	envelope += "4326)";
	return SaveExtractArea(envelope, bbox, "", name);
}

int64_t PgTransaction::SaveExtractWkt(const string &wkt, const string &name)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while creating an extract");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	string boundary = DbCheckExtractBoundary(*work, wkt);
	return SaveExtractArea("ST_GeomFromText(" + work->quote(boundary) + ", 4326)", vector<double>(), boundary, name);
}

std::string PgTransaction::BufferExtractBoundary(const string &wkt, double bufferMetres)
{
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	return DbBufferExtractBoundary(*work, wkt, bufferMetres);
}

int64_t PgTransaction::SaveExtractArea(const string &areaSql, const vector<double> &bbox,
	const string &wkt, const string &name)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while creating an extract");
	LockExtractTables("EXCLUSIVE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	// Capture both cursors from the same snapshot as the map query. No later
	// commits can advance these checkpoints beyond the contents being extracted.
	string activity = dbconn->quote_name(tableActivePrefix + "edit_activity");
	auto checkpoint = work->exec("SELECT COALESCE(max(id),0), COALESCE(max(atomic_edit_id),0) FROM " + activity);
	auto mode = work->exec("SELECT value FROM " + dbconn->quote_name(tableActivePrefix + "meta") +
		" WHERE key='useBboxInQuery'");
	bool bboxMode = !mode.empty() && atoi(mode[0][0].as<string>().c_str()) == 1;
	string sql = "INSERT INTO " + dbconn->quote_name(tableActivePrefix + "extracts") +
		" (name, bbox, use_bbox_in_query, performed_at, edit_activity_id, atomic_edit_id) VALUES (" +
		work->quote(name) + "," + areaSql + "," + (bboxMode ? "true" : "false") +
		",CURRENT_TIMESTAMP," + checkpoint[0][0].as<string>() + "," +
		checkpoint[0][1].as<string>() + ") RETURNING id";
	int64_t id = work->exec(sql)[0][0].as<int64_t>();
	shared_ptr<IDataStreamHandler> sink = make_shared<ExtractDatabaseWriter>(*dbconn, *work, tableActivePrefix, id);
	auto query = GetQueryMgr();
	int status = wkt.empty() ? query->Start(bbox, time(nullptr), sink) : query->Start(wkt, time(nullptr), sink);
	while(status == 0) status = query->Continue();
	if(status < 0) throw runtime_error("Extract map query failed");
	return id;
}

std::map<std::string, std::string> PgTransaction::GetExtractHeaderAttribs(int64_t extractId, const string &name,
	const string &publicUrl)
{
	std::vector<ExtractInfo> extracts, selected;
	this->ListExtracts(extracts);
	for(const ExtractInfo &info : extracts)
		if(extractId ? info.extractId == extractId : info.name == name)
			selected.push_back(info);
	TagMap attribs;
	if(selected.size() != 1)
		return attribs; // Not found or ambiguous is reported by the export itself
	const ExtractInfo &info = selected[0];
	// The IDs an extract is current to are its stored checkpoint, not the
	// map's latest: the extract may be waiting for an update
	if(info.editActivityId >= 0 && info.atomicEditId >= 0)
	{
		attribs["edit_activity_id"] = std::to_string(info.editActivityId);
		attribs["atomic_edit_id"] = std::to_string(info.atomicEditId);
	}
	if(info.rectangular)
	{
		string bbox;
		for(size_t i = 0; i < info.bbox.size(); i++)
			bbox += (i ? "," : "") + ShortestDouble(info.bbox[i]);
		attribs["bbox"] = bbox;
	}
	else
		attribs["boundary"] = info.boundaryWkt;
	// An extract kept up from another server follows that server's edits, and
	// its checkpoint is in that server's numbering
	if(!info.updateUrl.empty())
		attribs["update_url"] = info.updateUrl;
	else if(!publicUrl.empty())
		attribs["update_url"] = publicUrl;
	return attribs;
}

int64_t PgTransaction::ExportExtractToFile(int64_t extractId, const string &name,
	const string &filename, bool updateInfo, const string &publicUrl)
{
	OsmFileWriter::FormatOf(filename); // Refuse an unusable name before any work
	TagMap headerAttribs;
	if(updateInfo)
		headerAttribs = this->GetExtractHeaderAttribs(extractId, name, publicUrl);
	// The writer publishes only a complete file, keeping an existing one on failure
	OsmFileWriter writer(filename, headerAttribs);
	writer.SetSortedTypeThenId(); // As an extract is exported
	int64_t id = this->ExportExtract(extractId, name, writer.Encoder());
	writer.Close();
	return id;
}

std::vector<int64_t> PgTransaction::ImportExtractFromFile(const string &filename, const string &name,
	const vector<double> &bbox, const string &wkt, int64_t editActivityId, int64_t atomicEditId)
{
	if(!bbox.empty() && !wkt.empty())
		throw invalid_argument("Give a bbox or a boundary, not both");
	auto importer = this->StartImportExtract(name, bbox, editActivityId, atomicEditId);
	if(!wkt.empty())
		importer->SetBoundaryWkt(wkt);
	LoadOsmFromFile(filename, importer);
	return {importer->GetId(), importer->GetNumNodes(), importer->GetNumWays(), importer->GetNumRelations()};
}

int64_t PgTransaction::UpdateExtract(int64_t extractId, const string &name)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Main map must be locked before updating an extract");
	LockExtractTables("EXCLUSIVE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	return DbUpdateExtract(*dbconn, *work, tableStaticPrefix, tableActivePrefix, extractId, name);
}

std::shared_ptr<PgExtractExport> PgTransaction::StartExportExtract(int64_t extractId,
    const string &name, shared_ptr<IDataStreamHandler> output)
{
    if(extractId < 0 || (!extractId && name.empty()) || !output)
        throw invalid_argument("Select an extract by positive ID or nonempty name");
    if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
        throw runtime_error("Map must be locked while exporting an extract");
    LockExtractTables("ACCESS SHARE");
    return shared_ptr<PgExtractExport>(new PgExtractExport(dbconn, sharedWork,
        tableActivePrefix, extractId, name, output));
}

int64_t PgTransaction::ExportExtract(int64_t extractId, const string &name,
    shared_ptr<IDataStreamHandler> output)
{
    auto exporter = StartExportExtract(extractId, name, output);
    while(exporter->Continue() != 1) {}
    return exporter->GetId();
}

std::shared_ptr<PgExtractImport> PgTransaction::StartImportExtract(const string &name,
	const vector<double> &bbox, int64_t editActivityId, int64_t atomicEditId)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while importing an extract");
	LockExtractTables("EXCLUSIVE");
	return shared_ptr<PgExtractImport>(new PgExtractImport(dbconn, sharedWork,
		tableActivePrefix, name, bbox, editActivityId, atomicEditId));
}

std::shared_ptr<PgExtractPatch> PgTransaction::StartPatchExtract(int64_t extractId)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while changing an extract");
	if(extractId <= 0)
		throw invalid_argument("Select the extract by positive ID");
	LockExtractTables("EXCLUSIVE");
	return shared_ptr<PgExtractPatch>(new PgExtractPatch(dbconn, sharedWork, tableActivePrefix, extractId));
}

int64_t PgTransaction::SetExtractAutoUpdate(int64_t extractId, const string &name,
	bool autoUpdate, const string &updateUrl)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while changing an extract");
	LockExtractTables("EXCLUSIVE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	return DbSetExtractAutoUpdate(*dbconn, *work, tableActivePrefix, extractId, name, autoUpdate, updateUrl);
}

int64_t PgTransaction::DeleteExtract(int64_t extractId, const string &name)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while deleting an extract");
	LockExtractTables("EXCLUSIVE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	return DbDeleteExtract(*dbconn, *work, tableActivePrefix, extractId, name);
}

void PgTransaction::ListExtracts(vector<ExtractInfo> &out)
{
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while listing extracts");
	LockExtractTables("ACCESS SHARE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	DbListExtracts(*dbconn, *work, tableActivePrefix, 0, false, out);
}

bool PgTransaction::GetExtract(int64_t extractId, ExtractInfo &out)
{
	if(extractId <= 0) throw invalid_argument("Extract ID must be positive");
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while reading an extract");
	LockExtractTables("ACCESS SHARE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	vector<ExtractInfo> found;
	DbListExtracts(*dbconn, *work, tableActivePrefix, extractId, true, found);
	if(found.empty()) return false;
	out = found[0];
	return true;
}

void PgTransaction::CompareExtract(int64_t extractId, const string &name,
	ExtractComparison &out)
{
	DbCompareExtract(*this, extractId, name, out);
}

void PgTransaction::CompareAllExtracts(vector<ExtractComparison> &out)
{
	out.clear();
	if(shareMode != "ACCESS SHARE" && shareMode != "EXCLUSIVE")
		throw runtime_error("Map must be locked while comparing extracts");
	LockExtractTables("ACCESS SHARE");
	auto work = sharedWork->work;
	if(!work) throw runtime_error("Transaction has been deleted");
	for(int64_t id : DbListExtractIds(*dbconn, *work, tableActivePrefix))
	{
		out.emplace_back();
		DbCompareExtract(*this, id, "", out.back());
	}
}

void PgTransaction::EditActivityBeforeStore(const std::string &action, std::shared_ptr<class OsmData> block,
	std::shared_ptr<class OsmData> affectedParentsOut)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	if(!this->Recorder(work.get()).Enabled())
		throw runtime_error("Edit activity needs map schema version 14");
	activityRecorder->BeforeStore(action, *block);
	if(affectedParentsOut)
		*affectedParentsOut = activityRecorder->AffectedParents();
}

bool PgTransaction::EditActivityAfterStore(const std::string &action, std::shared_ptr<class OsmData> block,
	int64_t changeset, int64_t uid, int64_t timestamp, class PgMapError &errStr)
{
	if(!activityRecorder)
		throw runtime_error("EditActivityBeforeStore has not been called");
	class EditActivity activity;
	activityRecorder->Build(action, *block, activity);
	// An upload is one changeset by one user at one moment
	activity.changeset = changeset;
	activity.uid = uid;
	activity.timestamp = timestamp;
	return this->InsertEditActivity(activity, errStr);
}

class EditActivityRecorder &PgTransaction::Recorder(pqxx::transaction_base *work)
{
	// Only an edit records, and the edit's atomic edit ID is allocated only
	// once the exclusive map locks are held, so that the next writer cannot
	// allocate until this transaction commits or aborts
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	if(!activityRecorder)
		activityRecorder.reset(new class EditActivityRecorder(*dbconn, work, this->tableActivePrefix, this));
	return *activityRecorder;
}

bool PgTransaction::InsertEditActivity(const class EditActivity &activity,
		class PgMapError &errStr)
{
	/*
	 * Retaining context for rectangular extract synchronization
	 * -------------------------------------------------------
	 * An extract is a map-query result, not simply nodes inside its rectangle.
	 * In membership-based query mode, an inside node selects its parent ways,
	 * and all nodes of those ways are returned, including outside nodes. Direct
	 * relations of those returned nodes/ways are also selected. In bbox mode,
	 * stored way/relation envelopes determine selection instead. Neither mode
	 * implies unlimited expansion of the graph or completion of all relations.
	 * Consequently, an edit to an outside node can affect an extract, and moving
	 * a node inside can require adding an unchanged way and its other nodes.
	 * Recording only modified objects or their new positions is insufficient.
	 *
	 * The caller captures original object state before applying a block, then
	 * constructs this activity after applying it, while the map remains locked.
	 * existing/updated record object type, ID and version; affectedparents and
	 * related retain additional versioned references. Full object contents stay
	 * in the map's static, live and history tables rather than being duplicated
	 * in each activity row. Type is required because IDs are per object type;
	 * version is required because fetching today's object by ID would lose the
	 * state associated with this edit. Referenced history must remain available
	 * for as long as consumers are allowed to synchronize from these records.
	 * All lookups must respect the static baseline and active overlay, including
	 * suppression of overridden/deleted static objects.
	 *
	 * Spatial extents are retained beside the object lists as SRID-4326
	 * GeometryCollections: existing_bbox for existing, updated_bbox for updated,
	 * and affectedparents_before_bbox/affectedparents_after_bbox for
	 * affectedparents. Component i+1 of a collection belongs to entry i of its
	 * list: counts AND ordering must agree. A way or relation has its bounding
	 * rectangle (a relation spread across the antimeridian, a MultiPolygon of
	 * its two, one either side); a node, whose position is in the object itself, or an object
	 * whose extent is unknown, has POLYGON EMPTY as a placeholder. Both sides
	 * matter for an affected parent because its version does not change when a
	 * node moves, yet its extent can, so it may enter or leave a rectangle.
	 * An object version alone also does not identify its members' versions at
	 * this revision; retaining footprints does not reconstruct the whole graph.
	 * A column that was not recorded is SQL NULL; a list with no objects is
	 * paired with GEOMETRYCOLLECTION EMPTY.
	 *
	 * A future consumer uses retained spatial/dependency context to find candidate
	 * edits, retrieves the referenced historical objects, and determines which
	 * objects enter, leave, or change in its query result. Spatial overlap is a
	 * candidate filter, not proof of membership. Before removing a completion
	 * node or relation, other retained selection reasons must be checked. Leaving
	 * an extract is distinct from deletion from the global map. The correctness
	 * target is the result of a fresh query at the consumer's target revision.
	 *
	 * One upload may contain multiple action blocks. This method gives all rows
	 * in this PgTransaction one atomic_edit_id and ordered block_index values.
	 * Activity inserts and map changes share the same database transaction: a
	 * later validation/storage failure rolls them all back. Consumers must apply
	 * complete groups before advancing their checkpoint, not individual rows or
	 * timestamp windows. The exclusive locks serialize ID allocation and remain
	 * held until commit/abort; sequence gaps after rollback are harmless. Legacy
	 * rows with NULL grouping cannot be assumed to share transaction boundaries.
	 */
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	EditActivity groupedActivity(activity);
	groupedActivity.atomicEditId = this->Recorder(work.get()).AtomicEditId();
	groupedActivity.blockIndex = activityBlockIndex;
	bool ok = DbInsertEditActivity(*dbconn, work.get(), this->tableActivePrefix,
		groupedActivity,
		nativeErrStr,
		0);
	if(ok) ++activityBlockIndex;

	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgTransaction::ResetActiveTables(class PgMapError &errStr)
{
	std::string nativeErrStr;
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = ::ResetActiveTables(*dbconn, work.get(), this->tableActivePrefix, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

void PgTransaction::GetReplicateDiff(int64_t timestampStart, int64_t timestampEnd, class OsmChange &out)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	std::shared_ptr<class OsmData> osmData(new class OsmData);

	GetReplicateDiffNodes(*dbconn, work.get(), this->dbUsernameLookup, 
		this->tableStaticPrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffNodes(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffNodes(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffNodes(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffWays(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffWays(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffWays(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffWays(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffRelations(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffRelations(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, true, timestampStart, timestampEnd, out);

	GetReplicateDiffRelations(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, false, timestampStart, timestampEnd, out);

	GetReplicateDiffRelations(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, true, timestampStart, timestampEnd, out);
}

/**
* Dump live objects. Only current nodes are dumped, not old (non-visible) nodes.
*/
void PgTransaction::Dump(bool order, bool nodes, bool ways, bool relations, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	enc->StoreIsDiff(false);

	if(nodes)
	{
		DumpNodes(*dbconn, work.get(), this->dbUsernameLookup, this->tableActivePrefix, order, enc);

		enc->Reset();
	}

	if(ways)
	{
		DumpWays(*dbconn, work.get(), this->dbUsernameLookup, this->tableActivePrefix, order, enc);

		enc->Reset();
	}

	if(relations)
	{	
		DumpRelations(*dbconn, work.get(), this->dbUsernameLookup, this->tableActivePrefix, order, enc);
	}

	enc->Finish();
}

int64_t PgTransaction::GetAllocatedId(const string &type)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	class PgMapError perrStr;
	if(atoi(this->GetMetaValue("readonly", perrStr).c_str()) == 1)
	{
		perrStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	string errStr;
	int64_t val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = GetAllocatedIdFromDb(*dbconn, work.get(),
		this->tableActivePrefix,
		type, true, errStr, val);
	if(!ok)
		throw runtime_error(errStr);
	return val;
}

int PgTransaction::GetChangeset(int64_t objId,
	class PgChangeset &changesetOut,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	string errStrNative;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	int ret = GetChangesetFromDb(*dbconn, work.get(),
		this->tableActivePrefix,
		this->dbUsernameLookup, 
		objId,
		changesetOut,
		errStrNative);

	if(ret == -1)
	{
		ret = GetChangesetFromDb(*dbconn, work.get(),
			this->tableStaticPrefix,
			this->dbUsernameLookup, 
			objId,
			changesetOut,
			errStrNative);
	}

	errStr.errStr = errStrNative;
	return ret;
}

int PgTransaction::GetChangesetOsmChange(int64_t changesetId,
	std::shared_ptr<class IOsmChangeHandler> output,
	class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	std::shared_ptr<class OsmData> data(new class OsmData());
	GetAllNodesByChangeset(*dbconn, work.get(), this->dbUsernameLookup, 
		this->tableStaticPrefix,
		"", changesetId,
		data);
	GetAllNodesByChangeset(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix,
		"", changesetId,
		data);

	GetAllWaysByChangeset(*dbconn, work.get(), this->dbUsernameLookup,	
		this->tableStaticPrefix,
		"", changesetId,
		data);
	GetAllWaysByChangeset(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix,
		"", changesetId,
		data);

	GetAllRelationsByChangeset(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix,
		"", changesetId,
		data);
	GetAllRelationsByChangeset(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix,
		"", changesetId,
		data);

	class OsmData created, modified, deleted;
	FilterObjectsInOsmChange(1, *data, created);
	FilterObjectsInOsmChange(2, *data, modified);
	FilterObjectsInOsmChange(3, *data, deleted);

	const std::pair<const char *, const OsmData *> groups[] = {
		{"create", &created}, {"modify", &modified}, {"delete", &deleted}};
	for(const auto &group : groups)
	{
		if(group.second->IsEmpty())
			continue;
		OsmChangeBlock block(group.first);
		block.data = *group.second;
		output->StoreChangeBlock(block);
	}
	return 1;
}

bool PgTransaction::GetChangesets(std::vector<class PgChangeset> &changesetsOut,
	int64_t user_uid, //0 means don't filter
	int64_t openedBeforeTimestamp, //-1 means don't filter
	int64_t closedAfterTimestamp, //-1 means don't filter
	bool is_open_only,
	bool is_closed_only,
	class PgMapError &errStr)
{
	class PgChangesetQuery query;
	query.user_uid = user_uid;
	query.openedBeforeTimestamp = openedBeforeTimestamp;
	query.closedAfterTimestamp = closedAfterTimestamp;
	query.is_open_only = is_open_only;
	query.is_closed_only = is_closed_only;
	return this->GetChangesets(changesetsOut, query, errStr);
}

bool PgTransaction::GetChangesets(std::vector<class PgChangeset> &changesetsOut,
	const class PgChangesetQuery &query,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	string errStrNative;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = GetChangesetsFromDb(*dbconn, work.get(),
		this->tableActivePrefix, "",
		this->dbUsernameLookup, 
		query,
		changesetsOut,
		errStrNative);
	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	//Either set of tables could hold all of the changesets wanted, so each is
	//asked for the full number before the two are merged
	std::vector<class PgChangeset> changesetsStatic;
	ok = GetChangesetsFromDb(*dbconn, work.get(),
		this->tableStaticPrefix,
		this->tableActivePrefix,
		this->dbUsernameLookup, 
		query,
		changesetsStatic,
		errStrNative);
	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}
	changesetsOut.insert(changesetsOut.end(), changesetsStatic.begin(), changesetsStatic.end());

	bool oldestFirst = query.oldestFirst;
	std::stable_sort(changesetsOut.begin(), changesetsOut.end(),
		[oldestFirst](const PgChangeset &a, const PgChangeset &b) {
			if(a.open_timestamp != b.open_timestamp)
				return oldestFirst ? a.open_timestamp < b.open_timestamp : a.open_timestamp > b.open_timestamp;
			return oldestFirst ? a.objId < b.objId : a.objId > b.objId;
		});
	if(query.limit > 0 && changesetsOut.size() > query.limit)
		changesetsOut.resize(query.limit);
	return true;
}

void PgTransaction::GetChangesetChangeCounts(std::vector<class PgChangeset> &changesets)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	DbGetChangesetChangeCounts(*dbconn, work.get(), this->tableActivePrefix, changesets);
}

void PgTransaction::GetChangesetChangeCounts(class PgChangeset &changeset)
{
	std::vector<class PgChangeset> changesets = {changeset};
	this->GetChangesetChangeCounts(changesets);
	changeset = changesets[0];
}

int64_t PgTransaction::GetChangesetCount(int64_t user_uid)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	return DbCountChangesets(*dbconn, work.get(), this->tableActivePrefix, "", user_uid) +
		DbCountChangesets(*dbconn, work.get(), this->tableStaticPrefix, this->tableActivePrefix, user_uid);
}

int64_t PgTransaction::CreateChangeset(const class PgChangeset &changeset,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;	
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	class PgChangeset changesetMod(changeset);
	if(changesetMod.objId != 0)
		throw invalid_argument("Changeset ID should be zero since it is allocated by pgmap");

	int64_t cid = this->GetAllocatedId("changeset");
	changesetMod.objId = cid;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = InsertChangesetInDb(*dbconn, work.get(),
		this->tableActivePrefix,
		changesetMod,
		errStrNative);

	errStr.errStr = errStrNative;
	if(!ok)
		return 0;
	return cid;
}

bool PgTransaction::UpdateChangeset(const class PgChangeset &changeset,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;	
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Attempt to update in active table
	int rowsAffected = UpdateChangesetInDb(*dbconn, work.get(),
		this->tableActivePrefix,
		changeset,
		errStrNative);

	if(rowsAffected < 0)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	if(rowsAffected > 0)
		return true; //Success

	//Update a changeset in the static tables by copying to active table
	//and updating its values there
	size_t rowsAffected2 = 0;
	bool ok = CopyChangesetToActiveInDb(*dbconn, work.get(),
		this->tableStaticPrefix,
		this->tableActivePrefix,
		this->dbUsernameLookup, 
		changeset.objId,
		rowsAffected2,
		errStrNative);

	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}
	if(rowsAffected2 == 0)
	{
		errStr.errStr = "No changeset found in active or static table";
		return false;
	}

	return rowsAffected2 > 0;
}

bool PgTransaction::ExpandChangesetBbox(int64_t cid,
	const std::vector<double> &bbox,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;	
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Attempt to update in active table
	int rowsAffected = DbExpandChangesetBbox(*dbconn, work.get(),
		this->tableActivePrefix,
		cid,
		bbox,
		errStrNative);

	if(rowsAffected < 0)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	return rowsAffected > 0;
}

bool PgTransaction::CloseChangeset(int64_t changesetId,
	int64_t closedTimestamp,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	size_t rowsAffected = 0;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = CloseChangesetInDb(*dbconn, work.get(),
		this->tableActivePrefix,
		changesetId,
		closedTimestamp,
		rowsAffected,
		errStrNative);

	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	if(rowsAffected == 0)
	{
		//Close a changeset in the static tables by copying to active table
		//and setting the is_open flag to false.
		ok = CopyChangesetToActiveInDb(*dbconn, work.get(),
			this->tableStaticPrefix,
			this->tableActivePrefix,
			this->dbUsernameLookup, 
			changesetId,
			rowsAffected,
			errStrNative);

		if(!ok)
		{
			errStr.errStr = errStrNative;
			return false;
		}
		if(rowsAffected == 0)
		{
			errStr.errStr = "No changeset found in active or static table";
			return false;
		}

		ok = CloseChangesetInDb(*dbconn, work.get(),
			this->tableActivePrefix,
			changesetId,
			closedTimestamp,
			rowsAffected,
			errStrNative);
	}

	errStr.errStr = errStrNative;
	return ok;
}

bool PgTransaction::CloseChangesetsOlderThan(int64_t whereBeforeTimestamp,
	int64_t closedTimestamp,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;
	if(atoi(this->GetMetaValue("readonly", errStr).c_str()) == 1)
	{
		errStr.errStr = "Database is in READ ONLY mode";
		return false;
	}

	size_t rowsAffected = 0;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	//Find changesets in static that have not been closed in active tables
	std::vector<class PgChangeset> openStaticChangesets;
	class PgChangesetQuery openQuery;
	openQuery.limit = 0; //Get all
	openQuery.openedBeforeTimestamp = whereBeforeTimestamp;
	openQuery.is_open_only = true;
	bool ok = GetChangesetsFromDb(*dbconn, work.get(),
		this->tableStaticPrefix, this->tableActivePrefix,
		this->dbUsernameLookup, 
		openQuery,
		openStaticChangesets,
		errStrNative);
	if(!ok)
	{
		errStr.errStr = errStrNative;
		return false;
	}

	//Migrate open static changesets to active table
	for(size_t i=0; i<openStaticChangesets.size(); i++)
	{
		size_t rowsAffected = 0;
		ok = CopyChangesetToActiveInDb(*dbconn, work.get(),
			this->tableStaticPrefix,
			this->tableActivePrefix,
			this->dbUsernameLookup, 
			openStaticChangesets[i].objId,
			rowsAffected,
			errStrNative);

		if(!ok)
		{
			errStr.errStr = errStrNative;
			return false;
		}
	}

	//Close changesets in active table
	ok = CloseChangesetsOlderThanInDb(*dbconn, work.get(),
		this->tableActivePrefix,
		whereBeforeTimestamp,
		closedTimestamp,
		rowsAffected,
		errStrNative);

	errStr.errStr = errStrNative;
	return ok;
}

bool PgTransaction::GetEditActivityById(int64_t editActivityId,
	class EditActivity &editActivity,
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	string errStrNative;
	std::string val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool found = DbGetEditActivityById(*dbconn, work.get(),
		this->tableActivePrefix,
		editActivityId,
		editActivity,
		errStrNative);

	errStr.errStr = errStrNative;
	return found;
}

std::pair<int64_t, int64_t> PgTransaction::GetLatestEditIds()
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work) throw runtime_error("Transaction has been deleted");
	std::pair<int64_t, int64_t> ids(0, 0);
	DbGetLatestEditIds(*dbconn, work.get(), this->tableActivePrefix, ids.first, ids.second);
	return ids;
}

int64_t PgTransaction::GetAtomicEditIdAtTime(int64_t timestamp)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work) throw runtime_error("Transaction has been deleted");
	return DbGetAtomicEditIdAtTime(*dbconn, work.get(), this->tableActivePrefix, timestamp);
}

std::map<std::string, std::string> PgTransaction::GetLatestEditIdAttribs()
{
	auto ids = this->GetLatestEditIds();
	return {{"edit_activity_id", to_string(ids.first)}, {"atomic_edit_id", to_string(ids.second)}};
}

void PgTransaction::QueryEditActivityByIds(int64_t firstId, int64_t lastId,
	int64_t atomicEditId, std::vector<std::shared_ptr<class EditActivity> > &editActivity,
	class PgMapError &errStr, int64_t limit, const std::vector<double> &bbox, bool newest,
	const std::string &wkt)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	if(firstId < 0 || lastId < 0 || atomicEditId < 0 || (lastId > 0 && lastId < firstId))
		throw invalid_argument("Invalid activity ID range");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work) throw runtime_error("Transaction has been deleted");
	DbQueryEditActivityByIds(*dbconn, work.get(), this->tableActivePrefix,
		firstId, lastId, atomicEditId, editActivity, errStr.errStr, limit, bbox, newest, wkt);
}

void PgTransaction::QueryEditActivityByTimestamp(int64_t sinceTimestamp,
	int64_t untilTimestamp,
	std::vector<std::shared_ptr<class EditActivity> > &editActivity,
	class PgMapError &errStr, int64_t limit, const std::vector<double> &bbox, bool newest)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	string errStrNative;
	std::string val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbQueryEditActivityByTimestamp(*dbconn, work.get(),
		this->tableActivePrefix,
		sinceTimestamp, untilTimestamp,
		editActivity,
		errStrNative, limit, bbox, newest);

	errStr.errStr = errStrNative;
}

std::string PgTransaction::GetMetaValue(const std::string &key, 
	class PgMapError &errStr)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	string errStrNative;
	std::string val;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	try
	{
		val = DbGetMetaValue(*dbconn, work.get(),
			key, 
			this->tableActivePrefix,
			errStrNative);
	}
	catch(runtime_error &err)
	{
		//Hard coded defaults
		if(key == "readonly")
			return "0";

		throw err;
	}
	return val;
}

bool PgTransaction::SetMetaValue(const std::string &key, 
	const std::string &value, 
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ret = DbSetMetaValue(*dbconn, work.get(),
		key, 
		value, 
		this->tableActivePrefix, 
		errStrNative);
	errStr.errStr = errStrNative;
	return ret;
}

bool PgTransaction::UpdateUsername(int uid, const std::string &username,
	class PgMapError &errStr)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	string errStrNative;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbUpsertUsernamePrepare(*dbconn, work.get(), this->tableActivePrefix);

	DbUpsertUsername(*dbconn, work.get(), this->tableActivePrefix, 
		uid, username);

	return true;
}

void PgTransaction::DumpToFile(const std::string &filename, bool updateInfo, const std::string &publicUrl)
{
	OsmFileWriter::FormatOf(filename); // Refuse an unusable name before any work
	TagMap headerAttribs;
	if(updateInfo)
	{
		headerAttribs = this->GetLatestEditIdAttribs(); // From the same snapshot as the dump
		if(!publicUrl.empty())
			headerAttribs["update_url"] = publicUrl;
	}
	OsmFileWriter writer(filename, headerAttribs);
	writer.SetSortedTypeThenId(); // Each kind in ID order, as asked for here
	this->Dump(true, true, true, true, writer.Encoder());
	writer.Close();
}

void PgTransaction::OverpassQuery(const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	std::shared_ptr<IDataStreamHandler> enc,
	const OverpassQueryOptions &options)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbOverpassQueryObjVisible(*dbconn, work.get(),
		this->dbUsernameLookup,
		this->tableActivePrefix,
		objType, filters, bbox, ids, limit, enc, options);
}

void PgTransaction::OverpassQueryIds(const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	std::vector<int64_t> &idsOut,
	const OverpassQueryOptions &options)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbOverpassQueryIdsVisible(*dbconn, work.get(),
		this->tableActivePrefix,
		objType, filters, bbox, ids, limit, idsOut, options);
}

void PgTransaction::GetMetaValues(std::map<std::string, std::string> &valuesOut, bool staticTables)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	DbGetMetaValues(*dbconn, work.get(),
		staticTables ? this->tableStaticPrefix : this->tableActivePrefix, valuesOut);
}

bool PgTransaction::DeleteMetaValue(const std::string &key)
{
	if(this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	return DbDeleteMetaValue(*dbconn, work.get(), key, this->tableActivePrefix);
}

std::vector<int64_t> PgTransaction::CountWaysWithoutBbox()
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	pqxx::result r = work->exec("SELECT count(*), count(*) - count(bbox) FROM " +
		dbconn->quote_name(this->tableActivePrefix + "visibleways") + ";");
	return {r[0][0].as<int64_t>(), r[0][1].as<int64_t>()};
}

std::vector<int64_t> PgTransaction::EstimateWaysWithoutBbox()
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	// The way tables the visible ways come from: the base ones and the active
	// ones. Counting them is a read of every way, which on a planet takes
	// minutes; a table this small or smaller is counted.
	const int64_t countedBytes = 256 * 1024 * 1024;
	std::vector<string> tables;
	if(!this->tableStaticPrefix.empty() && this->tableStaticPrefix != this->tableActivePrefix)
		tables.push_back(this->tableStaticPrefix + "liveways");
	tables.push_back(this->tableActivePrefix + "liveways");

	auto statistics = [&](const string &table) {
		return work->exec("SELECT c.reltuples::bigint, (SELECT s.null_frac FROM pg_stats s WHERE s.schemaname = n.nspname "
			"AND s.tablename = c.relname AND s.attname = 'bbox'), pg_relation_size(c.oid) FROM pg_class c "
			"JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass(" +
			work->quote(dbconn->quote_name(table)) + ")");
	};

	bool small = true;
	for(const string &table : tables)
	{
		pqxx::result r = statistics(table);
		if(!r.empty() && r[0][2].as<int64_t>() > countedBytes)
			small = false;
	}
	if(small)
	{
		std::vector<int64_t> counted = this->CountWaysWithoutBbox();
		counted.push_back(1);
		return counted;
	}

	// Rows times the share of them with no box, as ANALYZE last found them.
	// Ways the active tables have taken over from the base ones are counted
	// in both, which an estimate can bear.
	double total = 0.0, missing = 0.0;
	for(const string &table : tables)
	{
		pqxx::result r = statistics(table);
		if(r.empty()) continue;
		if(r[0][0].as<int64_t>() < 0 || r[0][1].is_null())
		{
			// Never analysed: a sample of its rows is enough
			work->exec("ANALYZE " + dbconn->quote_name(table) + " (bbox);");
			r = statistics(table);
		}
		double rows = std::max<double>(0.0, r[0][0].as<double>());
		double nullShare = r[0][1].is_null() ? 0.0 : r[0][1].as<double>();
		total += rows;
		missing += rows * nullShare;
	}
	return {(int64_t)std::llround(total), (int64_t)std::llround(missing), 0};
}

bool PgTransaction::UseBboxInQuery()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	// Read as the map query reads it, so both treat the map the same way
	string errStrNative, value;
	try
	{
		value = DbGetMetaValue(*dbconn, work.get(), "useBboxInQuery",
			this->tableActivePrefix, errStrNative);
	}
	catch(runtime_error &err)
	{
	}
	return atoi(value.c_str()) == 1;
}

void PgTransaction::SetStatementTimeout(int64_t milliseconds)
{
	if(milliseconds < 0)
		throw invalid_argument("Timeout must not be negative");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	// SET LOCAL lasts until the transaction ends, so nothing leaks to its next user
	work->exec("SET LOCAL statement_timeout = " + to_string(milliseconds));
}

void PgTransaction::GetMostActiveUsers(int64_t startTimestamp,
	std::vector<int64_t> &uidOut,
	std::vector<std::vector<int64_t> > &objectCountOut)
{
	if(this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbGetMostActiveUsers(*dbconn, work.get(), 
		this->tableActivePrefix, 
		startTimestamp,
		uidOut,
		objectCountOut);
}

void PgTransaction::Commit()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	//Release locks
	work->commit();
}

void PgTransaction::Abort()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	work->abort();
}

// **********************************************

PgAdmin::PgAdmin(shared_ptr<pqxx::connection> dbconnIn,
		const string &tableStaticPrefixIn, 
		const string &tableModPrefixIn,
		const string &tableTestPrefixIn,
		std::shared_ptr<class PgWork> sharedWorkIn,
		const string &shareModeIn):

	PgCommon(dbconnIn, tableStaticPrefixIn, tableModPrefixIn, sharedWorkIn, shareMode),
	tableModPrefix(tableModPrefixIn),
	tableTestPrefix(tableTestPrefixIn)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	if(shareMode.size() > 0)
	{
		string errStr;
		bool ok = LockMap(work, this->tableStaticPrefix, this->shareMode, errStr);
		if(!ok)
			throw runtime_error(errStr);
		ok = LockMap(work, this->tableModPrefix, this->shareMode, errStr);
		if(!ok)
			throw runtime_error(errStr);
		ok = LockMap(work, this->tableTestPrefix, this->shareMode, errStr);
		if(!ok)
			throw runtime_error(errStr);
	}
}

PgAdmin::~PgAdmin()
{
	this->sharedWork->work.reset();
	this->sharedWork.reset();
}

bool PgAdmin::IsAdminMode()
{
	return true;
}

bool PgAdmin::CreateMapTables(int verbose, int targetVer, bool latest, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, "", this->tableStaticPrefix, targetVer, latest, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, this->tableStaticPrefix, this->tableModPrefix, targetVer, latest, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbSetSchemaVersion(*dbconn, work.get(), verbose, this->tableStaticPrefix, this->tableTestPrefix, targetVer, latest, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::DropMapTables(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	// Dropping everything does not need intermediate schema transformations.
	// Process dependent active views before the static tables they reference.
	for(const string &prefix : {this->tableTestPrefix, this->tableModPrefix, this->tableStaticPrefix})
	{
		if(!DbDropMapTables(*dbconn, work.get(), verbose, prefix, nativeErrStr))
		{
			errStr.errStr = nativeErrStr;
			return false;
		}
	}
	errStr.errStr.clear();
	bool ok = true;
	return ok;
}

bool PgAdmin::ImportMapFile(int verbose, const std::string &filename, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbImportMapFile(*dbconn, work.get(), verbose, filename, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::CreateMapIndices(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbCreateIndices(*dbconn, work.get(), verbose, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateIndices(*dbconn, work.get(), verbose, this->tableModPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateIndices(*dbconn, work.get(), verbose, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

int PgAdmin::GetLatestIndexVersion()
{
	return MAP_INDEX_VERSION;
}

bool PgAdmin::SetIndexVersion(int verbose, int targetVersion, class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	for(const string &prefix : {this->tableStaticPrefix, this->tableModPrefix, this->tableTestPrefix})
	{
		string nativeErrStr;
		if(!DbSetIndexVersion(*dbconn, work.get(), verbose, prefix, targetVersion, nativeErrStr))
		{
			errStr.errStr = nativeErrStr;
			return false;
		}
	}
	return true;
}

std::vector<int> PgAdmin::GetIndexVersions()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	std::vector<int> out;
	for(const string &prefix : {this->tableStaticPrefix, this->tableModPrefix, this->tableTestPrefix})
		out.push_back(DbGetIndexVersion(*dbconn, work.get(), prefix));
	return out;
}

std::vector<int> PgAdmin::GetSchemaVersions()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	std::vector<int> out;
	for(const string &prefix : {this->tableStaticPrefix, this->tableModPrefix, this->tableTestPrefix})
	{
		int version = 0;
		try
		{
			string nativeErrStr;
			version = std::stoi(DbGetMetaValue(*dbconn, work.get(), "schema_version", prefix, nativeErrStr));
		}
		catch(const std::exception &) {}
		out.push_back(version);
	}
	return out;
}

std::string PgAdmin::GetIndexVersionNotes()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	string notes;
	for(const string &prefix : {this->tableStaticPrefix, this->tableModPrefix, this->tableTestPrefix})
	{
		string note = DbIndexVersionNote(*dbconn, work.get(), prefix);
		if(!note.empty())
			notes += note + "\n";
	}
	return notes;
}

bool PgAdmin::ApplyDiffs(const std::string &diffPath, int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbApplyDiffs(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, diffPath, this, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

bool PgAdmin::RefreshMapIds(int verbose, class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");	

	ClearNextIdValuesById(*dbconn, work.get(), this->tableStaticPrefix, "node");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableStaticPrefix, "way");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableStaticPrefix, "relation");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableModPrefix, "node");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableModPrefix, "way");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableModPrefix, "relation");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableTestPrefix, "node");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableTestPrefix, "way");
	ClearNextIdValuesById(*dbconn, work.get(), this->tableTestPrefix, "relation");
	
	std::string nativeErrStr;

	bool ok = DbRefreshMaxIds(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

bool PgAdmin::ImportChangesetMetadata(const std::string &fina, int verbose, class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	class OsmChangesetsDecodeString osmChangesetsDecodeString;

	std::string content;
	try
	{
		int ret = ReadFileContents(fina.c_str(), 0, content);
		if(ret < 1)
		{
			errStr.errStr = "Error reading file";
			return false;
		}
	}
	catch(const std::bad_alloc &err)
	{
		errStr.errStr = "Failed to allocate buffer: ";
		errStr.errStr += err.what();
		return false;
	}
	cout << fina << "," << content.length() << endl;

	osmChangesetsDecodeString.DecodeSubString(content.c_str(), content.length(), 1);

	if(!osmChangesetsDecodeString.parseCompletedOk)
	{
		errStr = osmChangesetsDecodeString.errString;
		return false;
	}

	bool ok = true;
	for(size_t i=0; i<osmChangesetsDecodeString.outChangesets.size(); i++)
	{
		int rowsAffected = UpdateChangesetInDb(*dbconn, work.get(), 
			this->tableStaticPrefix,
			osmChangesetsDecodeString.outChangesets[i],
			errStr.errStr);

		if(rowsAffected==0)
			ok = InsertChangesetInDb(*dbconn, work.get(), 
				this->tableStaticPrefix,
				osmChangesetsDecodeString.outChangesets[i],
				errStr.errStr);
		if(!ok)
			break;
	}

	return ok;
}

bool PgAdmin::RefreshMaxChangesetUid(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbRefreshMaxChangesetUid(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

bool PgAdmin::GenerateUsernameTable(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbGenerateUsernameTable(*dbconn, work.get(), verbose, this->tableStaticPrefix, 
		this->tableModPrefix, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;

	return true;
}

bool PgAdmin::UpdateBboxes(int verbose, bool updateStatic, bool updateActive, class PgMapError &errStr,
	bool onlyMissingWays)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	bool ok = true;

    if(updateStatic)
    {
		ok = DbUpdateWayBboxes(*dbconn, work.get(), verbose, 
			this->tableStaticPrefix, 
			this,
			nativeErrStr, onlyMissingWays);
		errStr.errStr = nativeErrStr;
		if(!ok) return ok;
	}

	if (updateActive)
	{
		//Update way bboxes
		ok = DbUpdateWayBboxes(*dbconn, work.get(), verbose,
			this->tableModPrefix, 
			this,
			nativeErrStr, onlyMissingWays);
		errStr.errStr = nativeErrStr;
		if(!ok) return ok;
	}

    if(updateStatic)
	{
		ok = DbUpdateRelationBboxes(*dbconn, work.get(), verbose, 
			this->tableStaticPrefix, 
			this,
			nativeErrStr);
		errStr.errStr = nativeErrStr;
		if(!ok) return ok;
	}

	if (updateActive)
	{
		//Update relation bboxes
		ok = DbUpdateRelationBboxes(*dbconn, work.get(), verbose,
			this->tableModPrefix, 
			this,
			nativeErrStr);
		errStr.errStr = nativeErrStr;
		if(!ok) return ok;
	}

	std::vector<std::string> prefixes;
	if(updateStatic) prefixes.push_back(this->tableStaticPrefix);
	if(updateActive) prefixes.push_back(this->tableModPrefix);

	// Taken as one rectangle, what is spread across the antimeridian goes
	// round the world: those relations are worked out again, in two pieces
	if(!SplitWrapped(prefixes, verbose, errStr))
		return false;

	// The shapes kept for map renderers, where there are tables for them: the
	// base tables' first, which the active ones' relations may be made from,
	// and in each, ways before the relations made from them
	for(const std::string &prefix : prefixes)
		for(const char *kind : {"way", "relation"})
		{
			ok = DbUpdateObjectShapes(*dbconn, work.get(), verbose, prefix, kind, onlyMissingWays, nativeErrStr);
			errStr.errStr = nativeErrStr;
			if(!ok) return ok;
		}

	work->commit();

	return true;
}

bool PgAdmin::SetObjectShapes(bool enabled, int verbose, class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	// The base tables first, as the others' views show their columns
	std::string nativeErrStr;
	bool ok = DbSetObjectShapes(*dbconn, work.get(), verbose, "", this->tableStaticPrefix, enabled, nativeErrStr) &&
		DbSetObjectShapes(*dbconn, work.get(), verbose, this->tableStaticPrefix, this->tableModPrefix, enabled, nativeErrStr) &&
		DbSetObjectShapes(*dbconn, work.get(), verbose, this->tableStaticPrefix, this->tableTestPrefix, enabled, nativeErrStr);
	errStr.errStr = nativeErrStr;
	return ok;
}

bool PgAdmin::CreateBboxIndices(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbCreateBboxIndices(*dbconn, work.get(), verbose, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateBboxIndices(*dbconn, work.get(), verbose, this->tableModPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbCreateBboxIndices(*dbconn, work.get(), verbose, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::SplitWrapped(const std::vector<std::string> &prefixes, int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	if(!this->sharedWork->work)
		throw runtime_error("Transaction has been deleted");
	auto split = [&](pqxx::transaction_base *work) {
		for(const std::string &prefix : prefixes)
		{
			if(!DbCheckTableExists(*dbconn, work, prefix + "liverelations"))
				continue;
			if(DbSplitWrappedRelationBboxes(*dbconn, work, verbose, prefix, nativeErrStr) < 0)
			{
				errStr.errStr = nativeErrStr;
				return false;
			}
		}
		return true;
	};

	if(dynamic_cast<pqxx::nontransaction *>(this->sharedWork->work.get()) == nullptr)
		return split(this->sharedWork->work.get());

	// A transaction of its own, on a connection of its own: this one's
	// session is kept, as other things hold it, and, without a transaction,
	// holds no locks the other could wait for
	pqxx::connection own(dbconn->connection_string());
	pqxx::work work(own);
	if(!split(&work))
		return false;
	work.commit();
	return true;
}

bool PgAdmin::SplitWrappedRelationBboxes(int verbose, class PgMapError &errStr)
{
	// The base tables first: the active ones' relations may hold theirs
	return SplitWrapped({this->tableStaticPrefix, this->tableModPrefix, this->tableTestPrefix}, verbose, errStr);
}

bool PgAdmin::DropBboxIndices(int verbose, class PgMapError &errStr)
{
	std::string nativeErrStr;
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	bool ok = DbDropBboxIndices(*dbconn, work.get(), verbose, this->tableStaticPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbDropBboxIndices(*dbconn, work.get(), verbose, this->tableModPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;
	if(!ok) return ok;
	ok = DbDropBboxIndices(*dbconn, work.get(), verbose, this->tableTestPrefix, nativeErrStr);
	errStr.errStr = nativeErrStr;

	return ok;
}

bool PgAdmin::CheckNodesExistForWays(class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	DbCheckNodesExistForAllWays(*dbconn, work.get(), this->tableStaticPrefix, this->tableModPrefix,
		this->tableStaticPrefix, this->tableModPrefix);

	DbCheckNodesExistForAllWays(*dbconn, work.get(), this->tableModPrefix, "",
		this->tableStaticPrefix, this->tableModPrefix);

	return true;
}

bool PgAdmin::CheckObjectIdTables(class PgMapError &errStr)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	vector<string> objTypes = {"node", "way", "relation"};

	for(size_t i=0; i<objTypes.size(); i++)
	{
		DbCheckObjectIdTables(*dbconn, work.get(),
			this->tableModPrefix, "live", objTypes[i]);

		DbCheckObjectIdTables(*dbconn, work.get(),
			this->tableModPrefix, "old", objTypes[i]);

		DbCheckObjectIdTables(*dbconn, work.get(),
			this->tableStaticPrefix, "live", objTypes[i]);

		DbCheckObjectIdTables(*dbconn, work.get(),
			this->tableStaticPrefix, "old", objTypes[i]);
	}
	
	return true;
}

void PgAdmin::Commit()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	//Release locks
	work->commit();
}

void PgAdmin::Abort()
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");
	work->abort();
}

// **********************************************

PgMap::PgMap(const string &connection, const string &tableStaticPrefixIn, 
	const string &tableActivePrefixIn,
	const string &tableModPrefixIn,
	const string &tableTestPrefixIn)
{
	// Transactions share ownership of the connection, so tidy the prepared
	// statement record when the last owner lets go, not when PgMap does. A
	// later connection may be allocated at the same address.
	dbconn.reset(new pqxx::connection(connection), [](pqxx::connection *c) {
		forget_prepared(*c);
		delete c;
	});
	connectionString = connection;
	tableStaticPrefix = tableStaticPrefixIn;
	tableActivePrefix = tableActivePrefixIn;
	tableModPrefix = tableModPrefixIn;
	tableTestPrefix = tableTestPrefixIn;
}

PgMap::~PgMap()
{
	if(this->sharedWork)
		this->sharedWork->work.reset();
	this->sharedWork.reset();

	dbconn.reset();
}

bool PgMap::Ready()
{
	return dbconn->is_open();
}

std::shared_ptr<class PgTransaction> PgMap::GetTransaction(const std::string &shareMode)
{
	dbconn->cancel_query();
	if(this->sharedWork)
		this->sharedWork->work.reset();
	this->sharedWork.reset(new class PgWork(new pqxx::transaction<pqxx::repeatable_read>(*dbconn)));
	shared_ptr<class PgTransaction> out(new class PgTransaction(dbconn, tableStaticPrefix, tableActivePrefix, this->sharedWork, shareMode));
	return out;
}

std::shared_ptr<class PgAdmin> PgMap::GetAdmin()
{
	dbconn->cancel_query();
	if(this->sharedWork)
		this->sharedWork->work.reset();
	this->sharedWork.reset(new class PgWork(new pqxx::nontransaction(*dbconn)));
	shared_ptr<class PgAdmin> out(new class PgAdmin(dbconn, tableStaticPrefix, tableModPrefix, tableTestPrefix, this->sharedWork, ""));
	return out;
}

std::shared_ptr<class PgAdmin> PgMap::GetAdmin(const std::string &shareMode)
{
	dbconn->cancel_query();
	if(this->sharedWork)
		this->sharedWork->work.reset();
	this->sharedWork.reset(new class PgWork(new pqxx::transaction<pqxx::repeatable_read>(*dbconn)));
	shared_ptr<class PgAdmin> out(new class PgAdmin(dbconn, tableStaticPrefix, tableModPrefix, tableTestPrefix, this->sharedWork, shareMode));
	return out;
}
