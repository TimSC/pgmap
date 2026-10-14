#include <algorithm>
#include "dbextract.h"
#include <stdexcept>
#include <vector>
#include <utility>
using namespace std;

int64_t DbUpdateExtract(pqxx::connection &c, pqxx::transaction_base &w,
	const string &staticPrefix, const string &prefix, int64_t extractId, const string &name)
{
	if(extractId < 0 || (!extractId && name.empty()))
		throw invalid_argument("Select by positive extract ID or nonempty name");
	auto table = [&](const string &suffix) { return c.quote_name(prefix + suffix); };
	string condition = extractId ? "id=" + to_string(extractId) : "name=" + w.quote(name);
	auto metadata = w.exec("SELECT id, edit_activity_id, atomic_edit_id, use_bbox_in_query FROM " +
		table("extracts") + " WHERE " + condition + " ORDER BY id LIMIT 2");
	if(metadata.empty()) throw runtime_error("Extract not found");
	if(metadata.size() != 1) throw runtime_error("Extract name is ambiguous; select by ID");
	if(metadata[0][1].is_null() || metadata[0][2].is_null())
		throw runtime_error("Extract has no established synchronization checkpoint");
	extractId = metadata[0][0].as<int64_t>();
	string id = to_string(extractId);
	int64_t previous = metadata[0][1].as<int64_t>();
	int64_t previousAtomic = metadata[0][2].as<int64_t>();
	bool bboxMode = metadata[0][3].as<bool>();
	auto latest = w.exec("SELECT COALESCE(max(id),0),COALESCE(max(atomic_edit_id),0) FROM " + table("edit_activity"));
	int64_t target = latest[0][0].as<int64_t>();
	int64_t atomic = latest[0][1].as<int64_t>();
	if(target < previous || atomic < previousAtomic)
		throw runtime_error("Source activity precedes extract checkpoint; source may have been reset");
	if(target == previous) return extractId;
	string pending = "id>" + to_string(previous) + " AND id<=" + to_string(target);
	// Validate the entire pending interval before touching extract contents.
	// The checkpoint may only advance over complete atomic edits. The kind of
	// object edited does not matter: contents are recalculated from the snapshot.
	auto invalid = w.exec("SELECT id FROM " + table("edit_activity") + " WHERE " + pending +
		" AND (atomic_edit_id IS NULL OR block_index IS NULL OR atomic_edit_id<=" +
		to_string(previousAtomic) + ") LIMIT 1");
	if(!invalid.empty()) throw runtime_error("Ungrouped activity at row " + invalid[0][0].as<string>() +
		"; extract update aborted");
	auto broken = w.exec("SELECT atomic_edit_id FROM " + table("edit_activity") + " WHERE " + pending +
		" GROUP BY atomic_edit_id HAVING min(block_index)<>0 OR max(block_index)+1<>count(*) LIMIT 1");
	if(!broken.empty()) throw runtime_error("Incomplete atomic edit in pending activity");

	// All membership calculations below use current visible objects in the SAME
	// snapshot as the target checkpoint. Historical parent lookup is unnecessary
	// because the extract is updated only to that snapshot, never an intermediate
	// revision. The selection rules mirror PgMapQuery::Continue. SQL temp tables
	// bound client memory; work scales with the extract, like a fresh map query.
	string base = "pgmap_update_extract_" + id;
	string seeds = c.quote_name(base + "_seeds");
	string ways = c.quote_name(base + "_ways");
	string nodes = c.quote_name(base + "_nodes");
	string relations = c.quote_name(base + "_relations");
	string bbox = "(SELECT bbox FROM " + table("extracts") + " WHERE id=" + id + ")";

	// Static baseline then active overlay. Looking up each layer's tables by ID
	// uses their indexes; the visible* union views cannot be probed that way.
	// Static objects overridden or deleted in the active layer are suppressed.
	vector<pair<string,string> > layers;
	if(!staticPrefix.empty() && staticPrefix != prefix) layers.push_back({staticPrefix, prefix});
	layers.push_back({prefix, ""});
	auto suppressed = [&](const string &exclude, const string &kind, const string &alias) {
		if(exclude.empty()) return string("true");
		return "NOT EXISTS (SELECT 1 FROM " + c.quote_name(exclude + kind + "ids") +
			" x WHERE x.id=" + alias + ".id)";
	};
	// Current owners (way or relation) in each layer with a member in selected.
	auto owners = [&](const string &kind, const string &mems, const string &selected) {
		string sql;
		for(const auto &layer : layers)
		{
			if(!sql.empty()) sql += " UNION ";
			sql += "SELECT o.id FROM " + c.quote_name(layer.first + mems) + " m JOIN " + selected +
				" s ON m.member=s.id JOIN " + c.quote_name(layer.first + "live" + kind + "s") +
				" o ON m.id=o.id AND m.version=o.version WHERE " + suppressed(layer.second, kind, "o");
		}
		return sql;
	};
	// Visible rows of one object type restricted to the selected IDs.
	auto visible = [&](const string &kind, const string &columns, const string &selected) {
		string sql;
		for(const auto &layer : layers)
		{
			if(!sql.empty()) sql += " UNION ALL ";
			sql += "SELECT " + columns + " FROM " + c.quote_name(layer.first + "live" + kind + "s") +
				" o JOIN " + selected + " s ON o.id=s.id WHERE " + suppressed(layer.second, kind, "o");
		}
		return sql;
	};

	// The boundary may be any polygon: what is in it, not just in the
	// rectangle around it. For a rectangle the two are the same.
	w.exec("CREATE TEMP TABLE " + seeds + " ON COMMIT DROP AS SELECT id FROM " + table("visiblenodes") + " WHERE ST_Intersects(geom, " + bbox + ")");
	w.exec("CREATE UNIQUE INDEX ON " + seeds + " (id)");
	w.exec("CREATE TEMP TABLE " + ways + " ON COMMIT DROP AS " + (bboxMode ?
		"SELECT v.id FROM " + table("visibleways") + " v WHERE ST_Intersects(v.bbox, " + bbox + ")" :
		owners("way", "way_mems", seeds)));
	w.exec("CREATE UNIQUE INDEX ON " + ways + " (id)");
	w.exec("CREATE TEMP TABLE " + nodes + " ON COMMIT DROP AS SELECT id FROM " + seeds +
		" UNION SELECT member::text::bigint FROM (" + visible("way", "o.members", ways) +
		") v CROSS JOIN LATERAL jsonb_array_elements(v.members) member");
	w.exec("CREATE UNIQUE INDEX ON " + nodes + " (id)");
	w.exec("CREATE TEMP TABLE " + relations + " ON COMMIT DROP AS " + (bboxMode ?
		"SELECT v.id FROM " + table("visiblerelations") + " v WHERE ST_Intersects(v.bbox, " + bbox + ")" :
		owners("relation", "relation_mems_n", nodes) + " UNION " +
		owners("relation", "relation_mems_w", ways)));
	w.exec("CREATE UNIQUE INDEX ON " + relations + " (id)");

	// Replace only this extract's current contents. This also removes obsolete
	// parents/completion nodes while retaining shared dependencies selected above.
	// No global deletion is inferred from a local membership removal.
	for(const char *suffix : {"extract_liverelations","extract_liveways","extract_livenodes"})
		w.exec("DELETE FROM " + table(suffix) + " WHERE extract_id=" + id);
	for(const char *type : {"node","way","relation"})
	{
		string kind(type);
		string selected = kind=="node" ? nodes : kind=="way" ? ways : relations;
		string columns = "id,changeset,changeset_index,username,uid,timestamp,version,tags";
		if(kind=="node") columns+=",geom";
		else if(kind=="way") columns+=",members,bbox";
		else columns+=",members,memberroles,bbox";
		// Completion nodes missing from the map are omitted, as in a map query.
		string qualified = "o." + columns;
		for(size_t pos = qualified.find(','); pos != string::npos; pos = qualified.find(',', pos+3))
			qualified.replace(pos, 1, ",o.");
		w.exec("INSERT INTO " + table("extract_live"+kind+"s") + " (extract_id," + columns + ") SELECT " +
			id + ",v.* FROM (" + visible(kind, qualified, selected) + ") v");
	}
	w.exec("INSERT INTO " + table("extract_way_mems") + " (extract_id,id,version,index,member) SELECT " +
		id + ",v.id,v.version,(member.ordinality-1)::integer,member.value::text::bigint FROM " +
		table("extract_liveways") + " v CROSS JOIN LATERAL jsonb_array_elements(v.members) WITH ORDINALITY member WHERE v.extract_id=" + id);
	for(const string &kind : {string("node"),string("way"),string("relation")})
		w.exec("INSERT INTO " + table("extract_relation_mems_"+kind.substr(0,1)) +
			" (extract_id,id,version,index,member) SELECT " + id +
			",v.id,v.version,(member.ordinality-1)::integer,(member.value->>1)::bigint FROM " +
			table("extract_liverelations") + " v CROSS JOIN LATERAL jsonb_array_elements(v.members) WITH ORDINALITY member "
			"WHERE v.extract_id=" + id + " AND member.value->>0=" + w.quote(kind));
	w.exec("UPDATE " + table("extracts") + " SET edit_activity_id=" + to_string(target) +
		",atomic_edit_id=" + to_string(atomic) + ",performed_at=CURRENT_TIMESTAMP WHERE id=" + id);
	for(const string &temporary : {relations,nodes,ways,seeds}) w.exec("DROP TABLE " + temporary);
	return extractId;
}

#include "pgcommon.h"
#include "dbdecode.h"

PgExtractExport::PgExtractExport(shared_ptr<pqxx::connection> connectionIn,
    shared_ptr<PgWork> workIn, const string &prefixIn, int64_t id,
    const string &name, shared_ptr<IDataStreamHandler> outputIn):
    connection(connectionIn), work(workIn), output(outputIn), prefix(prefixIn)
{
    if(!work || !work->work) throw runtime_error("Transaction has been deleted");
    auto &w = *work->work;
    string predicate = id ? "id=" + to_string(id) : "name=" + w.quote(name);
    auto rows = w.exec("SELECT id, ST_XMin(bbox), ST_YMin(bbox), ST_XMax(bbox), ST_YMax(bbox), "
        "use_bbox_in_query, edit_activity_id FROM " +
        connection->quote_name(prefix + "extracts") + " WHERE " + predicate + " ORDER BY id LIMIT 2");
    if(rows.empty()) throw runtime_error("Extract not found");
    if(rows.size() > 1) throw runtime_error("Extract name is ambiguous; select by ID");
    extractId = rows[0][0].as<int64_t>();
    storedBboxMode = rows[0][5].as<bool>();
    auto mode = w.exec("SELECT value FROM " + connection->quote_name(prefix + "meta") +
        " WHERE key='useBboxInQuery'");
    mapBboxMode = !mode.empty() && atoi(mode[0][0].as<string>().c_str()) == 1;
    auto latest = w.exec("SELECT COALESCE(max(id),0) FROM " +
        connection->quote_name(prefix + "edit_activity"));
    pendingActivity = rows[0][6].is_null() || latest[0][0].as<int64_t>() != rows[0][6].as<int64_t>();
    output->StoreIsDiff(false);
    output->StoreBounds(Bounds(rows[0][1].as<double>(), rows[0][2].as<double>(),
        rows[0][3].as<double>(), rows[0][4].as<double>()));
}

int PgExtractExport::Continue()
{
    if(phase == 3) return 1;
    if(!work || !work->work) throw runtime_error("Transaction has been deleted");
    auto &w = *work->work;
    const char *kinds[] = {"node", "way", "relation"};
    string kind = kinds[phase];
    string table = connection->quote_name(prefix + "extract_live" + kind + "s");
    string condition = "extract_id=" + to_string(extractId) + " AND id>" + to_string(lastId);
    auto ids = w.exec("SELECT id FROM " + table + " WHERE " + condition + " ORDER BY id LIMIT 1000");
    if(ids.empty())
    {
        output->Reset();
        lastId = 0;
        if(++phase == 3) { output->Finish(); return 1; }
        return 0;
    }
    int64_t upper = ids[ids.size()-1][0].as<int64_t>();
    string columns = kind == "node" ? "*, ST_X(geom) AS lon, ST_Y(geom) AS lat" : "*";
    pqxx::icursorstream cursor(w, "SELECT " + columns + " FROM " + table + " WHERE " +
        condition + " AND id<=" + to_string(upper) + " ORDER BY id", "export_extract_batch", 1000);
    // Empty prefixes preserve usernames recorded in the stored snapshot.
    DbUsernameLookup usernames(*connection, &w, "", "");
    if(kind == "node") NodeResultsToEncoder(cursor, usernames, output);
    else if(kind == "way") WayResultsToEncoder(cursor, usernames, output);
    else RelationResultsToEncoder(cursor, usernames, set<int64_t>{}, output);
    lastId = upper;
    return 0;
}

#include "dbjson.h"
#include <cmath>
#include <cstdio>

// Objects per INSERT statement, and the number of membership rows that
// forces an early write when ways or relations have many members.
static const size_t IMPORT_BATCH_OBJECTS = 1000;
static const size_t IMPORT_BATCH_MEMBERS = 20000;

// Pending rows, as SQL value lists, for the tables of one extract.
struct ExtractRows
{
    // 0 nodes, 1 ways, 2 relations
    std::array<std::vector<std::string>, 3> objects;
    // 0 way_mems, then relation_mems_n, _w and _r
    std::array<std::vector<std::string>, 4> members;

    size_t NumObjects() const { return objects[0].size() + objects[1].size() + objects[2].size(); }
    size_t NumMembers() const
    {
        return members[0].size() + members[1].size() + members[2].size() + members[3].size();
    }
};

static void CheckExtractBbox(const vector<double> &bbox)
{
    if(bbox.size() != 4) throw invalid_argument("Bbox must have four coordinates");
    for(double value : bbox)
        if(!std::isfinite(value)) throw invalid_argument("Bbox coordinates must be finite");
    if(bbox[0] < -180 || bbox[2] > 180 || bbox[1] < -90 || bbox[3] > 90 ||
        bbox[0] >= bbox[2] || bbox[1] >= bbox[3])
        throw invalid_argument("Bbox must be a nonempty longitude/latitude rectangle");
}

static string ExactDouble(double value)
{
    char buffer[40];
    snprintf(buffer, sizeof(buffer), "%.17g", value);
    return buffer;
}

static bool ParseEditId(const string &text, int64_t &out)
{
    if(text.empty() || text.size() > 18 || text.find_first_not_of("0123456789") != string::npos)
        return false;
    out = stoll(text);
    return true;
}

PgExtractImport::PgExtractImport(shared_ptr<pqxx::connection> connectionIn,
    shared_ptr<PgWork> workIn, const string &prefixIn, const string &name,
    const vector<double> &bboxIn, int64_t editActivityIdIn, int64_t atomicEditIdIn):
    connection(connectionIn), work(workIn), prefix(prefixIn), rows(new ExtractRows())
{
    if(!bboxIn.empty())
    {
        CheckExtractBbox(bboxIn);
        bbox = bboxIn;
        haveBbox = true;
    }
    if((editActivityIdIn < 0) != (atomicEditIdIn < 0))
        throw invalid_argument("Give both the edit activity ID and the atomic edit ID, or neither");
    if(editActivityIdIn >= 0)
    {
        editActivityId = editActivityIdIn;
        atomicEditId = atomicEditIdIn;
        haveCheckpoint = true;
    }
    auto &w = Work();
    auto mode = w.exec("SELECT value FROM " + connection->quote_name(prefix + "meta") +
        " WHERE key='useBboxInQuery'");
    bool bboxMode = !mode.empty() && atoi(mode[0][0].as<string>().c_str()) == 1;
    // Objects refer to the extract's row, so it has to exist before the stream
    // says what the rectangle is. Finish replaces this empty placeholder.
    extractId = w.exec("INSERT INTO " + connection->quote_name(prefix + "extracts") +
        " (name, bbox, use_bbox_in_query, performed_at) VALUES (" +
        (name.empty() ? string("NULL") : w.quote(name)) + ",ST_MakeEnvelope(0,0,0,0,4326)," +
        (bboxMode ? "true" : "false") + ",CURRENT_TIMESTAMP) RETURNING id")[0][0].as<int64_t>();
}

pqxx::transaction_base &PgExtractImport::Work()
{
    if(!work || !work->work) throw runtime_error("Transaction has been deleted");
    return *work->work;
}

void PgExtractImport::StoreIsDiff(bool isDiff)
{
    if(isDiff) throw runtime_error("A diff cannot be imported as an extract");
}

void PgExtractImport::StoreAttributes(const TagMap &attributes)
{
    auto activity = attributes.find("edit_activity_id");
    auto atomic = attributes.find("atomic_edit_id");
    if(activity != attributes.end()) fileEditActivityId = activity->second;
    if(atomic != attributes.end()) fileAtomicEditId = atomic->second;
    // An extract whose boundary is not a rectangle is written with it, as WKT,
    // which a boundary from the caller replaces
    auto boundary = attributes.find("boundary");
    if(boundary != attributes.end() && !haveBbox)
        SetBoundaryWkt(boundary->second);
}

void PgExtractImport::StoreBounds(const Bounds &bounds)
{
    if(haveBbox) return; // A bbox from the caller, or the first in the stream, wins
    bbox = {bounds.minLon, bounds.minLat, bounds.maxLon, bounds.maxLat};
    CheckExtractBbox(bbox);
    haveBbox = true;
}

// ****** Rows of the extract tables, shared by importing and patching ******

// Columns: extract_id, id, changeset, username, uid, timestamp, version, tags.
// Metadata a file leaves out is stored as NULL, which reads back as absent.
static string ExtractCommonValues(pqxx::transaction_base &w, int64_t extractId, const OsmObject &object)
{
    const MetaData &meta = object.metaData;
    string tags;
    EncodeTags(object.tags, tags);
    return to_string(extractId) + "," + to_string(object.objId) + "," +
        (meta.changeset ? to_string(meta.changeset) : string("NULL")) + "," +
        (meta.username.empty() ? string("NULL") : w.quote(meta.username)) + "," +
        (meta.uid ? to_string(meta.uid) : string("NULL")) + "," +
        (meta.timestamp ? to_string(meta.timestamp) : string("NULL")) + "," +
        to_string(meta.version) + "," + w.quote(tags) + "::jsonb";
}

static void AddExtractNode(pqxx::transaction_base &w, int64_t extractId, const OsmNode &node, ExtractRows &rows)
{
    if(!std::isfinite(node.lon) || !std::isfinite(node.lat))
        throw runtime_error("Node " + to_string(node.objId) + " has no usable position");
    rows.objects[0].push_back("(" + ExtractCommonValues(w, extractId, node) + ",ST_SetSRID(ST_MakePoint(" +
        ExactDouble(node.lon) + "," + ExactDouble(node.lat) + "),4326))");
}

static void AddExtractWay(pqxx::transaction_base &w, int64_t extractId, const OsmWay &way, ExtractRows &rows)
{
    string members;
    EncodeInt64Vec(way.refs, members);
    rows.objects[1].push_back("(" + ExtractCommonValues(w, extractId, way) + "," + w.quote(members) + "::jsonb)");
    string owner = "(" + to_string(extractId) + "," + to_string(way.objId) + "," +
        to_string(way.metaData.version) + ",";
    for(size_t i = 0; i < way.refs.size(); i++)
        rows.members[0].push_back(owner + to_string(i) + "," + to_string(way.refs[i]) + ")");
}

static void AddExtractRelation(pqxx::transaction_base &w, int64_t extractId, const OsmRelation &relation,
    ExtractRows &rows)
{
    vector<string> types, roles;
    vector<int64_t> refs;
    string owner = "(" + to_string(extractId) + "," + to_string(relation.objId) + "," +
        to_string(relation.metaData.version) + ",";
    for(size_t i = 0; i < relation.members.size(); i++)
    {
        const RelationMember &member = relation.members[i];
        types.push_back(ObjectTypeName(member.type));
        refs.push_back(member.ref);
        roles.push_back(member.role);
        // Tables follow the order node, way, relation
        size_t table = member.type == ObjectType::Node ? 1 : (member.type == ObjectType::Way ? 2 : 3);
        rows.members[table].push_back(owner + to_string(i) + "," + to_string(member.ref) + ")");
    }
    string members, memberRoles;
    EncodeRelationMems(types, refs, members);
    EncodeStringVec(roles, memberRoles);
    rows.objects[2].push_back("(" + ExtractCommonValues(w, extractId, relation) + "," + w.quote(members) + "::jsonb," +
        w.quote(memberRoles) + "::jsonb)");
}

// Writes the pending rows and empties them.
static void InsertExtractRows(pqxx::connection &connection, pqxx::transaction_base &w,
    const string &prefix, ExtractRows &rows)
{
    auto insert = [&](const string &table, const string &columns, vector<string> &pending)
    {
        // Bound each statement, however many rows are waiting
        for(size_t start = 0; start < pending.size(); start += 5000)
        {
            string sql = "INSERT INTO " + connection.quote_name(prefix + table) + " (" + columns + ") VALUES ";
            for(size_t i = start; i < pending.size() && i < start + 5000; i++)
            {
                if(i > start) sql += ",";
                sql += pending[i];
            }
            w.exec(sql);
        }
        pending.clear();
    };
    // An object appearing twice breaks the primary key and fails the write,
    // because there would be no telling which copy the extract should hold.
    const string common = "extract_id,id,changeset,username,uid,timestamp,version,tags";
    insert("extract_livenodes", common + ",geom", rows.objects[0]);
    insert("extract_liveways", common + ",members", rows.objects[1]);
    insert("extract_liverelations", common + ",members,memberroles", rows.objects[2]);
    // Membership rows refer to their way or relation, so they follow it
    const char *memberTables[] = {"extract_way_mems", "extract_relation_mems_n",
        "extract_relation_mems_w", "extract_relation_mems_r"};
    for(size_t i = 0; i < 4; i++)
        insert(memberTables[i], "extract_id,id,version,index,member", rows.members[i]);
}

// Gives ways the bbox of whichever of their nodes the extract holds, as the
// map's own ways have. Relation bboxes are left unset: working one out needs
// members the extract may not hold, and nothing reads it from here. condition,
// if not empty, limits this to some ways, written against the alias w.
// Naming the extract in each lookup, rather than joining on it, keeps every
// step on a primary key however stale the planner's statistics are after a
// bulk insert.
static void UpdateExtractWayBboxes(pqxx::connection &connection, pqxx::transaction_base &w,
    const string &prefix, int64_t extractId, const string &condition = string())
{
    string id = to_string(extractId);
    w.exec("UPDATE " + connection.quote_name(prefix + "extract_liveways") + " w SET bbox=("
        "SELECT ST_Envelope(ST_Collect(n.geom)) FROM " +
        connection.quote_name(prefix + "extract_way_mems") + " m JOIN " +
        connection.quote_name(prefix + "extract_livenodes") + " n ON n.extract_id=" + id + " AND n.id=m.member "
        "WHERE m.extract_id=" + id + " AND m.id=w.id) WHERE w.extract_id=" + id +
        (condition.empty() ? string() : " AND (" + condition + ")"));
}

void PgExtractImport::StoreNode(const OsmNode &node)
{
    if(finished) throw runtime_error("Extract import has already finished");
    if(!node.metaData.visible) return; // An extract holds current objects only
    AddExtractNode(Work(), extractId, node, *rows);
    numNodes++;
    FlushIfLarge();
}

void PgExtractImport::StoreWay(const OsmWay &way)
{
    if(finished) throw runtime_error("Extract import has already finished");
    if(!way.metaData.visible) return;
    AddExtractWay(Work(), extractId, way, *rows);
    numWays++;
    FlushIfLarge();
}

void PgExtractImport::StoreRelation(const OsmRelation &relation)
{
    if(finished) throw runtime_error("Extract import has already finished");
    if(!relation.metaData.visible) return;
    AddExtractRelation(Work(), extractId, relation, *rows);
    numRelations++;
    FlushIfLarge();
}

void PgExtractImport::FlushIfLarge()
{
    if(rows->NumObjects() >= IMPORT_BATCH_OBJECTS || rows->NumMembers() >= IMPORT_BATCH_MEMBERS) Flush();
}

void PgExtractImport::Flush()
{
    InsertExtractRows(*connection, Work(), prefix, *rows);
}

void PgExtractImport::SetBoundaryWkt(const string &wkt)
{
    boundaryWkt = DbCheckExtractBoundary(Work(), wkt);
    haveBbox = true;
}

void PgExtractImport::Finish()
{
    if(finished) return;
    Flush();
    auto &w = Work();
    if(!haveBbox)
        throw runtime_error("The file does not say what area it covers; give a bbox");
    if(!haveCheckpoint && (!fileEditActivityId.empty() || !fileAtomicEditId.empty()))
    {
        if(!ParseEditId(fileEditActivityId, editActivityId) || !ParseEditId(fileAtomicEditId, atomicEditId))
            throw runtime_error("The file's edit_activity_id and atomic_edit_id are not a usable checkpoint");
        haveCheckpoint = true;
    }
    string id = to_string(extractId);
    string sql = "UPDATE " + connection->quote_name(prefix + "extracts") + " SET bbox=";
    if(!boundaryWkt.empty())
        sql += "ST_GeomFromText(" + w.quote(boundaryWkt) + ", 4326)";
    else
    {
        sql += "ST_MakeEnvelope(";
        for(double value : bbox) sql += ExactDouble(value) + ",";
        sql += "4326)";
    }
    if(haveCheckpoint)
        sql += ",edit_activity_id=" + to_string(editActivityId) + ",atomic_edit_id=" + to_string(atomicEditId);
    w.exec(sql + " WHERE id=" + id);
    UpdateExtractWayBboxes(*connection, w, prefix, extractId);
    finished = true;
}

// ****** Patching an extract from published edits ******

// Objects waiting to be applied, the highest version of each. One that is not
// visible stands for a deletion.
struct ExtractPending
{
    std::map<int64_t, OsmNode> nodes;
    std::map<int64_t, OsmWay> ways;
    std::map<int64_t, OsmRelation> relations;
};

template<class T> static void KeepHighestVersion(std::map<int64_t, T> &pending, const T &object)
{
    auto it = pending.find(object.objId);
    if(it == pending.end() || it->second.metaData.version <= object.metaData.version)
        pending[object.objId] = object;
}

static string IdArraySql(const std::vector<int64_t> &ids)
{
    string sql = "ARRAY[";
    for(size_t i = 0; i < ids.size(); i++)
        sql += (i ? "," : "") + to_string(ids[i]);
    return sql + "]::bigint[]";
}

static int ExtractTypeIndex(const string &type)
{
    if(type == "node") return 0;
    if(type == "way") return 1;
    if(type == "relation") return 2;
    throw invalid_argument("Object type must be node, way or relation");
}

PgExtractPatch::PgExtractPatch(shared_ptr<pqxx::connection> connectionIn,
    shared_ptr<PgWork> workIn, const string &prefixIn, int64_t extractIdIn):
    connection(connectionIn), work(workIn), prefix(prefixIn), extractId(extractIdIn),
    pending(new ExtractPending())
{
    auto rows = Work().exec("SELECT id FROM " + connection->quote_name(prefix + "extracts") +
        " WHERE id=" + to_string(extractId));
    if(rows.empty()) throw runtime_error("Extract not found");
}

pqxx::transaction_base &PgExtractPatch::Work()
{
    if(!work || !work->work) throw runtime_error("Transaction has been deleted");
    return *work->work;
}

void PgExtractPatch::StoreNode(const OsmNode &node) { KeepHighestVersion(pending->nodes, node); }
void PgExtractPatch::StoreWay(const OsmWay &way) { KeepHighestVersion(pending->ways, way); }
void PgExtractPatch::StoreRelation(const OsmRelation &relation) { KeepHighestVersion(pending->relations, relation); }

void PgExtractPatch::RemoveObject(const string &type, int64_t objId, int64_t version)
{
    int index = ExtractTypeIndex(type);
    if(index == 0)
    {
        OsmNode gone; gone.objId = objId; gone.metaData.version = version; gone.metaData.visible = false;
        KeepHighestVersion(pending->nodes, gone);
    }
    else if(index == 1)
    {
        OsmWay gone; gone.objId = objId; gone.metaData.version = version; gone.metaData.visible = false;
        KeepHighestVersion(pending->ways, gone);
    }
    else
    {
        OsmRelation gone; gone.objId = objId; gone.metaData.version = version; gone.metaData.visible = false;
        KeepHighestVersion(pending->relations, gone);
    }
}

std::vector<int64_t> PgExtractPatch::GetArrivedIds(const string &type) const
{
    return arrived[ExtractTypeIndex(type)];
}

void PgExtractPatch::Apply()
{
    auto &w = Work();
    string id = to_string(extractId);
    auto table = [&](const string &suffix) { return connection->quote_name(prefix + suffix); };
    const char *live[] = {"extract_livenodes", "extract_liveways", "extract_liverelations"};
    ExtractRows rows;
    std::vector<int64_t> replaced[3], candidates[3];

    // An object sent changes the extract if the extract does not hold it, or
    // holds it at the same or a lower version.
    auto storedVersions = [&](int index, const std::vector<int64_t> &ids)
    {
        std::map<int64_t, int64_t> stored;
        if(!ids.empty())
            for(const auto &row : w.exec("SELECT id, version FROM " + table(live[index]) +
                " WHERE extract_id=" + id + " AND id = ANY(" + IdArraySql(ids) + ")"))
                stored[row[0].as<int64_t>()] = row[1].as<int64_t>();
        return stored;
    };

    {
        std::vector<int64_t> ids;
        for(const auto &entry : pending->nodes) ids.push_back(entry.first);
        auto stored = storedVersions(0, ids);
        for(const auto &entry : pending->nodes)
        {
            auto held = stored.find(entry.first);
            if(held != stored.end() && held->second > entry.second.metaData.version) continue;
            if(held != stored.end()) replaced[0].push_back(entry.first);
            if(!entry.second.metaData.visible) continue;
            AddExtractNode(w, extractId, entry.second, rows);
            if(held == stored.end()) candidates[0].push_back(entry.first);
        }
    }
    {
        std::vector<int64_t> ids;
        for(const auto &entry : pending->ways) ids.push_back(entry.first);
        auto stored = storedVersions(1, ids);
        for(const auto &entry : pending->ways)
        {
            auto held = stored.find(entry.first);
            if(held != stored.end() && held->second > entry.second.metaData.version) continue;
            if(held != stored.end()) replaced[1].push_back(entry.first);
            if(!entry.second.metaData.visible) continue;
            AddExtractWay(w, extractId, entry.second, rows);
            if(held == stored.end()) candidates[1].push_back(entry.first);
        }
    }
    {
        std::vector<int64_t> ids;
        for(const auto &entry : pending->relations) ids.push_back(entry.first);
        auto stored = storedVersions(2, ids);
        for(const auto &entry : pending->relations)
        {
            auto held = stored.find(entry.first);
            if(held != stored.end() && held->second > entry.second.metaData.version) continue;
            if(held != stored.end()) replaced[2].push_back(entry.first);
            if(!entry.second.metaData.visible) continue;
            AddExtractRelation(w, extractId, entry.second, rows);
            if(held == stored.end()) candidates[2].push_back(entry.first);
        }
    }

    // Everything below looks only at what this patch touched and what is
    // joined to it, so that the work depends on the size of the patch and
    // not on the size of the extract.
    auto idsFrom = [&](const string &sql)
    {
        std::vector<int64_t> out;
        for(const auto &row : w.exec(sql))
            out.push_back(row[0].as<int64_t>());
        return out;
    };
    auto joined = [](std::vector<int64_t> first, const std::vector<int64_t> &second)
    {
        first.insert(first.end(), second.begin(), second.end());
        std::sort(first.begin(), first.end());
        first.erase(std::unique(first.begin(), first.end()), first.end());
        return first;
    };
    string own = "extract_id=" + id;
    std::vector<int64_t> touchedNodes = joined(replaced[0], candidates[0]);
    std::vector<int64_t> touchedWays = joined(replaced[1], candidates[1]);
    std::vector<int64_t> touchedRelations = joined(replaced[2], candidates[2]);

    // The nodes of the ways about to be replaced: once a way has other nodes,
    // the ones it had may have no reason left to be in the extract
    std::vector<int64_t> formerWayNodes;
    if(!replaced[1].empty())
        formerWayNodes = idsFrom("SELECT DISTINCT member FROM " + table("extract_way_mems") + " WHERE " + own +
            " AND id = ANY(" + IdArraySql(replaced[1]) + ")");

    // Membership rows go with their way or relation
    for(int index = 2; index >= 0; index--)
        if(!replaced[index].empty())
            w.exec("DELETE FROM " + table(live[index]) + " WHERE extract_id=" + id +
                " AND id = ANY(" + IdArraySql(replaced[index]) + ")");
    InsertExtractRows(*connection, w, prefix, rows);
    pending.reset(new ExtractPending());

    // Keep only what the extract's rule selects, in the order the rule builds
    // on itself: ways with a node in the rectangle, then nodes in it or used
    // by a way that stayed, then relations with a node or way that stayed.
    string bbox = "(SELECT bbox FROM " + table("extracts") + " WHERE id=" + id + ")";

    // Ways: those touched, and those holding a touched node, which may have
    // moved out of the rectangle or gone
    std::vector<int64_t> examinedWays = touchedWays;
    if(!touchedNodes.empty())
        examinedWays = joined(examinedWays, idsFrom("SELECT DISTINCT id FROM " + table("extract_way_mems") +
            " WHERE " + own + " AND member = ANY(" + IdArraySql(touchedNodes) + ")"));
    std::vector<int64_t> droppedWays, droppedWayNodes;
    if(!examinedWays.empty())
    {
        droppedWays = idsFrom("SELECT w.id FROM " + table("extract_liveways") + " w WHERE w." + own +
            " AND w.id = ANY(" + IdArraySql(examinedWays) + ")"
            " AND NOT EXISTS (SELECT 1 FROM " + table("extract_way_mems") + " m JOIN " +
            table("extract_livenodes") + " n ON n.extract_id=" + id + " AND n.id=m.member"
            " WHERE m.extract_id=" + id + " AND m.id=w.id AND ST_Intersects(n.geom, " + bbox + "))");
        if(!droppedWays.empty())
        {
            droppedWayNodes = idsFrom("SELECT DISTINCT member FROM " + table("extract_way_mems") + " WHERE " + own +
                " AND id = ANY(" + IdArraySql(droppedWays) + ")");
            w.exec("DELETE FROM " + table("extract_liveways") + " WHERE " + own +
                " AND id = ANY(" + IdArraySql(droppedWays) + ")");
        }
    }

    // Nodes: those touched, and those of ways that were replaced or dropped
    std::vector<int64_t> examinedNodes = joined(joined(touchedNodes, formerWayNodes), droppedWayNodes);
    std::vector<int64_t> droppedNodes;
    if(!examinedNodes.empty())
        droppedNodes = idsFrom("DELETE FROM " + table("extract_livenodes") + " n WHERE n." + own +
            " AND n.id = ANY(" + IdArraySql(examinedNodes) + ")"
            " AND NOT ST_Intersects(n.geom, " + bbox + ") AND NOT EXISTS (SELECT 1 FROM " +
            table("extract_way_mems") + " m WHERE m.extract_id=" + id + " AND m.member=n.id) RETURNING n.id");

    // Relations: those touched, and those with a member node or way that was
    // touched or dropped
    std::vector<int64_t> examinedRelations = touchedRelations;
    std::vector<int64_t> memberNodes = joined(touchedNodes, droppedNodes), memberWays = joined(touchedWays, droppedWays);
    if(!memberNodes.empty())
        examinedRelations = joined(examinedRelations, idsFrom("SELECT DISTINCT id FROM " +
            table("extract_relation_mems_n") + " WHERE " + own + " AND member = ANY(" + IdArraySql(memberNodes) + ")"));
    if(!memberWays.empty())
        examinedRelations = joined(examinedRelations, idsFrom("SELECT DISTINCT id FROM " +
            table("extract_relation_mems_w") + " WHERE " + own + " AND member = ANY(" + IdArraySql(memberWays) + ")"));
    if(!examinedRelations.empty())
        w.exec("DELETE FROM " + table("extract_liverelations") + " r WHERE r.extract_id=" + id +
            " AND r.id = ANY(" + IdArraySql(examinedRelations) + ")"
            " AND NOT EXISTS (SELECT 1 FROM " + table("extract_relation_mems_n") + " m JOIN " +
            table("extract_livenodes") + " n ON n.extract_id=" + id + " AND n.id=m.member"
            " WHERE m.extract_id=" + id + " AND m.id=r.id)"
            " AND NOT EXISTS (SELECT 1 FROM " + table("extract_relation_mems_w") + " m JOIN " +
            table("extract_liveways") + " y ON y.extract_id=" + id + " AND y.id=m.member"
            " WHERE m.extract_id=" + id + " AND m.id=r.id)");

    // The examined ways are the ones whose boxes can have changed
    if(!examinedWays.empty())
        UpdateExtractWayBboxes(*connection, w, prefix, extractId,
            "w.id = ANY(" + IdArraySql(examinedWays) + ")");

    // What the caller may need to follow up
    for(int index = 0; index < 3; index++)
    {
        arrived[index].clear();
        if(candidates[index].empty()) continue;
        for(const auto &row : w.exec("SELECT id FROM " + table(live[index]) + " WHERE extract_id=" + id +
            " AND id = ANY(" + IdArraySql(candidates[index]) + ") ORDER BY id"))
            arrived[index].push_back(row[0].as<int64_t>());
    }
    // Nodes that a way of this patch needs and the extract has not been given
    missingNodes.clear();
    if(!examinedWays.empty())
        missingNodes = idsFrom("SELECT DISTINCT m.member FROM " + table("extract_way_mems") +
            " m WHERE m.extract_id=" + id + " AND m.id = ANY(" + IdArraySql(examinedWays) + ")"
            " AND NOT EXISTS (SELECT 1 FROM " + table("extract_livenodes") +
            " n WHERE n.extract_id=" + id + " AND n.id=m.member) ORDER BY m.member");
}

void PgExtractPatch::SetCheckpoint(int64_t editActivityId, int64_t atomicEditId)
{
    if(editActivityId < 0 || atomicEditId < 0)
        throw invalid_argument("Edit IDs must not be negative");
    // An extract kept up this way follows the rule described above, whatever
    // the mode of the map it was first saved or imported under.
    Work().exec("UPDATE " + connection->quote_name(prefix + "extracts") + " SET edit_activity_id=" +
        to_string(editActivityId) + ",atomic_edit_id=" + to_string(atomicEditId) +
        ",performed_at=CURRENT_TIMESTAMP,use_bbox_in_query=false WHERE id=" + to_string(extractId));
}

#include "pgmap.h"
#include <map>
#include <ctime>

// Retains only type, ID and version of each object, plus the stream's bounds.
class ExtractVersionCollector : public IDataStreamHandler
{
public:
	// Keyed by (type index, ID) with type order node, way, relation.
	map<pair<int, int64_t>, int64_t> versions;
	vector<double> bounds;

	void StoreBounds(const Bounds &b) override
	{
		bounds = {b.minLon, b.minLat, b.maxLon, b.maxLat};
	}
	void StoreNode(const OsmNode &node) override
	{
		versions[{0, node.objId}] = node.metaData.version;
	}
	void StoreWay(const OsmWay &way) override
	{
		versions[{1, way.objId}] = way.metaData.version;
	}
	void StoreRelation(const OsmRelation &relation) override
	{
		versions[{2, relation.objId}] = relation.metaData.version;
	}
};

int64_t DbDeleteExtract(pqxx::connection &c, pqxx::transaction_base &w,
	const string &prefix, int64_t extractId, const string &name)
{
	if(extractId < 0 || (!extractId && name.empty()))
		throw invalid_argument("Select by positive extract ID or nonempty name");
	string extracts = c.quote_name(prefix + "extracts");
	string condition = extractId ? "id=" + to_string(extractId) : "name=" + w.quote(name);
	auto rows = w.exec("SELECT id FROM " + extracts + " WHERE " + condition + " ORDER BY id LIMIT 2");
	if(rows.empty()) throw runtime_error("Extract not found");
	if(rows.size() != 1) throw runtime_error("Extract name is ambiguous; select by ID");
	extractId = rows[0][0].as<int64_t>();
	// Objects and membership rows are removed by the ON DELETE CASCADE constraints.
	w.exec("DELETE FROM " + extracts + " WHERE id=" + to_string(extractId));
	return extractId;
}

int64_t DbSetExtractAutoUpdate(pqxx::connection &c, pqxx::transaction_base &w,
	const string &prefix, int64_t extractId, const string &name,
	bool autoUpdate, const string &updateUrl)
{
	if(extractId < 0 || (!extractId && name.empty()))
		throw invalid_argument("Select by positive extract ID or nonempty name");
	if(!updateUrl.empty())
	{
		if(updateUrl.compare(0, 7, "http://") != 0 && updateUrl.compare(0, 8, "https://") != 0)
			throw invalid_argument("Update URL must be blank or begin with http:// or https://");
		if(updateUrl.size() > 2000)
			throw invalid_argument("Update URL is too long");
		for(unsigned char ch : updateUrl)
			if(ch <= ' ' || ch == 0x7f)
				throw invalid_argument("Update URL must not contain spaces or control characters");
	}
	string extracts = c.quote_name(prefix + "extracts");
	string condition = extractId ? "id=" + to_string(extractId) : "name=" + w.quote(name);
	auto rows = w.exec("SELECT id FROM " + extracts + " WHERE " + condition + " ORDER BY id LIMIT 2");
	if(rows.empty()) throw runtime_error("Extract not found");
	if(rows.size() != 1) throw runtime_error("Extract name is ambiguous; select by ID");
	extractId = rows[0][0].as<int64_t>();
	w.exec("UPDATE " + extracts + " SET auto_update=" + (autoUpdate ? "true" : "false") +
		", update_url=" + w.quote(updateUrl) + " WHERE id=" + to_string(extractId));
	return extractId;
}

void DbListExtracts(pqxx::connection &c, pqxx::transaction_base &w, const string &prefix,
	int64_t extractId, bool withCounts, vector<ExtractInfo> &out)
{
	out.clear();
	auto table = [&](const string &suffix) { return c.quote_name(prefix + suffix); };
	auto count = [&](const string &kind) {
		if(!withCounts) return string("-1");
		return "(SELECT count(*) FROM " + table("extract_live" + kind + "s") + " o WHERE o.extract_id=e.id)";
	};
	auto rows = w.exec("SELECT e.id, e.name, ST_XMin(e.bbox), ST_YMin(e.bbox), ST_XMax(e.bbox), "
		"ST_YMax(e.bbox), e.use_bbox_in_query, EXTRACT(EPOCH FROM e.performed_at)::bigint, "
		"e.edit_activity_id, e.atomic_edit_id, (SELECT COALESCE(max(id),0) FROM " + table("edit_activity") +
		"), " + count("node") + ", " + count("way") + ", " + count("relation") +
		", e.auto_update, e.update_url, ST_AsText(e.bbox), ST_Equals(e.bbox, ST_Envelope(e.bbox)) FROM " +
		table("extracts") + " e" +
		(extractId > 0 ? " WHERE e.id=" + to_string(extractId) : string()) + " ORDER BY e.id");
	for(const auto &row : rows)
	{
		ExtractInfo info;
		info.extractId = row[0].as<int64_t>();
		if(!row[1].is_null()) info.name = row[1].as<string>();
		for(int i=2; i<6; i++) info.bbox.push_back(row[i].as<double>());
		info.useBboxInQuery = row[6].as<bool>();
		info.performedAt = row[7].as<int64_t>();
		if(!row[8].is_null()) info.editActivityId = row[8].as<int64_t>();
		if(!row[9].is_null()) info.atomicEditId = row[9].as<int64_t>();
		info.pendingActivity = info.editActivityId != row[10].as<int64_t>();
		info.nodes = row[11].as<int64_t>();
		info.ways = row[12].as<int64_t>();
		info.relations = row[13].as<int64_t>();
		info.autoUpdate = row[14].as<bool>();
		info.updateUrl = row[15].as<string>();
		info.boundaryWkt = row[16].as<string>();
		info.rectangular = row[17].as<bool>();
		out.push_back(info);
	}
}

vector<int64_t> DbListExtractIds(pqxx::connection &c, pqxx::transaction_base &w, const string &prefix)
{
	vector<int64_t> ids;
	for(const auto &row : w.exec("SELECT id FROM " + c.quote_name(prefix + "extracts") + " ORDER BY id"))
		ids.push_back(row[0].as<int64_t>());
	return ids;
}

void DbCompareExtract(PgTransaction &transaction, int64_t extractId,
	const string &name, ExtractComparison &out)
{
	out = ExtractComparison();
	auto stored = make_shared<ExtractVersionCollector>();
	auto exporter = transaction.StartExportExtract(extractId, name, stored);
	while(exporter->Continue() != 1) {}
	if(stored->bounds.size() != 4) throw runtime_error("Extract has no bbox");
	out.extractId = exporter->GetId();
	out.bbox = stored->bounds;
	out.pendingActivity = exporter->HasPendingActivity();
	out.queryModeDiffers = exporter->GetStoredBboxMode() != exporter->GetMapBboxMode();

	auto fresh = make_shared<ExtractVersionCollector>();
	shared_ptr<IDataStreamHandler> sink = fresh;
	auto query = transaction.GetQueryMgr();
	// The same query as made the extract: the rectangle, or the polygon
	ExtractInfo info;
	transaction.GetExtract(out.extractId, info);
	int status = info.rectangular ? query->Start(out.bbox, time(nullptr), sink) :
		query->Start(info.boundaryWkt, time(nullptr), sink);
	while(status == 0) status = query->Continue();
	if(status < 0) throw runtime_error("Map query failed");

	const char *kinds[] = {"node", "way", "relation"};
	out.extractCounts.assign(3, 0);
	out.queryCounts.assign(3, 0);
	for(const auto &entry : stored->versions) out.extractCounts[entry.first.first]++;
	for(const auto &entry : fresh->versions) out.queryCounts[entry.first.first]++;

	// Both maps are ordered by key, so one merge pass finds every difference.
	auto a = stored->versions.begin(), b = fresh->versions.begin();
	while(a != stored->versions.end() || b != fresh->versions.end())
	{
		ExtractDifference difference;
		bool inExtract = b == fresh->versions.end() || (a != stored->versions.end() && a->first <= b->first);
		bool inQuery = a == stored->versions.end() || (b != fresh->versions.end() && b->first <= a->first);
		const auto &key = inExtract ? a->first : b->first;
		difference.type = kinds[key.first];
		difference.objId = key.second;
		if(inExtract) difference.extractVersion = (a++)->second;
		if(inQuery) difference.queryVersion = (b++)->second;
		if(inExtract && inQuery)
		{
			if(difference.extractVersion == difference.queryVersion) continue;
			out.versionMismatches++;
		}
		else if(inExtract) out.notInQuery++;
		else out.missingFromExtract++;
		out.differences.push_back(difference);
	}
}

std::string DbCheckExtractBoundary(pqxx::transaction_base &work, const std::string &wkt)
{
	if(wkt.find_first_not_of(" \t\r\n") == string::npos)
		throw invalid_argument("The boundary is empty");
	// Read in a savepoint, so that text PostGIS cannot read leaves the
	// transaction usable for saying so
	pqxx::result r;
	pqxx::dbtransaction *outer = dynamic_cast<pqxx::dbtransaction *>(&work);
	string sql = "SELECT GeometryType(g), ST_IsEmpty(g), ST_IsValid(g), ST_IsValidReason(g), "
		"ST_XMin(g), ST_YMin(g), ST_XMax(g), ST_YMax(g), ST_AsText(g) FROM (SELECT ST_GeomFromText(" +
		work.quote(wkt) + ", 4326) AS g) boundary";
	try
	{
		if(outer != nullptr)
		{
			pqxx::subtransaction reading(*outer, "boundary");
			r = reading.exec(sql);
			reading.commit();
		}
		else
			r = work.exec(sql);
	}
	catch(const pqxx::sql_error &e)
	{
		string message = e.what();
		message = message.substr(0, message.find('\n'));
		throw invalid_argument("The boundary is not WKT that can be read: " + message);
	}
	string type = r[0][0].as<string>();
	if(type != "POLYGON")
		throw invalid_argument("The boundary must be a single polygon, not " + type);
	if(r[0][1].as<bool>())
		throw invalid_argument("The boundary is an empty polygon");
	if(!r[0][2].as<bool>())
		throw invalid_argument("The boundary is not a valid polygon: " + r[0][3].as<string>());
	double left = r[0][4].as<double>(), bottom = r[0][5].as<double>();
	double right = r[0][6].as<double>(), top = r[0][7].as<double>();
	if(left < -180 || right > 180 || bottom < -90 || top > 90)
		throw invalid_argument("The boundary goes beyond longitude -180 to 180 or latitude -90 to 90");
	if(right - left > 180)
		throw invalid_argument("The boundary is wider than 180 degrees of longitude, as one crossing the "
			"antimeridian would be; that is not supported");
	return r[0][8].as<string>();
}
