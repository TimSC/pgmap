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

	w.exec("CREATE TEMP TABLE " + seeds + " ON COMMIT DROP AS SELECT id FROM " + table("visiblenodes") + " WHERE geom && " + bbox);
	w.exec("CREATE UNIQUE INDEX ON " + seeds + " (id)");
	w.exec("CREATE TEMP TABLE " + ways + " ON COMMIT DROP AS " + (bboxMode ?
		"SELECT v.id FROM " + table("visibleways") + " v WHERE v.bbox && " + bbox :
		owners("way", "way_mems", seeds)));
	w.exec("CREATE UNIQUE INDEX ON " + ways + " (id)");
	w.exec("CREATE TEMP TABLE " + nodes + " ON COMMIT DROP AS SELECT id FROM " + seeds +
		" UNION SELECT member::text::bigint FROM (" + visible("way", "o.members", ways) +
		") v CROSS JOIN LATERAL jsonb_array_elements(v.members) member");
	w.exec("CREATE UNIQUE INDEX ON " + nodes + " (id)");
	w.exec("CREATE TEMP TABLE " + relations + " ON COMMIT DROP AS " + (bboxMode ?
		"SELECT v.id FROM " + table("visiblerelations") + " v WHERE v.bbox && " + bbox :
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
		" FROM " + table("extracts") + " e" +
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
	int status = query->Start(out.bbox, time(nullptr), sink);
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
