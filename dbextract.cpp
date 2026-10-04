#include "dbextract.h"
#include <stdexcept>
using namespace std;

int64_t DbUpdateExtractNodes(pqxx::connection &c, pqxx::transaction_base &w,
	const string &prefix, int64_t extractId, const string &name)
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
	// Node-only describes direct edits; unchanged parent objects may still need
	// inclusion/removal. We cannot skip unsupported edits and claim to be current.
	auto invalid = w.exec("SELECT id FROM " + table("edit_activity") + " WHERE " + pending +
		" AND (atomic_edit_id IS NULL OR block_index IS NULL OR atomic_edit_id<=" + to_string(previousAtomic) +
		" OR action NOT IN ('create','modify','delete') OR action IS NULL OR "
		"ways IS DISTINCT FROM 0 OR relations IS DISTINCT FROM 0 OR "
		"updated IS NULL OR jsonb_typeof(updated)<>'array' OR EXISTS "
		"(SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(updated)='array' "
		"THEN updated ELSE '[]'::jsonb END) ref WHERE ref->>0 IS DISTINCT FROM 'node')) LIMIT 1");
	if(!invalid.empty()) throw runtime_error("Unsupported or ungrouped activity at row " + invalid[0][0].as<string>() +
		"; node-only update aborted");
	auto broken = w.exec("SELECT atomic_edit_id FROM " + table("edit_activity") + " WHERE " + pending +
		" GROUP BY atomic_edit_id HAVING min(block_index)<>0 OR max(block_index)+1<>count(*) LIMIT 1");
	if(!broken.empty()) throw runtime_error("Incomplete atomic edit in pending activity");

	// All membership calculations below use current visible objects in the SAME
	// snapshot as the target checkpoint. Historical parent lookup is unnecessary
	// because this first implementation updates only to that snapshot, never an
	// intermediate revision. SQL temp tables bound client memory, although work
	// still scales with the extract/query area rather than only the changed nodes.
	string base = "pgmap_update_extract_" + id;
	string seeds = c.quote_name(base + "_seeds");
	string ways = c.quote_name(base + "_ways");
	string nodes = c.quote_name(base + "_nodes");
	string relations = c.quote_name(base + "_relations");
	string bbox = "(SELECT bbox FROM " + table("extracts") + " WHERE id=" + id + ")";
	w.exec("CREATE TEMP TABLE " + seeds + " ON COMMIT DROP AS SELECT id FROM " + table("visiblenodes") + " WHERE geom && " + bbox);
	w.exec("CREATE UNIQUE INDEX ON " + seeds + " (id)");
	string waySelection = bboxMode ? "v.bbox && " + bbox :
		"EXISTS (SELECT 1 FROM jsonb_array_elements(v.members) member JOIN " + seeds + " s ON s.id=member::text::bigint)";
	w.exec("CREATE TEMP TABLE " + ways + " ON COMMIT DROP AS SELECT v.id FROM " + table("visibleways") + " v WHERE " + waySelection);
	w.exec("CREATE UNIQUE INDEX ON " + ways + " (id)");
	w.exec("CREATE TEMP TABLE " + nodes + " ON COMMIT DROP AS SELECT id FROM " + seeds +
		" UNION SELECT member::text::bigint FROM " + table("visibleways") + " v JOIN " + ways +
		" s ON v.id=s.id CROSS JOIN LATERAL jsonb_array_elements(v.members) member");
	w.exec("CREATE UNIQUE INDEX ON " + nodes + " (id)");
	string relationSelection = bboxMode ? "v.bbox && " + bbox :
		"EXISTS (SELECT 1 FROM jsonb_array_elements(v.members) member WHERE "
		"(member->>0='node' AND (member->>1)::bigint IN (SELECT id FROM " + nodes + ")) OR "
		"(member->>0='way' AND (member->>1)::bigint IN (SELECT id FROM " + ways + ")))";
	w.exec("CREATE TEMP TABLE " + relations + " ON COMMIT DROP AS SELECT v.id FROM " + table("visiblerelations") + " v WHERE " + relationSelection);
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
		w.exec("INSERT INTO " + table("extract_live"+kind+"s") + " (extract_id," + columns + ") SELECT " +
			id + "," + columns + " FROM " + table("visible"+kind+"s") + " WHERE id IN (SELECT id FROM " + selected + ")");
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
    auto rows = w.exec("SELECT id, ST_XMin(bbox), ST_YMin(bbox), ST_XMax(bbox), ST_YMax(bbox) FROM " +
        connection->quote_name(prefix + "extracts") + " WHERE " + predicate + " ORDER BY id LIMIT 2");
    if(rows.empty()) throw runtime_error("Extract not found");
    if(rows.size() > 1) throw runtime_error("Extract name is ambiguous; select by ID");
    extractId = rows[0][0].as<int64_t>();
    output->StoreIsDiff(false);
    output->StoreBounds(rows[0][1].as<double>(), rows[0][2].as<double>(),
        rows[0][3].as<double>(), rows[0][4].as<double>());
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
