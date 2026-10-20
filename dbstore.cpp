#include "dbstore.h"
#include "dbids.h"
#include "dbcommon.h"
#include "dbjson.h"
#include "dbusername.h"
#include "dbquery.h"
#include "dbprepared.h"
#include "util.h"
#include <set>
using namespace std;

#define pqxxfield pqxx::field
#define pqxxrow pqxx::row

// The most objects written by one group of statements
static const size_t STORE_BATCH_OBJECTS = 500;
// The most rows in one INSERT of member rows
static const size_t STORE_BATCH_MEMBERS = 5000;

static string IdArrayLiteral(const std::vector<int64_t> &ids)
{
	stringstream ss;
	ss << "'{";
	for(size_t i=0; i<ids.size(); i++)
		ss << (i ? "," : "") << ids[i];
	ss << "}'::bigint[]";
	return ss.str();
}

// What is to be done with one object, decided from what the tables hold
struct StoreDecision
{
	const class OsmObject *obj;
	int64_t objId; // As stored: a new object has been given its ID
	bool copyLiveToOld, deleteLive, insertLive, updateLive, insertOld;
	bool memberRows; // Whether its member rows are still to be written
	bool dropMemberRows; // Whether the rows of the version that was live are to go
	string tagsJson, refsJson, rolesJson, wkt;
};

static bool StoreObjectBatch(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix,
	const std::string &typeStr,
	const std::vector<const class OsmObject *> &objPtrs, size_t first, size_t last,
	std::map<int64_t, int64_t> &createdIds,
	int64_t &nextObjId,
	const string &ocdn,
	std::string &errStr,
	int verbose,
	int64_t atomicEditId)
{
	/*
	 * Stores objPtrs[first..last), which must not name the same existing
	 * object twice. What happens to each object depends on the version the
	 * live table has of it (if any) and the highest version the history
	 * table has (if any):
	 *   - the live row goes if this is a deletion at least as new as it,
	 *   - the live row is first copied to history if this is newer than it,
	 *   - a visible object at least as new as both becomes the live row,
	 *     inserted or updated in place, and anything else goes to history.
	 * The member tables hold the members of live rows only (schema 15): a
	 * way or relation that stops being live, or is live at a new version,
	 * has its old member rows removed, and rows are written for what becomes
	 * live. A live row rewritten at the version it already has keeps its
	 * rows. On an older schema, which has no index to find an object's rows
	 * by, nothing is removed and rows are written for history versions too.
	 *
	 * The tables are asked about all the objects at once, and each kind of
	 * change is then made by one statement for all the objects it applies
	 * to, where it used to be several statements for every object.
	 */
	const bool isNode = typeStr == "node", isWay = typeStr == "way", isRelation = typeStr == "relation";
	const string liveTable = c.quote_name(tablePrefix + "live" + typeStr + "s");
	const string oldTable = c.quote_name(tablePrefix + "old" + typeStr + "s");
	const string idsTable = c.quote_name(tablePrefix + typeStr + "ids");
	const string extraColumns = isNode ? ", geom" : (isWay ? ", members" : ", members, memberroles");
	// Which atomic edit wrote each version (schema 16): atomicEditId for the
	// versions this store writes, NULL for one it is only copying (0), as an
	// upload does with the parents of what it changes. A row rewritten at the
	// version it has keeps its own, and a row moving from the live table to
	// history takes its own along.
	const bool atomicColumn = !work->exec("SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass(" +
		work->quote(liveTable) + ") AND attname = 'atomic_edit_id' AND NOT attisdropped").empty();
	const string atomicColumns = atomicColumn ? ", atomic_edit_id" : "";
	const string atomicValue = atomicColumn ? (atomicEditId > 0 ? "," + to_string(atomicEditId) : string(",NULL")) : "";
	// And which atomic edit made a version no longer current: in the history
	// table, where a version goes when a newer one replaces it, and in the
	// table of IDs this table set has taken over, for a version of its
	// parent's (the imported map, say) that it hid. Only an edit's new
	// versions replace anything; a copy is the version it was.
	const bool supersededColumn = atomicColumn && atomicEditId > 0 && !work->exec(
		"SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass(" + work->quote(oldTable) +
		") AND attname = 'superseded_atomic_edit_id' AND NOT attisdropped").empty();

	auto run = [&](const string &sql) {
		if(verbose >= 1)
			cout << sql << endl;
		work->exec(sql);
	};

	try
	{
		//Versions already in the live and history tables
		std::vector<int64_t> existingIds;
		for(size_t i=first; i<last; i++)
			if(objPtrs[i]->objId > 0)
				existingIds.push_back(objPtrs[i]->objId);
		std::map<int64_t, int64_t> liveVersions, oldVersions;
		if(!existingIds.empty())
		{
			string idArray = IdArrayLiteral(existingIds);
			pqxx::result r = work->exec("SELECT id, version FROM " + liveTable + " WHERE id = ANY(" + idArray + ");");
			for(pqxx::result::const_iterator row = r.begin(); row != r.end(); ++row)
				liveVersions[row[0].as<int64_t>()] = row[1].as<int64_t>();
			r = work->exec("SELECT id, MAX(version) FROM " + oldTable + " WHERE id = ANY(" + idArray + ") GROUP BY id;");
			for(pqxx::result::const_iterator row = r.begin(); row != r.end(); ++row)
				if(!row[1].is_null())
					oldVersions[row[0].as<int64_t>()] = row[1].as<int64_t>();
		}

		//Decide what happens to each object
		std::vector<StoreDecision> decisions;
		for(size_t i=first; i<last; i++)
		{
			const class OsmObject *osmObject = objPtrs[i];
			const class OsmNode *nodeObject = dynamic_cast<const class OsmNode *>(osmObject);
			const class OsmWay *wayObject = dynamic_cast<const class OsmWay *>(osmObject);
			const class OsmRelation *relationObject = dynamic_cast<const class OsmRelation *>(osmObject);
			if(isNode && nodeObject == nullptr)
				throw invalid_argument("Object type not node as expected");
			else if(isWay && wayObject == nullptr)
				throw invalid_argument("Object type not way as expected");
			else if(isRelation && relationObject == nullptr)
				throw invalid_argument("Object type not relation as expected");

			StoreDecision d;
			d.obj = osmObject;
			d.objId = osmObject->objId;
			int64_t version = osmObject->metaData.version;
			bool visible = osmObject->metaData.visible;

			auto liveIt = liveVersions.find(d.objId);
			bool foundExisting = d.objId > 0 && liveIt != liveVersions.end();
			int64_t currentVersion = foundExisting ? liveIt->second : -1;
			auto oldIt = oldVersions.find(d.objId);
			bool foundOld = d.objId > 0 && oldIt != oldVersions.end();
			int64_t oldVersion = foundOld ? oldIt->second : -1;

			d.deleteLive = foundExisting && version >= currentVersion && !visible;
			d.copyLiveToOld = foundExisting && version > currentVersion;
			bool latest = visible && (!foundExisting || version >= currentVersion) && (!foundOld || version >= oldVersion);
			d.insertLive = latest && !foundExisting;
			d.updateLive = latest && foundExisting;
			d.insertOld = !latest;
			// A live row rewritten at the version it already has, as happens to
			// the ways and relations holding an edited object, has its member
			// rows already. Adding them again would leave every member listed
			// once more each time.
			d.memberRows = !(d.updateLive && version == currentVersion);
			d.dropMemberRows = d.deleteLive || d.copyLiveToOld;

			if(d.objId <= 0)
			{
				if(version != 1)
				{
					errStr = "Cannot assign a new " + typeStr + " to any version but one.";
					return false;
				}

				//Assign a new ID
				d.objId = nextObjId;
				createdIds[osmObject->objId] = nextObjId;
				nextObjId ++;
			}

			//Convert tags, spatial and member data to appropriate formats
			EncodeTags(osmObject->tags, d.tagsJson);
			if(nodeObject != nullptr)
			{
				stringstream wkt;
				wkt.precision(9);
				wkt << "POINT(" << nodeObject->lon <<" "<< nodeObject->lat << ")";
				d.wkt = wkt.str();
			}
			else if(wayObject != nullptr)
				EncodeInt64Vec(wayObject->refs, d.refsJson);
			else
			{
				std::vector<std::string> refTypeStrs, refRoles;
				std::vector<int64_t> refIds;
				for(const RelationMember &member : relationObject->members)
				{
					refTypeStrs.push_back(ObjectTypeName(member.type));
					refIds.push_back(member.ref);
					refRoles.push_back(member.role);
				}
				EncodeRelationMems(refTypeStrs, refIds, d.refsJson);
				EncodeStringVec(refRoles, d.rolesJson);
			}
			decisions.push_back(d);
		}

		//The values of an object's own columns, after its ID
		auto metaValues = [&](const StoreDecision &d) {
			const class MetaData &m = d.obj->metaData;
			stringstream ss;
			ss << m.changeset << "," << work->quote(m.username) << "," << m.uid << "," << m.timestamp << ","
				<< m.version << "," << work->quote(d.tagsJson);
			return ss.str();
		};
		auto extraValues = [&](const StoreDecision &d) {
			if(isNode) return ",ST_GeometryFromText(" + work->quote(d.wkt) + ", 4326)";
			if(isWay) return "," + work->quote(d.refsJson);
			return "," + work->quote(d.refsJson) + "," + work->quote(d.rolesJson);
		};

		//Copy rows about to be superseded from the live to the history table.
		//This comes first: it needs the rows as they are.
		std::vector<int64_t> ids;
		for(const StoreDecision &d : decisions)
			if(d.copyLiveToOld) ids.push_back(d.objId);
		if(!ids.empty())
			run("INSERT INTO " + oldTable + " (id, changeset, changeset_index, username, uid, timestamp, version, tags, visible" +
				extraColumns + atomicColumns + ") SELECT id, changeset, changeset_index, username, uid, timestamp, version, tags, true" +
				extraColumns + atomicColumns + " FROM " + liveTable + " WHERE id = ANY(" + IdArrayLiteral(ids) + ") " + ocdn + ";");

		//Delete from the live table, and the shape kept for it, if any
		ids.clear();
		for(const StoreDecision &d : decisions)
			if(d.deleteLive) ids.push_back(d.objId);
		if(!ids.empty())
		{
			run("DELETE FROM " + liveTable + " WHERE id = ANY(" + IdArrayLiteral(ids) + ");");
			if(!isNode && DbCheckTableExists(c, work, tablePrefix + typeStr + "_shapes"))
				run("DELETE FROM " + c.quote_name(tablePrefix + typeStr + "_shapes") + " WHERE id = ANY(" + IdArrayLiteral(ids) + ");");
		}

		//Insert into the live table
		stringstream sql;
		size_t rows = 0;
		for(const StoreDecision &d : decisions)
		{
			if(!d.insertLive) continue;
			sql << (rows ? "," : "") << "(" << d.objId << "," << metaValues(d) << extraValues(d) << atomicValue << ")";
			rows ++;
		}
		if(rows > 0)
			run("INSERT INTO " + liveTable + " (id, changeset, username, uid, timestamp, version, tags" +
				extraColumns + atomicColumns + ") VALUES " + sql.str() + ";");

		//Update rows of the live table in place. One statement for each row,
		//sent together.
		sql.str("");
		rows = 0;
		for(const StoreDecision &d : decisions)
		{
			if(!d.updateLive) continue;
			const class MetaData &m = d.obj->metaData;
			sql << "UPDATE " << liveTable << " SET changeset=" << m.changeset << ", username=" << work->quote(m.username)
				<< ", uid=" << m.uid << ", timestamp=" << m.timestamp << ", version=" << m.version
				<< ", tags=" << work->quote(d.tagsJson);
			if(isNode)
				sql << ", geom=ST_GeometryFromText(" << work->quote(d.wkt) << ", 4326)";
			else if(isWay)
				sql << ", members=" << work->quote(d.refsJson);
			else
				sql << ", members=" << work->quote(d.refsJson) << ", memberroles=" << work->quote(d.rolesJson);
			// A new version is this edit's; the same version keeps whose it was
			if(atomicColumn && (int64_t)m.version != liveVersions[d.objId])
				sql << ", atomic_edit_id=" << atomicValue.substr(1);
			sql << " WHERE id = " << d.objId << ";";
			rows ++;
		}
		if(rows > 0)
			run(sql.str());

		//Insert into the history table
		sql.str("");
		rows = 0;
		for(const StoreDecision &d : decisions)
		{
			if(!d.insertOld) continue;
			sql << (rows ? "," : "") << "(" << d.objId << "," << metaValues(d) << ","
				<< (d.obj->metaData.visible ? "true" : "false") << extraValues(d) << atomicValue << ")";
			rows ++;
		}
		if(rows > 0)
			run("INSERT INTO " + oldTable + " (id, changeset, username, uid, timestamp, version, tags, visible" +
				extraColumns + atomicColumns + ") VALUES " + sql.str() + " " + ocdn + ";");

		//The versions this edit's new ones replace, which have gone, or were
		//already, into history: all of an object's earlier versions not yet
		//marked, which is the one that was current. A version stored out of
		//order, older than one the tables have, replaces nothing.
		if(supersededColumn)
		{
			std::vector<int64_t> newIds, newVersions;
			for(const StoreDecision &d : decisions)
			{
				int64_t version = d.obj->metaData.version;
				auto live = liveVersions.find(d.objId);
				auto old = oldVersions.find(d.objId);
				if((live == liveVersions.end() || version > live->second) && (old == oldVersions.end() || version > old->second))
				{
					newIds.push_back(d.objId);
					newVersions.push_back(version);
				}
			}
			if(!newIds.empty())
				run("UPDATE " + oldTable + " o SET superseded_atomic_edit_id = " + to_string(atomicEditId) +
					" FROM unnest(" + IdArrayLiteral(newIds) + ", " + IdArrayLiteral(newVersions) + ") n(id, version)"
					" WHERE o.id = n.id AND o.version < n.version AND o.superseded_atomic_edit_id IS NULL;");
		}

		//Update existing id lists (nodeids, wayids, relationids). An object
		//this table set did not have before is either new, or its parent's,
		//whose version this edit replaces and hides from now on.
		ids.clear();
		std::vector<int64_t> takenOver;
		for(const StoreDecision &d : decisions)
			if(d.insertLive || d.insertOld)
				(supersededColumn && d.obj->objId > 0 ? takenOver : ids).push_back(d.objId);
		if(!ids.empty())
			run("INSERT INTO " + idsTable + " (id) SELECT DISTINCT v FROM unnest(" + IdArrayLiteral(ids) +
				") v WHERE NOT EXISTS (SELECT 1 FROM " + idsTable + " x WHERE x.id = v);");
		if(!takenOver.empty())
			run("INSERT INTO " + idsTable + " (id, superseded_atomic_edit_id) SELECT DISTINCT v, " +
				to_string(atomicEditId) + " FROM unnest(" + IdArrayLiteral(takenOver) +
				") v WHERE NOT EXISTS (SELECT 1 FROM " + idsTable + " x WHERE x.id = v);");

		//Update member tables
		if(isWay || isRelation)
		{
			const char *suffixes[3] = {"way_mems", "", ""};
			if(isRelation)
			{
				suffixes[0] = "relation_mems_n";
				suffixes[1] = "relation_mems_w";
				suffixes[2] = "relation_mems_r";
			}
			// Whether the tables are as schema 15 has them: keyed, and holding
			// live versions only
			bool liveOnly = !work->exec("SELECT to_regclass(" + work->quote(c.quote_name(
				tablePrefix + suffixes[0] + "_key")) + ")")[0][0].is_null();
			if(liveOnly)
			{
				ids.clear();
				for(const StoreDecision &d : decisions)
					if(d.dropMemberRows || (d.memberRows && (d.insertLive || d.updateLive)))
						ids.push_back(d.objId);
				if(!ids.empty())
					for(const char *suffix : suffixes)
						if(suffix[0] != '\0')
							run("DELETE FROM " + c.quote_name(tablePrefix + suffix) + " WHERE id = ANY(" +
								IdArrayLiteral(ids) + ");");
			}

			stringstream memberSql[3];
			size_t memberRows[3] = {0, 0, 0};
			auto flush = [&](int table) {
				if(memberRows[table] == 0) return;
				run("INSERT INTO " + c.quote_name(tablePrefix + suffixes[table]) +
					" (id, version, index, member) VALUES " + memberSql[table].str() + ";");
				memberSql[table].str("");
				memberRows[table] = 0;
			};
			auto add = [&](int table, int64_t objId, int64_t version, size_t index, int64_t member) {
				memberSql[table] << (memberRows[table] ? "," : "") << "(" << objId << "," << version << ","
					<< index << "," << member << ")";
				memberRows[table] ++;
				if(memberRows[table] >= STORE_BATCH_MEMBERS) flush(table);
			};
			for(const StoreDecision &d : decisions)
			{
				if(!d.memberRows) continue;
				if(liveOnly && d.insertOld) continue;
				if(isWay)
				{
					const class OsmWay *way = dynamic_cast<const class OsmWay *>(d.obj);
					for(size_t j=0; j<way->refs.size(); j++)
						add(0, d.objId, way->metaData.version, j, way->refs[j]);
				}
				else
				{
					const class OsmRelation *relation = dynamic_cast<const class OsmRelation *>(d.obj);
					for(size_t j=0; j<relation->members.size(); j++)
					{
						const RelationMember &member = relation->members[j];
						add((int)member.type, d.objId, relation->metaData.version, j, member.ref);
					}
				}
			}
			for(int table=0; table<3; table++)
				flush(table);
		}
	}
	catch (const pqxx::sql_error &e)
	{
		stringstream ss2;
		ss2 << e.what() << ":" << e.query();
		errStr = ss2.str();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}
	return true;
}

bool ObjectsToDatabase(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const std::string &typeStr,
	const std::vector<const class OsmObject *> &objPtrs, 
	std::map<int64_t, int64_t> &createdNodeIds,
	map<string, int64_t> &nextIdMap,
	std::string &errStr,
	int verbose,
	int64_t atomicEditId)
{
	auto it = nextIdMap.find(typeStr);
	int64_t &nextObjId = it->second;

	const string ocdn = " ON CONFLICT DO NOTHING";

	//The objects are stored a batch at a time. A batch ends early at an
	//object the batch already has, so that the later one finds what the
	//earlier one stored, as when a diff holds several versions of an object.
	size_t first = 0;
	while(first < objPtrs.size())
	{
		std::set<int64_t> seen;
		size_t last = first;
		while(last < objPtrs.size() && last - first < STORE_BATCH_OBJECTS)
		{
			int64_t objId = objPtrs[last]->objId;
			if(objId > 0 && !seen.insert(objId).second)
				break;
			last ++;
		}
		if(!StoreObjectBatch(c, work, tablePrefix, typeStr, objPtrs, first, last,
			createdNodeIds, nextObjId, ocdn, errStr, verbose, atomicEditId))
			return false;
		first = last;
	}
	return true;
}

bool StoreObjects(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tablePrefix, 
	class OsmData osmData, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	std::string &errStr,
	int64_t atomicEditId)
{
	map<string, int64_t> nextIdMapOriginal, nextIdMap;
	bool ok = GetNextObjectIds(c, work, tablePrefix, nextIdMapOriginal, errStr);
	if(!ok)
		return false;
	nextIdMap = nextIdMapOriginal;

	//Store nodes
	std::vector<const class OsmObject *> objPtrs;
	for(size_t i=0; i<osmData.nodes.size(); i++)
		objPtrs.push_back(&osmData.nodes[i]);
	ok = ObjectsToDatabase(c, work, tablePrefix, "node", objPtrs, createdNodeIds, nextIdMap, errStr, 0, atomicEditId);
	if(!ok)
		return false;

	//Update numbering for created nodes used in ways and relations
	for(size_t i=0; i<osmData.ways.size(); i++)
	{
		class OsmWay &way = osmData.ways[i];
		for(size_t j=0; j<way.refs.size(); j++)
		{
			if(way.refs[j] > 0) continue;
			std::map<int64_t, int64_t>::iterator it = createdNodeIds.find(way.refs[j]);
			if(it == createdNodeIds.end())
			{
				stringstream ss;
				ss << "Way "<< way.objId << " depends on undefined node " << way.refs[j];
				errStr = ss.str();
				return false;
			}
			way.refs[j] = it->second;
		}
	}

	for(size_t i=0; i<osmData.relations.size(); i++)
	{
		class OsmRelation &rel = osmData.relations[i];
		for(RelationMember &member : rel.members)
		{
			if(member.type != ObjectType::Node or member.ref > 0) continue;
			std::map<int64_t, int64_t>::iterator it = createdNodeIds.find(member.ref);
			if(it == createdNodeIds.end())
			{
				stringstream ss;
				ss << "Relation "<< rel.objId << " depends on undefined node " << member.ref;
				errStr = ss.str();
				return false;
			}
			member.ref = it->second;
		}
	}

	//Store ways
	objPtrs.clear();
	for(size_t i=0; i<osmData.ways.size(); i++)
		objPtrs.push_back(&osmData.ways[i]);
	ok = ObjectsToDatabase(c, work, tablePrefix, "way", objPtrs, createdWayIds, nextIdMap, errStr, 0, atomicEditId);
	if(!ok)
		return false;

	//Update numbering for created ways used in relations
	for(size_t i=0; i<osmData.relations.size(); i++)
	{
		class OsmRelation &rel = osmData.relations[i];
		for(RelationMember &member : rel.members)
		{
			if(member.type != ObjectType::Way or member.ref > 0) continue;
			std::map<int64_t, int64_t>::iterator it = createdWayIds.find(member.ref);
			if(it == createdWayIds.end())
			{
				stringstream ss;
				ss << "Relation "<< rel.objId << " depends on undefined way " << member.ref;
				errStr = ss.str();
				return false;
			}
			member.ref = it->second;
		}
	}

	//Update numbering for created relations used in later relations. A relation
	//can only depend on one that comes before it. The IDs are those the store
	//below is about to give out, in the same order.
	std::map<int64_t, int64_t> relationIdsToCome;
	int64_t nextRelationId = nextIdMap["relation"];
	for(size_t i=0; i<osmData.relations.size(); i++)
	{
		class OsmRelation &rel = osmData.relations[i];
		for(RelationMember &member : rel.members)
		{
			if(member.type != ObjectType::Relation or member.ref > 0) continue;
			std::map<int64_t, int64_t>::iterator it = createdRelationIds.find(member.ref);
			if(it == createdRelationIds.end())
			{
				it = relationIdsToCome.find(member.ref);
				if(it == relationIdsToCome.end())
				{
					stringstream ss;
					ss << "Relation "<< rel.objId << " depends on undefined relation " << member.ref;
					errStr = ss.str();
					return false;
				}
			}
			member.ref = it->second;
		}
		if(rel.objId <= 0)
			relationIdsToCome[rel.objId] = nextRelationId ++;
	}

	//Store relations
	objPtrs.clear();
	for(size_t i=0; i<osmData.relations.size(); i++)
		objPtrs.push_back(&osmData.relations[i]);
	ok = ObjectsToDatabase(c, work, tablePrefix, "relation", objPtrs, createdRelationIds, nextIdMap, errStr, 0, atomicEditId);
	if(!ok)
		return false;

	ok = UpdateNextObjectIds(c, work, tablePrefix, nextIdMap, nextIdMapOriginal, errStr);
	if(!ok)
		return false;
	return true;
}

// A way's shape is the line through its nodes, in order. It is NULL when it
// cannot be drawn whole: with fewer than two nodes, or with a node that is
// not to be found (as at the edge of an extract). A closed way is a closed
// line; whether it is an area is for the renderer, by its tags.
std::string WayShapeSql(pqxx::connection &c, const std::string &tablePrefix, const std::string &alias)
{
	return "(SELECT CASE WHEN count(*) >= 2 AND count(n.geom) = count(*) THEN ST_MakeLine(n.geom ORDER BY e.ord) END "
		"FROM jsonb_array_elements_text(" + alias + ".members) WITH ORDINALITY AS e(ref, ord) "
		"LEFT JOIN " + c.quote_name(tablePrefix + "visiblenodes") + " n ON n.id = e.ref::bigint)";
}

// A relation's shape is made from its node and way members. A multipolygon or
// boundary is the area its ways enclose, as a MultiPolygon (roles are not
// needed to tell holes from outlines); any other relation is the collection
// of its nodes' points and its ways' lines, in the order of its members.
// Member relations are left out.
// It is NULL with no node or way members, when one of them is not to be found
// or has no shape, or when a multipolygon's ways enclose no area.
std::string RelationShapeSql(pqxx::connection &c, const std::string &tablePrefix, const std::string &alias)
{
	string area = alias + ".tags->>'type' IN ('multipolygon', 'boundary')";
	return "(SELECT CASE WHEN s.total = 0 OR s.found < s.total THEN NULL "
		"WHEN " + area + " THEN CASE WHEN ST_IsEmpty(s.area) THEN NULL ELSE s.area END "
		"ELSE s.everything END FROM ("
		"SELECT count(*) AS total, count(p.g) AS found, "
		"ST_Multi(ST_CollectionExtract(ST_BuildArea(ST_Collect(p.g) FILTER (WHERE p.kind = 'way' AND " + area + ")), 3)) AS area, "
		"ST_Collect(p.g ORDER BY p.ord) AS everything FROM ("
		"SELECT m.value->>0 AS kind, m.ord, CASE m.value->>0 WHEN 'node' THEN n.geom ELSE w.shape END AS g "
		"FROM jsonb_array_elements(" + alias + ".members) WITH ORDINALITY m(value, ord) "
		"LEFT JOIN " + c.quote_name(tablePrefix + "visiblenodes") + " n ON m.value->>0 = 'node' AND n.id = (m.value->>1)::bigint "
		"LEFT JOIN " + c.quote_name(tablePrefix + "visible_way_shapes") + " w ON m.value->>0 = 'way' AND w.id = (m.value->>1)::bigint "
		"WHERE m.value->>0 IN ('node', 'way')) p) s)";
}

int UpdateWayBboxesById(pqxx::connection &c, pqxx::transaction_base *work,
	const std::set<int64_t> &wayIds,
	int verbose,
	const std::string &tablePrefix, 
	std::string &errStr)
{
	stringstream sqlFrags;
	int count = 0;
	string wayTable = tablePrefix+"liveways";
	string shape;
	for(std::set<int64_t>::const_iterator it = wayIds.begin(); it != wayIds.end(); it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << wayTable << ".id = " << *it;
		count ++;

		if(count > 100)
		{
			string sql = "UPDATE "+tablePrefix+"liveways SET bbox=ST_Envelope(ST_Union(ARRAY(SELECT geom FROM "+tablePrefix+"visiblenodes WHERE "+tablePrefix+"visiblenodes.id::bigint = ANY(ARRAY(SELECT jsonb_array_elements("+tablePrefix+"liveways.members))::text[]::bigint[])))) WHERE ("+sqlFrags.str()+");";
			//cout << sql << endl;

			work->exec(sql);

			sqlFrags.str("");
			count = 0;
		}
	}

	if(count > 0)
	{
		string sql = "UPDATE "+tablePrefix+"liveways SET bbox=ST_Envelope(ST_Union(ARRAY(SELECT geom FROM "+tablePrefix+"visiblenodes WHERE "+tablePrefix+"visiblenodes.id::bigint = ANY(ARRAY(SELECT jsonb_array_elements("+tablePrefix+"liveways.members))::text[]::bigint[])))) WHERE ("+sqlFrags.str()+");";
		//cout << sql << endl;

		work->exec(sql);
	}

	// The shapes, where the table set keeps them, of those ways it has. A way
	// it does not have, deleted say, lost its shape as it went (StoreObjects).
	if(!wayIds.empty() && DbCheckTableExists(c, work, tablePrefix + "way_shapes"))
	{
		std::vector<int64_t> ids(wayIds.begin(), wayIds.end());
		work->exec("INSERT INTO " + c.quote_name(tablePrefix + "way_shapes") + " (id, shape) SELECT w.id, " +
			WayShapeSql(c, tablePrefix, "w") + " FROM " + c.quote_name(wayTable) + " w WHERE w.id = ANY(" +
			IdArrayLiteral(ids) + ") ON CONFLICT (id) DO UPDATE SET shape = EXCLUDED.shape;");
	}

	return 0;	
}

// The stored boxes of relations, as the boxes they are made of: one, or two
// for a relation spread across the antimeridian (see FindOuterBboxes). A
// relation without a box is there with none.
static void GetVisibleRelationBoxPiecesById(pqxx::connection &c, pqxx::transaction_base *work,
	const string &tablePrefix, const std::set<int64_t> &ids,
	std::map<int64_t, std::vector<std::vector<double> > > &out)
{
	if(ids.empty())
		return;
	string relations = c.quote_name(tablePrefix + "visiblerelations");
	pqxx::result rows = work->exec("SELECT v.id, ST_XMin(d.geom), ST_YMin(d.geom), ST_XMax(d.geom), ST_YMax(d.geom) "
		"FROM " + relations + " v LEFT JOIN LATERAL ST_Dump(v.bbox) d ON true "
		"WHERE v.id = ANY(" + IdArrayLiteral(std::vector<int64_t>(ids.begin(), ids.end())) + ");");
	for(const pqxx::row &row : rows)
	{
		std::vector<std::vector<double> > &pieces = out[row[0].as<int64_t>()];
		if(!row[1].is_null())
			pieces.push_back({row[1].as<double>(), row[2].as<double>(), row[3].as<double>(), row[4].as<double>()});
	}
}

// A relation's box as SQL: one envelope, or two collected as a multipolygon
static string RelationBoxSql(const std::vector<std::vector<double> > &pieces)
{
	stringstream sql;
	sql.precision(17);
	for(size_t i = 0; i < pieces.size(); i++)
	{
		const std::vector<double> &b = pieces[i];
		sql << (i ? ", " : "") << "ST_MakeEnvelope(" << b[0] << ", " << b[1] << ", " << b[2] << ", " << b[3] << ", 4326)";
	}
	return pieces.size() == 1 ? sql.str() : "ST_Multi(ST_Collect(ARRAY[" + sql.str() + "]))";
}

void UpdateSingleRelation(pqxx::connection &conn, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	OsmRelation &rel,
	const std::set<int64_t> &pendingRelIds,
	const std::set<int64_t> &skipRelIds,  
	std::set<int64_t> &nextPassIds, int verbose, bool shapes)
{
	class DbUsernameLookup dbUsernameLookup(conn, work, "", ""); //Don't care about accurate usernames

	std::set<int64_t> memNodeIds, memWayIds, memRelIds;
	for(const RelationMember &member : rel.members)
	{
		if(member.type == ObjectType::Node) memNodeIds.insert(member.ref);
		if(member.type == ObjectType::Way) memWayIds.insert(member.ref);
		if(member.type == ObjectType::Relation and skipRelIds.find(member.ref) == skipRelIds.end()) 
			memRelIds.insert(member.ref);
	}

	//Check of member relations have already been processed
	bool dependsOnPending = false;
	for(auto it2=memRelIds.begin(); it2!=memRelIds.end(); it2++)
	{
		dependsOnPending = pendingRelIds.find(*it2) != pendingRelIds.end();
		if(dependsOnPending) break;
	}
	if(dependsOnPending)
	{
		nextPassIds.insert(rel.objId);
		return;
	}

	std::map<int64_t, vector<double> > memBboxesOfType1, memBboxesOfType2;
	std::vector<vector<double> > memBboxes;
	GetVisibleObjectBboxesById(conn, work, dbUsernameLookup,
		tablePrefix, "node", memNodeIds, memBboxesOfType1);
	for(auto it=memBboxesOfType1.begin(); it!=memBboxesOfType1.end(); it++)
	{
		vector<double> &bb = it->second; 
		memBboxes.push_back(bb);
	}

	// A member that exists but has no stored box must not simply be left out:
	// the relation's box would then be too small, and nothing would say so.
	// That happens on a map whose base tables never had their boxes worked out.
	bool extentUnknown = false;

	GetVisibleObjectBboxesById(conn, work, dbUsernameLookup,
		tablePrefix, "way", memWayIds, memBboxesOfType2);

	// A way's box can be had from its nodes
	std::set<int64_t> unboxedWayIds;
	for(auto it=memBboxesOfType2.begin(); it!=memBboxesOfType2.end(); it++)
		if(it->second.size() != 4)
			unboxedWayIds.insert(it->first);
	if(!unboxedWayIds.empty())
	{
		std::shared_ptr<class OsmData> ways(new class OsmData());
		auto wayIt = unboxedWayIds.begin();
		while(wayIt != unboxedWayIds.end())
			GetVisibleObjectsById(conn, work, dbUsernameLookup, tablePrefix, "way", unboxedWayIds, wayIt, 1000, ways);
		std::set<int64_t> wayNodeIds;
		for(const OsmWay &way : ways->ways)
			wayNodeIds.insert(way.refs.begin(), way.refs.end());
		std::map<int64_t, vector<double> > positions;
		GetVisibleObjectBboxesById(conn, work, dbUsernameLookup, tablePrefix, "node", wayNodeIds, positions);
		for(const OsmWay &way : ways->ways)
		{
			std::vector<vector<double> > nodeBoxes;
			for(int64_t ref : way.refs)
			{
				auto found = positions.find(ref);
				if(found != positions.end())
					nodeBoxes.push_back(found->second);
			}
			// With none of its nodes to be found a way has no extent, which is not the same as not knowing it
			std::vector<double> wayBox;
			FindOuterBbox(nodeBoxes, wayBox);
			memBboxesOfType2[way.objId] = wayBox;
		}
	}
	for(auto it=memBboxesOfType2.begin(); it!=memBboxesOfType2.end(); it++)
	{
		vector<double> &bb = it->second; 
		memBboxes.push_back(bb);
	}

	// Each piece of a member relation's box, as one spread across the
	// antimeridian is two
	std::map<int64_t, std::vector<std::vector<double> > > memRelationPieces;
	GetVisibleRelationBoxPiecesById(conn, work, tablePrefix, memRelIds, memRelationPieces);

	// A member relation's box cannot be had so cheaply. Without one, and with
	// members of its own that might have an extent, this relation's box is not
	// known, and is stored as NULL, which is what that means everywhere else.
	std::set<int64_t> unboxedRelationIds;
	for(auto it=memRelationPieces.begin(); it!=memRelationPieces.end(); it++)
		if(it->second.empty())
			unboxedRelationIds.insert(it->first);
	if(!unboxedRelationIds.empty())
	{
		std::shared_ptr<class OsmData> relations(new class OsmData());
		auto relIt = unboxedRelationIds.begin();
		while(relIt != unboxedRelationIds.end())
			GetVisibleObjectsById(conn, work, dbUsernameLookup, tablePrefix, "relation", unboxedRelationIds, relIt, 1000, relations);
		for(const OsmRelation &member : relations->relations)
			if(!member.members.empty())
				extentUnknown = true;
	}
	for(auto it=memRelationPieces.begin(); it!=memRelationPieces.end(); it++)
		for(const vector<double> &piece : it->second)
			memBboxes.push_back(piece);

	// One box, or two either side of the antimeridian
	std::vector<std::vector<double> > outerBboxes;
	if(!extentUnknown)
		FindOuterBboxes(memBboxes, outerBboxes);

	if(!outerBboxes.empty())
	{
		string sql = "UPDATE " + conn.quote_name(tablePrefix + "liverelations") + " SET bbox=" +
			RelationBoxSql(outerBboxes) + " WHERE id = " + to_string(rel.objId) + ";";
		if(verbose >= 2) cout << sql << endl;
		work->exec(sql);
	}
	else
	{
		stringstream sql;
		sql << "UPDATE " << tablePrefix << "liverelations SET bbox=null WHERE id = "<<rel.objId<<";";
		if(verbose >= 2) cout << sql.str() << endl;

		work->exec(sql.str());
	}

	// The shape, where the table set keeps them. The member ways' shapes are
	// brought up to date first, as their boxes are.
	if(shapes && DbCheckTableExists(conn, work, tablePrefix + "relation_shapes"))
	{
		string sql = "INSERT INTO " + conn.quote_name(tablePrefix + "relation_shapes") + " (id, shape) SELECT r.id, " +
			RelationShapeSql(conn, tablePrefix, "r") + " FROM " + conn.quote_name(tablePrefix + "liverelations") +
			" r WHERE r.id = " + to_string(rel.objId) + " ON CONFLICT (id) DO UPDATE SET shape = EXCLUDED.shape;";
		if(verbose >= 2) cout << sql << endl;
		work->exec(sql);
	}
}

int UpdateRelationBboxesById(pqxx::connection &conn, pqxx::transaction_base *work,
	const std::set<int64_t> &objectIds,
	int verbose,
	const std::string &tablePrefix, 
	std::string &errStr,
	bool shapes)
{
	class DbUsernameLookup dbUsernameLookup(conn, work, "", ""); //Don't care about accurate usernames
	std::set<int64_t> pendingRelIds = objectIds;
	int passNum = 0;
	std::set<int64_t> emptySet;

	while(pendingRelIds.size() > 0 and passNum < 10)
	{
		std::set<int64_t> nextPassIds;
		//cout << "UpdateRelationBboxesById pass " << passNum << ", pending " << pendingRelIds.size() << endl;

		for(auto it1 = pendingRelIds.begin(); it1 != pendingRelIds.end(); it1 ++)
		{
			std::set<int64_t> singleObjId;
			singleObjId.insert(*it1);
			auto firstRecord = singleObjId.begin();

			std::shared_ptr<class OsmData> relObjs(new class OsmData());
			GetVisibleObjectsById(conn, work, dbUsernameLookup,
				tablePrefix,
				"relation",
				singleObjId, 
				firstRecord, 1000, relObjs);
			
			if(relObjs->relations.size()==0)
				continue;
			OsmRelation &rel = relObjs->relations[0];
	
			UpdateSingleRelation(conn, work, tablePrefix, rel, 
				pendingRelIds, emptySet, 
				nextPassIds, verbose, shapes);
		}
		
		if(pendingRelIds.size() == nextPassIds.size())
		{
			pendingRelIds = nextPassIds;
			break; //Stop if we are not making progress
		}

		pendingRelIds = nextPassIds;
		passNum ++;
	}

	//The remaining relations probably have circular dependencies
	//Process them ignoring circular dependencies at first
	//Then rerun circular dependencies again with minimal bbox assumption
	for(int pass=0; pass<10; pass++)
	{
		std::set<int64_t> unused;

		for(auto it1 = pendingRelIds.begin(); it1 != pendingRelIds.end(); it1 ++)
		{
			std::set<int64_t> singleObjId;
			singleObjId.insert(*it1);
			auto firstRecord = singleObjId.begin();

			std::shared_ptr<class OsmData> relObjs(new class OsmData());
			GetVisibleObjectsById(conn, work, dbUsernameLookup,
				tablePrefix,
				"relation",
				singleObjId, 
				firstRecord, 1000, relObjs);
			
			if(relObjs->relations.size()==0)
				continue;
			OsmRelation &rel = relObjs->relations[0];

			std::set<int64_t> skipRelIds;
			if(pass == 0) skipRelIds = pendingRelIds;

			UpdateSingleRelation(conn, work, tablePrefix, rel, 
				emptySet, skipRelIds, 
				unused, verbose, shapes);
		}
	}

	return 0;
}

bool DbInsertQueryActivity(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	int64_t timestamp,
	const std::vector<double> &bbox,
	std::string &errStr,
	int verbose)
{

	stringstream sql;
	sql << "INSERT INTO "<< c.quote_name(tablePrefix+"query_activity") << " (timestamp, bbox, metrics) VALUES ";
	sql << "("<<timestamp<<",";
	if(bbox.size() == 4)
		sql << "ST_MakeEnvelope("<<bbox[0]<<", "<<bbox[1]<<", "<<bbox[2]<<", "<<bbox[3]<<", 4326)";
	else
		sql << "null";
	sql << ",null);";

	try
	{
		if(verbose >= 1)
			cout << sql.str() << endl;
		work->exec(sql.str());
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


