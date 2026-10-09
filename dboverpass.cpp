
#include "dboverpass.h"
#include "dbdecode.h"
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h> //rapidjson-dev
#include "dbquery.h"
#include <cmath>

/*
Example queries :

SELECT id, tags FROM planet_static_liveways WHERE tags @> '{"name":"Portsmouth"}'::jsonb  LIMIT 10;

SELECT id, tags FROM planet_static_liveways WHERE tags ? 'highway' LIMIT 10;

SELECT id, tags FROM planet_static_livenodes WHERE tags @> '{"natural":"peak"}'::jsonb AND geom && ST_MakeEnvelope(-1.146698,50.7338476,-0.9338379,50.8744446, 4326) LIMIT 100;

*/

std::string DbXapiQueryGenerateSql(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox)
{
	string objTable = c.quote_name(tablePrefix+"visible"+objType+"s");

	string sql = "SELECT *";

	if(objType == "node")
		sql += ", ST_X(geom) as lon, ST_Y(geom) AS lat";

	sql += " FROM "+ objTable;

	if(tagKey.size() > 0 or bbox.size() == 4)
	{
		sql += " WHERE (";

		if(tagKey.size() > 0)
		{
			if(tagValue.size() > 0)
			{
				StringBuffer buffer;
				Writer<StringBuffer> writer(buffer);

				writer.StartObject();
				writer.Key(tagKey.c_str(), tagKey.size(), true);
				writer.String(tagValue.c_str(), tagValue.size(), true);
				writer.EndObject(1);

				sql += "jsonb_to_tsvector('english', tags, '\"all\"') @@ to_tsquery('english', "+work->quote(tagKey)+") AND jsonb_to_tsvector('english', tags, '\"all\"') @@ to_tsquery('english', "+work->quote(tagValue)+") AND tags @> "+work->quote(buffer.GetString())+"::jsonb";
			}
			else
			{
				sql += "jsonb_to_tsvector('english', tags, '\"all\"') @@ to_tsquery('english', "+work->quote(tagKey)+") AND tags ? "+work->quote(tagKey);
			}
		}

		if(bbox.size() == 4)
		{
			if(tagKey.size() > 0) sql += " AND";

			stringstream sql2;
			sql2.precision(9);
			if(objType == "node")
				sql2 << " geom";
			else
				sql2 << " bbox";
			sql2 << fixed << " && ST_MakeEnvelope("<<bbox[0]<<","<<bbox[1]<<","<<bbox[2]<<","<<bbox[3]<<", 4326)";
			sql += sql2.str();
		}

		sql += ")";
	}

	sql += ";";

	return sql;
}


void DbXapiQueryIdVisible(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox, 
	std::vector<int64_t> &idsOut)
{
	string sql = DbXapiQueryGenerateSql(c, work, 
		tablePrefix, 
		objType,
		tagKey,
		tagValue,
		bbox);

	pqxx::icursorstream cursor( *work, sql, "nodecursor", 1000 );

	int records = 1;
	while (records>0)
	{		
		records = ObjectResultsToListIdVer(cursor,
			&idsOut,
			nullptr);
	}
}

void DbXapiQueryObjVisible(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	string sql = DbXapiQueryGenerateSql(c, work, 
		tablePrefix, 
		objType,
		tagKey,
		tagValue,
		bbox);

	pqxx::icursorstream cursor( *work, sql, "nodecursor", 1000 );

	int count = 1;
	if(objType == "node")
		while(count > 0)
			count = NodeResultsToEncoder(cursor, usernames, enc);
	if(objType == "way")
		while(count > 0)
			count = WayResultsToEncoder(cursor, usernames, enc);
	if(objType == "relation")
	{
		std::set<int64_t> skipIds;
		RelationResultsToEncoder(cursor, usernames, skipIds, enc);
	}
}

void DbXapiQueryVisible(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	std::set<int64_t> relationIdsSet, wayIdsSet, nodeIdsSet;

	if(objType == "relation" or objType == "*")
	{
		std::shared_ptr<class OsmData> relationObjs(new class OsmData());
		DbXapiQueryObjVisible(c, work, 
			usernames, 
			tablePrefix, 
			"relation",
			tagKey,
			tagValue,
			bbox, 
			relationObjs);

		//Get object Ids needed to complete relations
		for(size_t i=0; i<relationObjs->relations.size(); i++)
		{
			OsmRelation &rel = relationObjs->relations[i];
			for(const RelationMember &member : rel.members)
			{	
				if(member.type == ObjectType::Node)
					nodeIdsSet.insert(member.ref);
				else if(member.type == ObjectType::Way)
					wayIdsSet.insert(member.ref);
				else if(member.type == ObjectType::Relation)
					relationIdsSet.insert(member.ref);
			}
		}
		relationObjs.reset();

		//Recursively get child relations
		std::set<int64_t> pendingRelationIds = relationIdsSet;
		int depth = 0;
		while(pendingRelationIds.size() > 0 and depth < 10)
		{
			std::set<int64_t>::const_iterator it = pendingRelationIds.begin();
			std::shared_ptr<class OsmData> childRelationObjs(new class OsmData());
			while(it != pendingRelationIds.end())
				GetVisibleObjectsById(c, work, 
					usernames, 
					tablePrefix, 
					"relation",
					pendingRelationIds, it, 
					1000, childRelationObjs);

			pendingRelationIds.clear();
			for(size_t i=0; i<childRelationObjs->relations.size(); i++)
			{
				OsmRelation &rel = childRelationObjs->relations[i];
				for(const RelationMember &member : rel.members)
				{	
					if(member.type == ObjectType::Node)
						nodeIdsSet.insert(member.ref);
					else if(member.type == ObjectType::Way)
						wayIdsSet.insert(member.ref);
					else if(member.type == ObjectType::Relation)
					{
						if (relationIdsSet.find(member.ref) == relationIdsSet.end())
						{
							relationIdsSet.insert(member.ref);
							pendingRelationIds.insert(member.ref);
						}					
					}
				}
			}

			depth += 1;
		}
	}

	if(objType == "way" or objType == "*")
	{
		//Get way IDs
		std::shared_ptr<class OsmData> wayObjs(new class OsmData());
		DbXapiQueryObjVisible(c, work, 
			usernames, 
			tablePrefix, 
			"way",
			tagKey,
			tagValue,
			bbox, 
			wayObjs);

		//Get nodes to complete ways
		for(size_t i=0; i<wayObjs->ways.size(); i++)
		{
			OsmWay &way = wayObjs->ways[i];
			wayIdsSet.insert(way.objId);
			for(size_t j=0; j<way.refs.size(); j++)
			{	
				nodeIdsSet.insert(way.refs[j]);
			}
		}
	}

	if(objType == "node" or objType == "*")
	{
		std::shared_ptr<class OsmData> nodeObjs(new class OsmData());
		DbXapiQueryObjVisible(c, work, 
			usernames, 
			tablePrefix, 
			"node",
			tagKey,
			tagValue,
			bbox, 
			nodeObjs);

		if(objType == "*")
		{
			//Merge node ids with previously found data
			for(size_t i=0; i<nodeObjs->nodes.size(); i++)
			{
				OsmNode &node = nodeObjs->nodes[i];
				nodeIdsSet.insert(node.objId);
			}
		}
		else
			nodeObjs->StreamTo(*enc);
	}

	//Output final objects
	std::set<int64_t>::const_iterator it = nodeIdsSet.begin();
	while(it != nodeIdsSet.end())
		GetVisibleObjectsById(c, work, usernames,
			tablePrefix, "node", nodeIdsSet, 
			it, 1000, enc);

	it = wayIdsSet.begin();
	while(it != wayIdsSet.end())
		GetVisibleObjectsById(c, work, usernames,
			tablePrefix, "way", wayIdsSet, 
			it, 1000, enc);

	it = relationIdsSet.begin();
	while(it != relationIdsSet.end())
		GetVisibleObjectsById(c, work, usernames,
			tablePrefix, "relation", relationIdsSet, 
			it, 1000, enc);
}


// ************* Overpass queries *************

static std::string OverpassJsonPair(const std::string &key, const std::string &value)
{
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	writer.StartObject();
	writer.Key(key.c_str(), key.size(), true);
	writer.String(value.c_str(), value.size(), true);
	writer.EndObject(1);
	return buffer.GetString();
}

///A condition the tag text index can answer, true of every object whose tags
///hold the text as a key or value. It only narrows the search: the exact
///condition still has to follow. Text made of nothing the index holds, such as
///a common English word, cannot be looked up, so then it selects everything.
static std::string OverpassTextIndexSql(pqxx::transaction_base *work, const std::string &text)
{
	std::string query = "plainto_tsquery('english', " + work->quote(text) + ")";
	return "(numnode(" + query + ") = 0 OR jsonb_to_tsvector('english', tags, '\"all\"') @@ " + query + ")";
}

///The query for the objects, or with idsOnly only for their IDs and versions.
static std::string OverpassQuerySql(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix,
	const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	bool idsOnly)
{
	if(objType != "node" && objType != "way" && objType != "relation")
		throw invalid_argument("Object type must be node, way or relation");
	if(!bbox.empty() && bbox.size() != 4)
		throw invalid_argument("Bbox must have four coordinates");
	for(double value : bbox)
		if(!std::isfinite(value)) throw invalid_argument("Bbox coordinates must be finite");

	string sql = idsOnly ? "SELECT id, version" : "SELECT *";
	if(objType == "node" && !idsOnly)
		sql += ", ST_X(geom) as lon, ST_Y(geom) AS lat";
	sql += " FROM " + c.quote_name(tablePrefix+"visible"+objType+"s") + " WHERE TRUE";

	for(const OverpassTagFilter &filter : filters)
	{
		string key = work->quote(filter.key);
		string hasKey = "tags ? " + key;
		string regexOp = filter.ignoreCase ? " ~* " : " ~ ";
		string matches = "(" + hasKey + " AND (tags->>" + key + ")" + regexOp + work->quote(filter.value) + ")";
		string equals = "tags @> " + work->quote(OverpassJsonPair(filter.key, filter.value)) + "::jsonb";
		switch(filter.op)
		{
		case OverpassTagFilter::Exists:
			sql += " AND " + OverpassTextIndexSql(work, filter.key) + " AND " + hasKey;
			break;
		case OverpassTagFilter::NotExists:
			sql += " AND NOT COALESCE(" + hasKey + ", false)";
			break;
		case OverpassTagFilter::Equals:
			sql += " AND " + OverpassTextIndexSql(work, filter.key) + " AND " +
				OverpassTextIndexSql(work, filter.value) + " AND " + equals;
			break;
		case OverpassTagFilter::NotEquals:
			sql += " AND NOT COALESCE(" + equals + ", false)";
			break;
		case OverpassTagFilter::Matches:
			sql += " AND " + matches;
			break;
		case OverpassTagFilter::NotMatches:
			sql += " AND NOT COALESCE(" + matches + ", false)";
			break;
		default:
			throw invalid_argument("Unknown tag filter operation");
		}
	}

	if(bbox.size() == 4)
	{
		stringstream sql2;
		sql2.precision(9);
		sql2 << fixed << " AND " << (objType == "node" ? "geom" : "bbox")
			<< " && ST_MakeEnvelope("<<bbox[0]<<","<<bbox[1]<<","<<bbox[2]<<","<<bbox[3]<<", 4326)";
		sql += sql2.str();
	}

	if(!ids.empty())
	{
		sql += " AND id = ANY(ARRAY[";
		for(size_t i=0; i<ids.size(); i++)
		{
			if(i) sql += ",";
			sql += to_string(ids[i]);
		}
		sql += "]::bigint[])";
	}

	if(limit > 0)
		sql += " LIMIT " + to_string(limit);
	sql += ";";
	return sql;
}

///Reports a failed Overpass query as the exceptions its callers document.
static void OverpassRethrow(const pqxx::sql_error &e)
{
	string state = e.sqlstate();
	if(state == "2201B")
		throw invalid_argument(string("Invalid regular expression: ") + e.what());
	if(state == "57014")
		throw runtime_error("Query timed out");
	throw;
}

void DbOverpassQueryIdsVisible(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix,
	const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	std::vector<int64_t> &idsOut)
{
	string sql = OverpassQuerySql(c, work, tablePrefix, objType, filters, bbox, ids, limit, true);
	try
	{
		pqxx::icursorstream cursor( *work, sql, "overpasscursor", 10000 );
		while(ObjectResultsToListIdVer(cursor, &idsOut, nullptr) > 0) {}
	}
	catch(const pqxx::sql_error &e)
	{
		OverpassRethrow(e);
	}
}

void DbOverpassQueryObjVisible(pqxx::connection &c, pqxx::transaction_base *work,
	class DbUsernameLookup &usernames,
	const std::string &tablePrefix,
	const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string sql = OverpassQuerySql(c, work, tablePrefix, objType, filters, bbox, ids, limit, false);
	try
	{
		pqxx::icursorstream cursor( *work, sql, "overpasscursor", 1000 );

		int count = 1;
		if(objType == "node")
			while(count > 0)
				count = NodeResultsToEncoder(cursor, usernames, enc);
		if(objType == "way")
			while(count > 0)
				count = WayResultsToEncoder(cursor, usernames, enc);
		if(objType == "relation")
		{
			std::set<int64_t> skipIds;
			RelationResultsToEncoder(cursor, usernames, skipIds, enc);
		}
	}
	catch(const pqxx::sql_error &e)
	{
		OverpassRethrow(e);
	}
}
