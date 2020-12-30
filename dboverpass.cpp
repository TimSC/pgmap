
#include "dboverpass.h"
#include "dbdecode.h"
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h> //rapidjson-dev
#include "dbquery.h"

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

				sql += " tags @> "+work->quote(buffer.GetString())+"::jsonb";
			}
			else
			{
				sql += " tags ? "+work->quote(tagKey);
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

	cout << sql << endl;

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

	cout << sql << endl;

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
	if(objType == "relation")
	{
		DbXapiQueryObjVisible(c, work, 
			usernames, 
			tablePrefix, 
			objType,
			tagKey,
			tagValue,
			bbox, 
			enc);
	}
	if(objType == "way")
	{
		//Get way IDs
		std::shared_ptr<class OsmData> wayObjs(new class OsmData());
		DbXapiQueryObjVisible(c, work, 
			usernames, 
			tablePrefix, 
			objType,
			tagKey,
			tagValue,
			bbox, 
			wayObjs);

		//Get nodes to complete ways
		std::set<int64_t> nodeIdsSet;
		for(size_t i=0; i<wayObjs->ways.size(); i++)
		{
			OsmWay &way = wayObjs->ways[i];
			for(size_t j=0; j<way.refs.size(); j++)
			{	
				nodeIdsSet.insert(way.refs[j]);
			}
		}

		std::set<int64_t>::const_iterator it = nodeIdsSet.begin();
		while(it != nodeIdsSet.end())
			GetVisibleObjectsById(c, work, usernames,
				tablePrefix, "node", nodeIdsSet, 
				it, 1000, enc);

		//Output ways
		wayObjs->StreamTo(*enc);

	}
	if(objType == "node")
	{
		DbXapiQueryObjVisible(c, work, 
			usernames, 
			tablePrefix, 
			objType,
			tagKey,
			tagValue,
			bbox, 
			enc);
	}
}

