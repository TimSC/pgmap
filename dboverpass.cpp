
#include "dboverpass.h"
#include "dbdecode.h"

/*
Example queries :

SELECT id, tags FROM planet_static_liveways WHERE tags @> '{"name":"Portsmouth"}'::jsonb  LIMIT 10;

SELECT id, tags FROM planet_static_liveways WHERE tags ? 'highway' LIMIT 10;

SELECT id, tags FROM planet_static_livenodes WHERE tags @> '{"natural":"peak"}'::jsonb AND geom && ST_MakeEnvelope(-1.146698,50.7338476,-0.9338379,50.8744446, 4326) LIMIT 100;

*/

void DbXapiQueryVisible(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox, 
	std::shared_ptr<IDataStreamHandler> enc)
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
			if(tagKey.size() > 0)
			{
				//TODO escape inputs!!
				sql += " tags @> '{\""+tagKey+"\":\""+tagValue+"\"}'::jsonb";
			}
			else
			{
				//TODO escape inputs!!
				sql += " tags ? '"+tagKey+"'";
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

