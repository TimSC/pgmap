#include "dbquery.h"
#include "dbdecode.h"
#include "dbfilters.h"
using namespace std;

// ************* Basic query methods ***************

void GetLiveWaysThatContainNodes(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const string &excludeTablePrefix,
	const std::set<int64_t> &nodeIds, std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = c.quote_name(tablePrefix + "liveways");
	string wayMemTable = c.quote_name(tablePrefix + "way_mems");
	int step = 1000;
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "wayids");

	auto it=nodeIds.begin();
	while(it != nodeIds.end())
	{
		stringstream sqlFrags;
		int count = 0;
		for(; it != nodeIds.end() && count < step; it++)
		{
			if(count >= 1)
				sqlFrags << " OR ";
			sqlFrags << wayMemTable << ".member = " << *it;
			count ++;
		}

		string sql = "SELECT "+wayTable+".*";
		if(excludeTable.size() > 0)
			sql += ", "+excludeTable+".id";

		sql += " FROM "+wayMemTable+" INNER JOIN "+wayTable+" ON "\
			+wayMemTable+".id = "+wayTable+".id AND "+wayMemTable+".version = "+wayTable+".version";
		if(excludeTable.size() > 0)
			sql += " LEFT JOIN "+excludeTable+" ON "+wayTable+".id = "+excludeTable+".id";

		sql += " WHERE ("+sqlFrags.str()+")";
		if(excludeTable.size() > 0)
			sql += " AND "+excludeTable+".id IS NULL";
		sql += ";";

		pqxx::icursorstream cursor( *work, sql, "wayscontainingnodes", 1000 );	

		int records = 1;
		std::shared_ptr<FilterObjectsUnique> encUnique = make_shared<FilterObjectsUnique>(enc);
		while (records>0)
			records = WayResultsToEncoder(cursor, usernames, encUnique);
	}
}

void GetLiveRelationsForObjects(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	char qtype, const set<int64_t> &qids, 
	set<int64_t>::const_iterator &it, size_t step,
	const set<int64_t> &skipIds, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	string relTable = c.quote_name(tablePrefix + "liverelations");
	string relMemTable = c.quote_name(tablePrefix + "relation_mems_" + qtype);
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "relationids");

	stringstream sqlFrags;
	size_t count = 0;
	for(; it != qids.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << relMemTable << ".member = " << *it;
		count ++;
	}

	string sql = "SELECT "+relTable+".*";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+relMemTable+" INNER JOIN "+relTable+" ON "+relMemTable+".id = "+relTable+".id AND "+relMemTable+".version = "+relTable+".version";
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+relTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "relationscontainingobjects", 1000 );	

	std::shared_ptr<FilterObjectsUnique> encUnique = make_shared<FilterObjectsUnique>(enc);
	RelationResultsToEncoder(cursor, usernames, skipIds, encUnique);
}

void GetVisibleObjectsById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::string &objType,
	const std::set<int64_t> &objIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string nodeTable = c.quote_name(tablePrefix + "visible" +objType+ "s");

	stringstream sqlFrags;
	size_t count = 0;
	for(; it != objIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << nodeTable << ".id = " << *it;
		count ++;
	}
	if(count == 0) return;

	std::string sql = "SELECT *";
	if(objType == "node")
		sql += ", ST_X(geom) as lon, ST_Y(geom) AS lat";

	sql += " FROM "+ nodeTable;
	sql += " WHERE ("+sqlFrags.str()+")";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "objcursor", 1000 );	

	count = 1;
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

void DbGetObjectsByIdVer(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::string &objType, const std::string &liveOrOld,
	const std::set<std::pair<int64_t, int64_t> > &objIdVers, std::set<std::pair<int64_t, int64_t> >::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string objTable = c.quote_name(tablePrefix+liveOrOld+objType+"s");

	stringstream sqlFrags;
	size_t count = 0;
	if(objType == "relation") //Relations are dumped in one shot
		step = 0;
	for(; it != objIdVers.end() && (count < step || step == 0); it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << "(" << objTable << ".id = " << it->first << " AND " << objTable << ".version = " << it->second << ")";
		count ++;
	}

	string sql = "SELECT *";
	if(objType == "node")
		sql += ", ST_X(geom) as lon, ST_Y(geom) AS lat";
	sql += " FROM "+ objTable;
	sql += " WHERE "+sqlFrags.str();
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "objectidvercursor", 1000 );	

	count = 1;
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

void DbGetObjectsHistoryById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::string &objType, const std::string &liveOrOld,
	const std::set<int64_t> &objIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string objTable = c.quote_name(tablePrefix+liveOrOld+objType+"s");

	stringstream sqlFrags;
	size_t count = 0;
	if(objType == "relation") //Relations are dumped in one shot
		step = 0;
	for(; it != objIds.end() && (count < step || step == 0); it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << objTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *";
	if(objType == "node")
		sql += ", ST_X(geom) as lon, ST_Y(geom) AS lat";
	sql += " FROM "+ objTable;
	sql += " WHERE "+sqlFrags.str();
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "objecthistorycursor", 1000 );	

	count = 1;
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

void GetVisibleObjectBboxesById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix,
	const string &objType, 
	const std::set<int64_t> &wayIds,
	std::map<int64_t, vector<double> > &out)
{
	if(wayIds.size() == 0)
		return;
	string objTable = c.quote_name(tablePrefix+"visible"+objType+"s");

	stringstream sqlFrags;
	int count = 0;
	for(std::set<int64_t>::const_iterator it=wayIds.begin(); it != wayIds.end(); it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << objTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *, ";
	if(objType == "node")
	{
		sql += "ST_XMin("+objTable+".geom) AS lon1, ST_XMax("+objTable+".geom) AS lon2, ";
		sql += "ST_YMin("+objTable+".geom) AS lat1, ST_YMax("+objTable+".geom) AS lat2";
	}
	else
	{
		sql += "ST_XMin("+objTable+".bbox) AS lon1, ST_XMax("+objTable+".bbox) AS lon2, ";
		sql += "ST_YMin("+objTable+".bbox) AS lat1, ST_YMax("+objTable+".bbox) AS lat2";
	}

	sql += " FROM "+ objTable;

	sql += " WHERE ("+sqlFrags.str()+")";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "waycursor", 1000 );	

	pqxx::result rows;
	while (true)
	{
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read

		int idCol = rows.column_number("id");
		int lat1Col = rows.column_number("lat1");
		int lat2Col = rows.column_number("lat2");
		int lon1Col = rows.column_number("lon1");
		int lon2Col = rows.column_number("lon2");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) 
		{
			int64_t objId = c[idCol].as<int64_t>();
			if(c[lat1Col].is_null())
			{
				std::vector<double> empty;
				out[objId] = empty;
				continue;
			}

			double lat1 = c[lat1Col].as<double>();
			double lat2 = c[lat2Col].as<double>();
			double lon1 = c[lon1Col].as<double>();
			double lon2 = c[lon2Col].as<double>();

			std::vector<double> bbox = {lon1, lat1, lon2 , lat2}; 
			out[objId] = bbox;
		}
	}
}



