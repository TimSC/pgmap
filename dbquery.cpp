#include "dbquery.h"
#include "dbdecode.h"
#include "dbfilters.h"
using namespace std;

// ************* Basic query methods ***************

std::shared_ptr<pqxx::icursorstream> VisibleNodesInBboxStart(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const std::vector<double> &bbox, 
	int64_t existsAtTimestamp,
	const string &excludeTablePrefix)
{
	if(bbox.size() != 4)
		throw invalid_argument("Bbox has wrong length");
	string vNodeTable = c.quote_name(tablePrefix + "visiblenodes");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "nodeids");

	stringstream sql;
	sql.precision(9);
	sql << "SELECT "<<vNodeTable<<".*, ST_X("<<vNodeTable<<".geom) as lon, ST_Y("<<vNodeTable<<".geom) AS lat";
	sql << " FROM ";
	sql << vNodeTable;
	if(excludeTable.size() > 0)
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<vNodeTable<<".id = "<<excludeTable<<".id";
	sql << " WHERE "<<vNodeTable<<".geom && ST_MakeEnvelope(";
	sql << bbox[0] <<","<< bbox[1] <<","<< bbox[2] <<","<< bbox[3] << ", 4326)";
	if(excludeTable.size() > 0)
		sql << " AND "<<excludeTable<<".id IS NULL";
	if(existsAtTimestamp != 0)
		sql << " AND timestamp <= " << existsAtTimestamp;
	sql <<";";

	return std::shared_ptr<pqxx::icursorstream>(new pqxx::icursorstream( *work, sql.str(), "nodesinbbox", 1000 ));
}

std::shared_ptr<pqxx::icursorstream> VisibleNodesInWktStart(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const std::string &wkt, int srid,
	const string &excludeTablePrefix)
{
	string vNodeTable = c.quote_name(tablePrefix + "visiblenodes");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "nodeids");

	stringstream sql;
	sql.precision(9);
	sql << "SELECT "<<vNodeTable<<".*, ST_X("<<vNodeTable<<".geom) as lon, ST_Y("<<vNodeTable<<".geom) AS lat";
	sql << " FROM ";
	sql << vNodeTable;
	if(excludeTable.size() > 0)
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<vNodeTable<<".id = "<<excludeTable<<".id";
	sql << " WHERE "<<vNodeTable<<".geom && ST_GeomFromText(";
	sql << c.quote(wkt) << ", "<< srid << ")";
	if(excludeTable.size() > 0)
		sql << " AND "<<excludeTable<<".id IS NULL";
	sql <<";";

	return std::shared_ptr<pqxx::icursorstream>(new pqxx::icursorstream( *work, sql.str(), "nodesinbbox", 1000 ));
}

int LiveNodesInBboxContinue(std::shared_ptr<pqxx::icursorstream> cursor, class DbUsernameLookup &usernames, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	pqxx::icursorstream *c = cursor.get();
	return NodeResultsToEncoder(*c, usernames, enc);
}

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

void GetLiveNodesById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const string &excludeTablePrefix, 
	const std::set<int64_t> &nodeIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string nodeTable = c.quote_name(tablePrefix + "livenodes");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "nodeids");

	stringstream sqlFrags;
	int count = 0;
	for(; it != nodeIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << nodeTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+ nodeTable;
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+nodeTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "nodecursor", 1000 );	

	count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, usernames, enc);
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
	int count = 0;
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

//Can this be combined into GetLiveNodesById?
void GetLiveWaysById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &wayIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = c.quote_name(tablePrefix + "liveways");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "wayids");

	stringstream sqlFrags;
	int count = 0;
	for(; it != wayIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << wayTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+ wayTable;
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+wayTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "waycursor", 1000 );	

	set<int64_t> empty;
	int records = 1;
	while(records > 0)
		records = WayResultsToEncoder(cursor, usernames, enc);
}

//Can this be combined into GetLiveNodesById?
void GetLiveRelationsById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &relationIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string relationTable = c.quote_name(tablePrefix + "liverelations");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "relationids");

	stringstream sqlFrags;
	int count = 0;
	for(; it != relationIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << relationTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+ relationTable;
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+relationTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "relationcursor", 1000 );	

	set<int64_t> empty;
	RelationResultsToEncoder(cursor, usernames, empty, enc);
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
	int count = 0;
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
	int count = 0;
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

void QueryOldNodesInBbox(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::vector<double> &bbox, 
	int64_t existsAtTimestamp,
	std::shared_ptr<IDataStreamHandler> enc)
{
	if(bbox.size() != 4)
		throw invalid_argument("Bbox has wrong length");
	string liveNodeTable = c.quote_name(tablePrefix + "oldnodes");

	stringstream sql;
	sql.precision(9);
	sql << "SELECT "<<liveNodeTable<<".*, ST_X("<<liveNodeTable<<".geom) as lon, ST_Y("<<liveNodeTable<<".geom) AS lat";
	sql << " FROM ";
	sql << liveNodeTable;
	sql << " WHERE "<<liveNodeTable<<".geom && ST_MakeEnvelope(";
	sql << bbox[0] <<","<< bbox[1] <<","<< bbox[2] <<","<< bbox[3] << ", 4326)";
	if(existsAtTimestamp != 0)
		sql << " AND timestamp <= " << existsAtTimestamp;
	sql <<";";

	pqxx::icursorstream cursor( *work, sql.str(), "oldnodesinbbox", 1000 );

	int ret = 1;
	while(ret > 0)
		ret = NodeResultsToEncoder(cursor, usernames, enc);
}

void GetWayIdVersThatContainNodes(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::set<int64_t> &nodeIds, std::set<std::pair<int64_t, int64_t> > &wayIdVersOut)
{
	string wayMemTable = c.quote_name(tablePrefix + "way_mems");
	int step = 1000;

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

		string sql = "SELECT * FROM "+wayMemTable;
		sql += " WHERE ("+sqlFrags.str()+")";
		sql += ";";

		pqxx::icursorstream cursor( *work, sql, "wayidverscontainingnodes", 1000 );	

		int records = 1;
		while (records>0)
		{
			records = 0;
			pqxx::result rows;
			cursor.get(rows);
			if ( rows.empty() )
			{
				// nothing left to read
				continue;
			}

			int idCol = rows.column_number("id");
			int verCol = rows.column_number("version");

			for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c)
			{
				int64_t objId = c[idCol].as<int64_t>();
				int64_t objVer = c[verCol].as<int64_t>();
				std::pair<int64_t, int64_t> idVer(objId, objVer);
				wayIdVersOut.insert(wayIdVersOut.begin(), idVer);
				records ++;
			}
		}
	}
}

void GetLiveWayBboxesById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &wayIds,
	std::map<int64_t, vector<double> > &out)
{
	string wayTable = c.quote_name(tablePrefix + "liveways");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "wayids");

	stringstream sqlFrags;
	int count = 0;
	for(std::set<int64_t>::const_iterator it=wayIds.begin(); it != wayIds.end(); it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << wayTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *, ";
	sql += "ST_XMin("+wayTable+".bbox) as lon1, ST_XMax("+wayTable+".bbox) as lon2, ";
	sql += "ST_YMin("+wayTable+".bbox) AS lat1, ST_YMax("+wayTable+".bbox) AS lat2";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+ wayTable;
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+wayTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
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
			double lat1 = c[lat1Col].as<double>();
			double lat2 = c[lat2Col].as<double>();
			double lon1 = c[lon1Col].as<double>();
			double lon2 = c[lon2Col].as<double>();

			std::vector<double> bbox = {lon1, lat1, lon2 , lat2}; 
			out[objId] = bbox;
		}
	}
}


