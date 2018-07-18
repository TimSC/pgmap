#include "dbquery.h"
#include "dbdecode.h"
using namespace std;

// ************* Basic query methods ***************

std::shared_ptr<pqxx::icursorstream> LiveNodesInBboxStart(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const std::vector<double> &bbox, 
	int64_t existsAtTimestamp,
	const string &excludeTablePrefix)
{
	if(bbox.size() != 4)
		throw invalid_argument("Bbox has wrong length");
	string liveNodeTable = c.quote_name(tablePrefix + "livenodes");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "nodeids");

	stringstream sql;
	sql.precision(9);
	sql << "SELECT "<<liveNodeTable<<".*, ST_X("<<liveNodeTable<<".geom) as lon, ST_Y("<<liveNodeTable<<".geom) AS lat";
	sql << " FROM ";
	sql << liveNodeTable;
	if(excludeTable.size() > 0)
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<liveNodeTable<<".id = "<<excludeTable<<".id";
	sql << " WHERE "<<liveNodeTable<<".geom && ST_MakeEnvelope(";
	sql << bbox[0] <<","<< bbox[1] <<","<< bbox[2] <<","<< bbox[3] << ", 4326)";
	if(excludeTable.size() > 0)
		sql << " AND "<<excludeTable<<".id IS NULL";
	if(existsAtTimestamp != 0)
		sql << " AND timestamp <= " << existsAtTimestamp;
	sql <<";";

	return std::shared_ptr<pqxx::icursorstream>(new pqxx::icursorstream( *work, sql.str(), "nodesinbbox", 1000 ));
}

std::shared_ptr<pqxx::icursorstream> LiveNodesInWktStart(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const std::string &wkt, int srid,
	const string &excludeTablePrefix)
{
	string liveNodeTable = c.quote_name(tablePrefix + "livenodes");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "nodeids");

	stringstream sql;
	sql.precision(9);
	sql << "SELECT "<<liveNodeTable<<".*, ST_X("<<liveNodeTable<<".geom) as lon, ST_Y("<<liveNodeTable<<".geom) AS lat";
	sql << " FROM ";
	sql << liveNodeTable;
	if(excludeTable.size() > 0)
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<liveNodeTable<<".id = "<<excludeTable<<".id";
	sql << " WHERE "<<liveNodeTable<<".geom && ST_GeomFromText(";
	sql << c.quote(wkt) << ", "<< srid << ")";
	if(excludeTable.size() > 0)
		sql << " AND "<<excludeTable<<".id IS NULL";
	sql <<";";

	return std::shared_ptr<pqxx::icursorstream>(new pqxx::icursorstream( *work, sql.str(), "nodesinbbox", 1000 ));
}

int LiveNodesInBboxContinue(std::shared_ptr<pqxx::icursorstream> cursor, std::shared_ptr<IDataStreamHandler> enc)
{
	pqxx::icursorstream *c = cursor.get();
	return NodeResultsToEncoder(*c, enc);
}

void GetLiveWaysThatContainNodes(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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
		while (records>0)
			records = WayResultsToEncoder(cursor, enc);
	}
}

void GetLiveNodesById(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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
		count = NodeResultsToEncoder(cursor, enc);
}

void GetLiveRelationsForObjects(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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

	RelationResultsToEncoder(cursor, skipIds, enc);
}

//Can this be combined into GetLiveNodesById?
void GetLiveWaysById(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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
		records = WayResultsToEncoder(cursor, enc);
}

//Can this be combined into GetLiveNodesById?
void GetLiveRelationsById(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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
	RelationResultsToEncoder(cursor, empty, enc);
}

void DbGetObjectsById(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &type, const std::set<int64_t> &objectIds, 
	const std::string &activeTablePrefix, 
	const std::string &staticTablePrefix, 
	std::shared_ptr<IDataStreamHandler> out)
{
	if(type == "node")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveNodesById(c, work, staticTablePrefix, activeTablePrefix, objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveNodesById(c, work, activeTablePrefix, "", objectIds, 
				it, 1000, out);
	}
	else if(type == "way")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveWaysById(c, work, staticTablePrefix, activeTablePrefix, objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveWaysById(c, work, activeTablePrefix, "", objectIds, 
				it, 1000, out);
	}
	else if(type == "relation")
	{
		std::set<int64_t>::const_iterator it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveRelationsById(c, work, staticTablePrefix, activeTablePrefix,
				objectIds, 
				it, 1000, out);
		it = objectIds.begin();
		while(it != objectIds.end())
			GetLiveRelationsById(c, work, activeTablePrefix, "",
				objectIds, 
				it, 1000, out);
	}
	else
		throw invalid_argument("Known object type");
}

void DbGetObjectsByIdVer(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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
			count = NodeResultsToEncoder(cursor, enc);
	if(objType == "way") 
		while(count > 0)
			count = WayResultsToEncoder(cursor, enc);
	if(objType == "relation") 
	{
		std::set<int64_t> skipIds;
		RelationResultsToEncoder(cursor, skipIds, enc);
	}
}

void DbGetObjectsHistoryById(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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
			count = NodeResultsToEncoder(cursor, enc);
	if(objType == "way") 
		while(count > 0)
			count = WayResultsToEncoder(cursor, enc);
	if(objType == "relation") 
	{
		std::set<int64_t> skipIds;
		RelationResultsToEncoder(cursor, skipIds, enc);
	}
}

void QueryOldNodesInBbox(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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
		ret = NodeResultsToEncoder(cursor, enc);
}

void GetWayIdVersThatContainNodes(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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

void DbGetFullObjectById(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &type, int64_t objectId, 
	const std::string &activeTablePrefix, 
	const std::string &staticTablePrefix, 
	std::shared_ptr<class OsmData> outData)
{
	std::set<int64_t> objectIds;
	objectIds.insert(objectId);
	DbGetObjectsById(c, work, type, objectIds, 
		activeTablePrefix,
		staticTablePrefix,
		outData);

	//Get members of main object
	if(type == "way")
	{
		if (outData->ways.size() != 1)
			throw runtime_error("Unexpected number of objects in intermediate result");
		class OsmWay &mainWay = outData->ways[0];

		std::set<int64_t> memberNodes;
		for(int64_t i=0; i<mainWay.refs.size(); i++)
			memberNodes.insert(mainWay.refs[i]);
		DbGetObjectsById(c, work, "node", memberNodes, 
			activeTablePrefix,
			staticTablePrefix,
			outData);
 	}
	else if(type == "relation")
	{
		if (outData->relations.size() != 1)
			throw runtime_error("Unexpected number of objects in intermediate result");
		class OsmRelation &mainRelation = outData->relations[0];

		std::set<int64_t> memberNodes, memberWays, memberRelations;
		for(int64_t i=0; i<mainRelation.refIds.size(); i++)
		{
			if(mainRelation.refTypeStrs[i]=="node")
				memberNodes.insert(mainRelation.refIds[i]);
			else if(mainRelation.refTypeStrs[i]=="way")
				memberWays.insert(mainRelation.refIds[i]);
			else if(mainRelation.refTypeStrs[i]=="relation")
				memberRelations.insert(mainRelation.refIds[i]);
		}

		std::shared_ptr<class OsmData> memberWayObjs(new class OsmData());
		DbGetObjectsById(c, work, "node", memberNodes, 
			activeTablePrefix,
			staticTablePrefix,
			outData);
		DbGetObjectsById(c, work, "way", memberWays, 
			activeTablePrefix,
			staticTablePrefix,
			memberWayObjs);
		DbGetObjectsById(c, work, "relation", memberRelations, 
			activeTablePrefix,
			staticTablePrefix,
			outData);
		memberWayObjs->StreamTo(*outData.get());

		std::set<int64_t> memberNodes2;
		for(int64_t i=0; i<memberWayObjs->ways.size(); i++)
			for(int64_t j=0; j<memberWayObjs->ways[i].refs.size(); j++)
				memberNodes2.insert(memberWayObjs->ways[i].refs[j]);
		DbGetObjectsById(c, work, "node", memberNodes2, 
			activeTablePrefix,
			staticTablePrefix,
			outData);
	}
	else
		throw invalid_argument("Known object type");
}
