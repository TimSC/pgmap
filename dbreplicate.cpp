#include "dbreplicate.h"
#include "dbdecode.h"
#include <set>
using namespace std;

void GetReplicateDiffNodes(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames,
	const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	class OsmChange &out)
{
	string nodeTable = c.quote_name(tablePrefix + "livenodes");
	if(selectOld)
		nodeTable = c.quote_name(tablePrefix + "oldnodes");

	//Discourage sequential scans of tables, since they is not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT "<< nodeTable << ".*, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << nodeTable;
	sql << " WHERE timestamp > " << timestampStart << " AND timestamp <= " << timestampEnd;
	sql << " ORDER BY " << nodeTable << ".timestamp;";

	pqxx::icursorstream cursor( *work, sql.str(), "nodediff", 1000 );	

	std::shared_ptr<class OsmData> data = make_shared<class OsmData>();
	int count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, usernames, data);

	for(size_t i=0; i < data->nodes.size(); i++)
	{
		const class OsmObject &obj = data->nodes[i];
		out.StoreOsmData(&obj, false);
	}
}

void GetReplicateDiffWays(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames,
	const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	class OsmChange &out)
{
	string wayTable = c.quote_name(tablePrefix + "liveways");
	if(selectOld)
		wayTable = c.quote_name(tablePrefix + "oldways");

	//Discourage sequential scans of tables, since they is not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << wayTable << ".*";
	sql << ", ST_XMin(bbox) AS bbox_xmin, ST_XMax(bbox) AS bbox_xmax, ST_YMin(bbox) AS bbox_ymin, ST_YMax(bbox) AS bbox_ymax";
	sql << " FROM ";
	sql << wayTable;
	sql << " WHERE timestamp > " << timestampStart << " AND timestamp <= " << timestampEnd;
	sql << " ORDER BY " << wayTable << ".timestamp;";

	pqxx::icursorstream cursor( *work, sql.str(), "waydiff", 1000 );	

	std::shared_ptr<class OsmData> data = make_shared<class OsmData>();
	int count = 1;
	while (count > 0)
		count = WayResultsToEncoder(cursor, usernames, data);

	for(size_t i=0; i < data->ways.size(); i++)
	{
		const class OsmObject &obj = data->ways[i];
		out.StoreOsmData(&obj, false);
	}
}

void GetReplicateDiffRelations(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames,
	const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	class OsmChange &out)
{
	string relationTable = c.quote_name(tablePrefix + "liverelations");
	if(selectOld)
		relationTable = c.quote_name(tablePrefix + "oldrelations");

	//Discourage sequential scans of tables, since they is not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << relationTable << ".*";
	sql << ", ST_XMin(bbox) AS bbox_xmin, ST_XMax(bbox) AS bbox_xmax, ST_YMin(bbox) AS bbox_ymin, ST_YMax(bbox) AS bbox_ymax";
	sql << " FROM ";
	sql << relationTable;
	sql << " WHERE timestamp > " << timestampStart << " AND timestamp <= " << timestampEnd;
	sql << " ORDER BY " << relationTable << ".timestamp;";

	pqxx::icursorstream cursor( *work, sql.str(), "relationdiff", 1000 );	

	std::shared_ptr<class OsmData> data = make_shared<class OsmData>();
	set<int64_t> empty;
	RelationResultsToEncoder(cursor, usernames, empty, data);

	for(size_t i=0; i < data->relations.size(); i++)
	{
		const class OsmObject &obj = data->relations[i];
		out.StoreOsmData(&obj, false);
	}
}

