#include "dbreplicate.h"
#include "dbdecode.h"
#include <set>
using namespace std;

void GetReplicateDiffNodes(pqxx::connection &c, pqxx::work *work, const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	std::shared_ptr<IDataStreamHandler> enc)
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

	int count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, enc);
}

void GetReplicateDiffWays(pqxx::connection &c, pqxx::work *work, const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = c.quote_name(tablePrefix + "liveways");
	if(selectOld)
		wayTable = c.quote_name(tablePrefix + "oldways");

	//Discourage sequential scans of tables, since they is not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << wayTable << ".* FROM ";
	sql << wayTable;
	sql << " WHERE timestamp > " << timestampStart << " AND timestamp <= " << timestampEnd;
	sql << " ORDER BY " << wayTable << ".timestamp;";

	pqxx::icursorstream cursor( *work, sql.str(), "waydiff", 1000 );	

	int count = 1;
	while (count > 0)
		count = WayResultsToEncoder(cursor, enc);
}

void GetReplicateDiffRelations(pqxx::connection &c, pqxx::work *work, const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string relationTable = c.quote_name(tablePrefix + "liverelations");
	if(selectOld)
		relationTable = c.quote_name(tablePrefix + "oldrelations");

	//Discourage sequential scans of tables, since they is not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << relationTable << ".* FROM ";
	sql << relationTable;
	sql << " WHERE timestamp > " << timestampStart << " AND timestamp <= " << timestampEnd;
	sql << " ORDER BY " << relationTable << ".timestamp;";

	pqxx::icursorstream cursor( *work, sql.str(), "relationdiff", 1000 );	

	set<int64_t> empty;
	RelationResultsToEncoder(cursor, empty, enc);
}

