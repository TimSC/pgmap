#include "dbdump.h"
#include "dbdecode.h"

/**
* Dump live nodes but skip node IDs that are in the excludeTablePrefix table. Only current
* nodes are dumped, not old (non-visible) nodes.
*/
void DumpNodes(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const string &excludeTablePrefix,
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string liveNodeTable = c.quote_name(tablePrefix + "livenodes");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "nodeids");

	//Discourage sequential scans of tables, since they are not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	/*
		                                                    QUERY PLAN                                                        
	--------------------------------------------------------------------------------------------------------------------------
	 Merge Anti Join  (cost=1.01..50174008.28 rows=1365191502 width=8)
	   Merge Cond: (planet_livenodes.id = planet_mod_nodeids.id)
	   ->  Index Only Scan using planet_livenodes_pkey on planet_livenodes  (cost=0.58..35528317.86 rows=1368180352 width=8)
	   ->  Index Only Scan using planet_mod_nodeids_pkey on planet_mod_nodeids  (cost=0.43..11707757.01 rows=2988850 width=8)
	(4 rows)
	*/

	stringstream sql;
	sql << "SELECT "<< liveNodeTable << ".*, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << liveNodeTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<liveNodeTable<<".id = "<<excludeTable<<".id";
		sql << " WHERE "<<excludeTable<<".id IS NULL";
	}
	if(order)
		sql << " ORDER BY " << liveNodeTable << ".id";
	sql << ";";
	//cout << sql.str() << endl;

	pqxx::icursorstream cursor( *work, sql.str(), "nodecursor", 1000 );	

	int count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, enc);
}

void DumpWays(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const string &excludeTablePrefix,
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = c.quote_name(tablePrefix + "liveways");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "wayids");

	//Discourage sequential scans of tables, since they are not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << wayTable << ".* FROM ";
	sql << wayTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<wayTable<<".id = "<<excludeTable<<".id";
		sql << " WHERE "<<excludeTable<<".id IS NULL";
	}
	if(order)
		sql << " ORDER BY " << wayTable << ".id";
	sql << ";";

	pqxx::icursorstream cursor( *work, sql.str(), "waycursor", 1000 );	

	int count = 1;
	while (count > 0)
		count = WayResultsToEncoder(cursor, enc);
}

void DumpRelations(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const string &excludeTablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string relationTable = c.quote_name(tablePrefix + "liverelations");
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = c.quote_name(excludeTablePrefix + "relationids");

	//Discourage sequential scans of tables, since they are not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << relationTable << ".* FROM ";
	sql << relationTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<relationTable<<".id = "<<excludeTable<<".id";
		sql << " WHERE "<<excludeTable<<".id IS NULL";
	}
	if(order)
		sql << " ORDER BY " << relationTable << ".id";
	sql << ";";

	pqxx::icursorstream cursor( *work, sql.str(), "relationcursor", 1000 );	

	set<int64_t> empty;
	RelationResultsToEncoder(cursor, empty, enc);
}

