#include "dbdump.h"
#include "dbdecode.h"

/**
* Dump visible nodes. Only current
* nodes are dumped, not old (non-visible) nodes.
*/
void DumpNodes(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string vNodeTable = c.quote_name(tablePrefix + "visiblenodes");
	string excludeTable;

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
	sql << "SELECT "<< vNodeTable << ".*, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << vNodeTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<vNodeTable<<".id = "<<excludeTable<<".id";
		sql << " WHERE "<<excludeTable<<".id IS NULL";
	}
	if(order)
		sql << " ORDER BY " << vNodeTable << ".id";
	sql << ";";
	//cout << sql.str() << endl;

	pqxx::icursorstream cursor( *work, sql.str(), "nodecursor", 1000 );	

	int count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, usernames, enc);
}

void DumpWays(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = c.quote_name(tablePrefix + "visibleways");
	string excludeTable;

	//Discourage sequential scans of tables, since they are not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << wayTable << ".*, ST_XMin(" << wayTable << ".bbox) AS bbox_xmin, ST_XMax(" << wayTable << ".bbox) AS bbox_xmax, ST_YMin(" << wayTable << ".bbox) AS bbox_ymin, ST_YMax(" << wayTable << ".bbox) AS bbox_ymax FROM ";
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
		count = WayResultsToEncoder(cursor, usernames, enc);
}

void DumpRelations(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const string &tablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string relationTable = c.quote_name(tablePrefix + "visiblerelations");
	string excludeTable;

	//Discourage sequential scans of tables, since they are not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << relationTable << ".*, ST_XMin(" << relationTable << ".bbox) AS bbox_xmin, ST_XMax(" << relationTable << ".bbox) AS bbox_xmax, ST_YMin(" << relationTable << ".bbox) AS bbox_ymin, ST_YMax(" << relationTable << ".bbox) AS bbox_ymax FROM ";
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
	RelationResultsToEncoder(cursor, usernames, empty, enc);
}

