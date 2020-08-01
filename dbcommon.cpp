#include "dbcommon.h"
#include <iostream>
using namespace std;

#if PQXX_VERSION_MAJOR >= 6
#define pqxxrow pqxx::row
#else
#define pqxxrow pqxx::result::tuple
#endif 

bool DbExec(pqxx::transaction_base *work, const string& sql, string &errStr, size_t *rowsAffected, int verbose)
{
	pqxx::result r;
	try
	{
		if(verbose >= 1)
			cout << sql << endl;
		r = work->exec(sql);
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = e.what();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}
	if(rowsAffected != nullptr)
		*rowsAffected = r.affected_rows();
	return true;
}

void DbGetPrimaryKeyCols(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tableName, std::vector<std::string> &colsOut)
{
	//http://technosophos.com/2015/10/26/querying-postgresql-to-find-the-primary-key-of-a-table.html

	string sql = "SELECT c.column_name FROM information_schema.key_column_usage AS c";
	sql += " LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name";
	sql += " WHERE t.table_name = "+c.quote(tableName)+" AND t.constraint_type = 'PRIMARY KEY';";

	pqxx::result r = work->exec(sql);
	int colNameCol = r.column_number("column_name");
	for (unsigned int rownum=0; rownum < r.size(); ++rownum)
	{
		const pqxxrow row = r[rownum];
		colsOut.push_back(row[colNameCol].as<string>());
	}
}

size_t DbCountPrimaryKeyCols(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tableName)
{
	std::vector<std::string> cols;
	DbGetPrimaryKeyCols(c, work, tableName, cols);
	return cols.size();
}

bool DbCheckIndexExists(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &indexName)
{
	//Based on http://blog.timmattison.com/archives/2014/09/02/checking-postgresql-to-see-if-an-index-already-exists/

	string sql = "SELECT c.relname FROM pg_class c JOIN pg_namespace n";
	sql += " ON n.oid = c.relnamespace WHERE n.nspname = 'public'";
	sql += " AND c.relkind='i' AND c.relname="+c.quote(indexName)+";";

	pqxx::result r = work->exec(sql);
	return r.size() > 0;
}

bool DbCheckTableExists(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tableName)
{
	// https://stackoverflow.com/a/24089729/4288232
	string sql = "SELECT EXISTS (SELECT 1";
	sql += " FROM   information_schema.tables";
	sql += " WHERE  table_schema = 'public'";
	sql += " AND    table_name = "+c.quote(tableName)+");";

	pqxx::result r = work->exec(sql);
	return r.size() > 0;
}

void DbGetVersion(pqxx::connection &c, pqxx::transaction_base *work, int &majorVerOut, int &minorVerOut)
{
	string sql = "SELECT current_setting('server_version_num');";
	pqxx::result r = work->exec(sql);
	int ver = r[0][0].as<int>();
	majorVerOut = ver / 10000;
	minorVerOut = (ver / 100) % 100;
}

