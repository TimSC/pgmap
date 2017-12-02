#include "dbcommon.h"
#include <iostream>
using namespace std;

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
		const pqxx::result::tuple row = r[rownum];
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
