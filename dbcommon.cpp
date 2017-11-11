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

