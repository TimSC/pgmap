#include "dbcommon.h"
using namespace std;

bool DbExec(pqxx::work *work, const string& sql, string &errStr, size_t *rowsAffected)
{
	pqxx::result r;
	try
	{
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

