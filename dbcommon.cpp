#include "dbcommon.h"
using namespace std;

bool DbExec(pqxx::work *work, const string& sql, string &errStr)
{
	try
	{
		work->exec(sql);
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
	return true;
}

