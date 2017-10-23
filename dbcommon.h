#ifndef _DB_COMMON_H
#define _DB_COMMON_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include <string>

///Tolerate NULL values when we copy row from live to old table
template<class T> void BindVal(pqxx::prepare::invocation &invoc, const pqxx::result::field &field)
{
	if(field.is_null())
		invoc();
	else
		invoc(field.as<T>());
}

bool DbExec(pqxx::work *work, const std::string& sql, std::string &errStr);

#endif //_DB_COMMON_H

