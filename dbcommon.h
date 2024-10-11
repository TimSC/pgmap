#ifndef _DB_COMMON_H
#define _DB_COMMON_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include <string>
#include <vector>

#if PQXX_VERSION_MAJOR >= 6
#define pqxxfield pqxx::field
#else
#define pqxxfield pqxx::result::field
#endif

///Tolerate NULL values when we copy row from live to old table
template<class T> void BindVal(pqxx::params &invoc, const pqxxfield &field)
{
	if(field.is_null())
		invoc.append();
	else
		invoc.append(field.as<T>());
}

bool DbExec(pqxx::transaction_base *work, const std::string& sql, std::string &errStr, size_t *rowsAffected = nullptr, int verbose = 0);

void DbGetPrimaryKeyCols(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tableName, std::vector<std::string> &colsOut);

size_t DbCountPrimaryKeyCols(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tableName);

bool DbCheckIndexExists(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &indexName);

bool DbCheckTableExists(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tableName);

void DbGetVersion(pqxx::connection &c, pqxx::transaction_base *work, int &majorVerOut, int &minorVerOut);

std::string md5sum(const char *buffer, unsigned len);

std::string md5sum(const std::string &buffer);

#endif //_DB_COMMON_H

