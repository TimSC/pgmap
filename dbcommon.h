#ifndef _DB_COMMON_H
#define _DB_COMMON_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include <string>
#include <vector>

///Tolerate NULL values when we copy row from live to old table
template<class T> void BindVal(pqxx::prepare::invocation &invoc, const pqxx::result::field &field)
{
	if(field.is_null())
		invoc();
	else
		invoc(field.as<T>());
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

void DbCheckOcdnSupport(pqxx::connection &c, pqxx::transaction_base *work,
	bool &ocdnSupported, std::string &ocdn);

class PgWork
{
public:
	PgWork();
	PgWork(pqxx::transaction_base *workIn);
	PgWork(const PgWork &obj);
	virtual ~PgWork();
	PgWork& operator=(const PgWork &obj);

	std::shared_ptr<pqxx::transaction_base> work;
};

bool LockMap(std::shared_ptr<pqxx::transaction_base> work, const std::string &prefix, 
	const std::string &accessMode, std::string &errStr);

#endif //_DB_COMMON_H

