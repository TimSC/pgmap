#include "dbprepared.h"
#include "dbcommon.h"
#include <iostream>
#include <map>
#include <mutex>
using namespace std;

static std::map<const pqxx::connection *, std::map<std::string, std::string> > connectionKeyToSql;
static std::mutex keyToSqlMutex;

void prepare_deduplicated(pqxx::connection &c, std::string key, std::string sql)
{
	//cout << "prepare " << key << " " << sql << endl;
	std::lock_guard<std::mutex> lock(keyToSqlMutex);
	auto &keyToSql = connectionKeyToSql[&c];
	auto existing = keyToSql.find(key);
	if (existing != keyToSql.end())
	{
		if (existing->second != sql)
			throw runtime_error("SQL statement in prepared statement has changed");
		return;
	}

	c.prepare(key, sql);

	keyToSql[key] = sql;
}

void forget_prepared(pqxx::connection &c)
{
	std::lock_guard<std::mutex> lock(keyToSqlMutex);
	connectionKeyToSql.erase(&c);
}

