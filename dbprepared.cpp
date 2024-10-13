#include "dbprepared.h"
#include "dbcommon.h"
#include <iostream>
#include <map>
using namespace std;

std::map<std::string, std::string> keyToSql;

void prepare_deduplicated(pqxx::connection &c, std::string key, std::string sql)
{
	//cout << "prepare " << key << " " << sql << endl;
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

