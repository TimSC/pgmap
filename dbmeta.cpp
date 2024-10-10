#include "dbmeta.h"
#include <stdexcept>
#include <iostream>
using namespace std;

#if PQXX_VERSION_MAJOR >= 6
#define pqxxrow pqxx::row
#else
#define pqxxrow pqxx::result::tuple
#endif 

std::string DbGetMetaValue(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &key, 
	const std::string &tablePrefix, 
	std::string &errStr)
{
	string metaTable = c.quote_name(tablePrefix + "meta");

	stringstream sql;
	sql << "SELECT value FROM " << metaTable << " WHERE key = $1";

	string prepkey = metaTable+"get"+key;
	c.prepare(prepkey, sql.str());

	pqxx::result r = work->exec_prepared(prepkey, key);
	int valueCol = r.column_number("value");	
	for (unsigned int rownum=0; rownum < r.size(); ++rownum)
	{
		const pqxxrow row = r[rownum];
		return row[valueCol].as<string>();
	}

	throw runtime_error("Key not found");
	return "";
}

bool DbSetMetaValue(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &key, 
	const std::string &value, 
	const std::string &tablePrefix, 
	std::string &errStr)
{
	string metaTable = c.quote_name(tablePrefix + "meta");

	stringstream sql;
	sql << "UPDATE "<< metaTable << " SET value=$2";
	sql << " WHERE key = $1;";

	string prepkey = metaTable+"update"+key;
	c.prepare(prepkey, sql.str());

	pqxx::result r = work->exec_prepared(prepkey, key, value);

	if(r.affected_rows() == 0)
	{
		stringstream sql2;
		sql2 << "INSERT INTO "<< metaTable << " (key, value) VALUES ($1, $2);";

		string prepkey2 = metaTable+"insert"+key;
		c.prepare(prepkey2, sql2.str());

		pqxx::result r = work->exec_prepared(prepkey2, key, value);
		if(r.affected_rows() != 1)
			throw runtime_error("Setting metadata should have updated a single row");
	}

	return true;
}

