#include "dbmeta.h"
#include "dbprepared.h"
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
	prepare_deduplicated(c, prepkey, sql.str());

#if PQXX_VERSION_MAJOR >= 7
	pqxx::result r = work->exec_prepared(prepkey, key);
#else
	pqxx::result r = work->prepared(prepkey)(key).exec();
#endif
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
	prepare_deduplicated(c, prepkey, sql.str());

#if PQXX_VERSION_MAJOR >= 7
	pqxx::result r = work->exec_prepared(prepkey, key, value);
#else
	pqxx::result r = work->prepared(prepkey)(key)(value).exec();
#endif

	if(r.affected_rows() == 0)
	{
		stringstream sql2;
		sql2 << "INSERT INTO "<< metaTable << " (key, value) VALUES ($1, $2);";

		string prepkey2 = metaTable+"insert";
		prepare_deduplicated(c, prepkey2, sql2.str());

#if PQXX_VERSION_MAJOR >= 7
		pqxx::result r = work->exec_prepared(prepkey2, key, value);
#else
		pqxx::result r = work->prepared(prepkey2)(key)(value).exec();
#endif
		if(r.affected_rows() != 1)
			throw runtime_error("Setting metadata should have updated a single row");
	}

	return true;
}

