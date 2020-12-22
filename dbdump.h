#ifndef _DB_DUMP_H
#define _DB_DUMP_H

#include <pqxx/pqxx>
#include <string>
#include "dbusername.h"
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

void DumpNodes(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc);
void DumpWays(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc);
void DumpRelations(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc);

#endif //_DB_DUMP_H
