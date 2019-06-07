#ifndef _DB_DUMP_H
#define _DB_DUMP_H

#include <pqxx/pqxx>
#include <string>
#include "dbusername.h"
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

int DumpNodes(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	bool order,
	int64_t startFromId,
	std::shared_ptr<IDataStreamHandler> enc);

int DumpWays(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	bool order,
	int64_t startFromId,
	std::shared_ptr<IDataStreamHandler> enc);

int DumpRelations(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	bool order,
	int64_t startFromId,
	std::shared_ptr<IDataStreamHandler> enc);

#endif //_DB_DUMP_H
