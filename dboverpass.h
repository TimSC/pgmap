#ifndef _DB_OVERPASS_H
#define _DB_OVERPASS_H

#include <pqxx/pqxx>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"
#include "dbusername.h"

void DbXapiQueryVisible(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox, 
	std::shared_ptr<IDataStreamHandler> enc);

#endif //_DB_OVERPASS_H
