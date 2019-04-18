#ifndef _DB_SHAPELOG_H
#define _DB_SHAPELOG_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

///Log the old way shapes that are impacted by new nodes, or have new way versions
bool DbLogWayShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &usernames, 
	int64_t timestamp,
	const class OsmData &osmData,
	std::set<int64_t> &touchedWayIdsOut,
	std::string &errStr);

bool DbLogRelationShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	int64_t timestamp,
	const std::set<int64_t> &touchedNodeIds,
	const std::set<int64_t> &touchedWayIds,
	const std::set<int64_t> &touchedRelationIds,
	std::string &errStr);

#endif //_DB_SHAPELOG_H
