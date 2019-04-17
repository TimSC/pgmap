#ifndef _DB_STORE_H
#define _DB_STORE_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

void EncodeTags(const TagMap &tagmap, std::string &out);

///Log the old way shapes that are impacted by new nodes, or have new way versions
bool DbLogWayShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &usernames, 
	int64_t timestamp,
	const class OsmData &osmData,
	std::string &errStr);

bool DbStoreObjects(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	class OsmData osmData,
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	std::string &errStr);

#endif //_DB_STORE_H
