#ifndef _DB_STORE_H
#define _DB_STORE_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

void EncodeTags(const TagMap &tagmap, std::string &out);

bool DbStoreObjects(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	class OsmData osmData,
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	std::string &errStr);

bool DbCheckAndCopyObjectsToActiveTable(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup,
	const std::string &typeStr, const std::set<int64_t> &objectsIds, 
	bool ocdnSupported,
	const std::string &ocdn,
	std::string &errStr, int verbose);

#endif //_DB_STORE_H
