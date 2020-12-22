#ifndef _DB_STORE_H
#define _DB_STORE_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

void EncodeTags(const TagMap &tagmap, std::string &out);

bool StoreObjects(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	class OsmData osmData, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	std::string &errStr);

int UpdateWayBboxesById(pqxx::connection &c, pqxx::transaction_base *work,
	const std::set<int64_t> &wayIds,
    int verbose,
	const std::string &tablePrefix, 
	std::string &errStr);

#endif //_DB_STORE_H
