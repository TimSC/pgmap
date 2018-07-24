#ifndef _DB_SHAPE_CACHE_H
#define _DB_SHAPE_CACHE_H

#include <pqxx/pqxx>
#include <string>
#include <set>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

bool DbRefreshWayShapes(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	const std::string &backingTablePrefix, 
	const std::set<int64_t> &objIds,
	std::string &errStr);

bool DbGetObjectCachedBbox(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, const std::string &typeStr, int64_t id, std::vector<double> &bboxOut);

#endif //_DB_SHAPE_CACHE_H
