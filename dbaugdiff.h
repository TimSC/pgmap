#ifndef _DB_AUGDIFF_H
#define _DB_AUGDIFF_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

bool DbQueryAugDiff(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	int64_t startTimestamp,
	bool bboxSet,
	const std::vector<double> &bbox,
	std::streambuf &fiOut,
	std::string &errStr);

#endif //_DB_AUGDIFF_H
