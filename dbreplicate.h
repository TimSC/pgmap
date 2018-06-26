#ifndef _DB_REPLICATE_H
#define _DB_REPLICATE_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

void GetReplicateDiffNodes(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	class OsmChange &out);

void GetReplicateDiffWays(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	class OsmChange &out);

void GetReplicateDiffRelations(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	class OsmChange &out);

#endif //_DB_REPLICATE_H
