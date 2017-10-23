#ifndef _DB_REPLICATE_H
#define _DB_REPLICATE_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

void GetReplicateDiffNodes(pqxx::work *work, const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	std::shared_ptr<IDataStreamHandler> enc);

void GetReplicateDiffWays(pqxx::work *work, const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	std::shared_ptr<IDataStreamHandler> enc);

void GetReplicateDiffRelations(pqxx::work *work, const string &tablePrefix, 
	bool selectOld,
	int64_t timestampStart, int64_t timestampEnd,
	std::shared_ptr<IDataStreamHandler> enc);

#endif //_DB_REPLICATE_H
