#ifndef _DB_DUMP_H
#define _DB_DUMP_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

void DumpNodes(pqxx::work *work, const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	bool order,
	std::shared_ptr<IDataStreamHandler> enc);
void DumpWays(pqxx::work *work, const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	bool order,
	std::shared_ptr<IDataStreamHandler> enc);
void DumpRelations(pqxx::work *work, const std::string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc);

#endif //_DB_DUMP_H
