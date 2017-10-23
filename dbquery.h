#ifndef _DB_QUERY_H
#define _DB_QUERY_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"
#include <set>

std::shared_ptr<pqxx::icursorstream> LiveNodesInBboxStart(pqxx::work *work, const string &tablePrefix, 
	const std::vector<double> &bbox, 
	const string &excludeTablePrefix);
int LiveNodesInBboxContinue(std::shared_ptr<pqxx::icursorstream> cursor, std::shared_ptr<IDataStreamHandler> enc);

void GetLiveWaysThatContainNodes(pqxx::work *work, const string &tablePrefix, 
	const string &excludeTablePrefix,
	const std::set<int64_t> &nodeIds, std::shared_ptr<IDataStreamHandler> enc);

void GetLiveRelationsForObjects(pqxx::work *work, const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	char qtype, const set<int64_t> &qids, 
	set<int64_t>::const_iterator &it, size_t step,
	const set<int64_t> &skipIds, 
	std::shared_ptr<IDataStreamHandler> enc);

void GetLiveNodesById(pqxx::work *work, const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &nodeIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc);

void GetLiveWaysById(pqxx::work *work, const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &wayIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc);

void GetLiveRelationsById(pqxx::work *work, const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &relationIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc);

#endif //_DB_QUERY_H

