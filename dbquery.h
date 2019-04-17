#ifndef _DB_QUERY_H
#define _DB_QUERY_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"
#include <set>
#include "dbusername.h"

//Functions for querying current (live) data

std::shared_ptr<pqxx::icursorstream> LiveNodesInBboxStart(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	const std::vector<double> &bbox, 
	int64_t existsAtTimestamp,
	const std::string &excludeTablePrefix);

std::shared_ptr<pqxx::icursorstream> LiveNodesInWktStart(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	const std::string &wkt, int srid,
	const std::string &excludeTablePrefix);

int LiveNodesInBboxContinue(std::shared_ptr<pqxx::icursorstream> cursor, class DbUsernameLookup &usernames, 
	std::shared_ptr<IDataStreamHandler> enc);

void GetLiveWaysThatContainNodes(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	const std::set<int64_t> &nodeIds, std::shared_ptr<IDataStreamHandler> enc);

void GetLiveRelationsForObjects(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	char qtype, const std::set<int64_t> &qids, 
	std::set<int64_t>::const_iterator &it, size_t step,
	const std::set<int64_t> &skipIds, 
	std::shared_ptr<IDataStreamHandler> enc);

void GetLiveNodesById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &nodeIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc);

void GetLiveWaysById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &wayIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc);

void GetLiveRelationsById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &relationIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc);

// Query old versions

void DbGetObjectsByIdVer(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &objType, const std::string &liveOrOld,
	const std::set<std::pair<int64_t, int64_t> > &objIdVers, std::set<std::pair<int64_t, int64_t> >::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc);

void DbGetObjectsHistoryById(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &objType, const std::string &liveOrOld,
	const std::set<int64_t> &objIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc);

// Functions for historical querys at a specific timestamp

void QueryOldNodesInBbox(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::vector<double> &bbox, 
	int64_t existsAtTimestamp,
	std::shared_ptr<IDataStreamHandler> enc);

void GetWayIdVersThatContainNodes(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::set<int64_t> &nodeIds, std::set<std::pair<int64_t, int64_t> > &wayIdVersOut);

// Active/static aware high-level fetch

void DbGetObjectsById(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tableStaticPrefix, 
	const std::string &tableActivePrefix, 
	class DbUsernameLookup &dbUsernameLookup,  
	const std::string &type, const std::set<int64_t> &objectIds, 
	std::shared_ptr<IDataStreamHandler> out);

#endif //_DB_QUERY_H

