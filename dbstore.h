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

int UpdateRelationBboxesById(pqxx::connection &c, pqxx::transaction_base *work,
	const std::set<int64_t> &objectIds,
    int verbose,
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbInsertEditActivity(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	int64_t changeset,
	int64_t timestamp,
	int64_t uid,
	const std::vector<double> &bbox,
	const std::string &action,
	int nodes,
	int ways,
	int relations,
	std::string &errStr,
	int verbose);

bool DbInsertQueryActivity(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	int64_t timestamp,
	const std::vector<double> &bbox,
	std::string &errStr,
	int verbose);

void DbGetMostActiveUsers(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, 
	int64_t startTimestamp,
	std::vector<int64_t> &uidOut,
	std::vector<std::vector<int64_t> > &objectCountOut);

#endif //_DB_STORE_H
