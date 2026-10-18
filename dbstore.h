#ifndef _DB_STORE_H
#define _DB_STORE_H

#include <pqxx/pqxx>
#include <string>
#include "cppo5m/o5m.h"
#include "cppo5m/model.h"

void EncodeTags(const TagMap &tagmap, std::string &out);

bool StoreObjects(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	class OsmData osmData, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	std::string &errStr,
	// The atomic edit the objects are new versions of, recorded with each
	// version written (schema 16); 0 for objects only being copied, such as
	// the parents of what an edit changes, which are recorded as NULL
	int64_t atomicEditId = 0);

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

bool DbInsertQueryActivity(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	int64_t timestamp,
	const std::vector<double> &bbox,
	std::string &errStr,
	int verbose);

#endif //_DB_STORE_H
