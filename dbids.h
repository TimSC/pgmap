#ifndef _DB_IDS_H
#define _DB_IDS_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

bool GetMaxObjIdLiveOrOld(pqxx::transaction_base *work, const string &tablePrefix, 
	const std::string &objType, 
	const std::string &field,
	std::string errStr,
	int64_t &val);

bool GetMaxFieldInTable(pqxx::transaction_base *work, 
	const string &tableName,
	const string &field,
	string &errStr,
	int64_t &val);

bool ClearNextIdValuesById(pqxx::transaction_base *work, 
	const string &tablePrefix,
	const string &key);

bool SetNextIdValue(pqxx::connection &c,
	pqxx::transaction_base *work, 
	const string &tablePrefix,
	const string &objType,
	int64_t value);

bool GetNextId(pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const std::string &objType,
	std::string &errStr,
	int64_t &out);

bool GetAllocatedIdFromDb(pqxx::connection &c,
	pqxx::transaction_base *work, 
	const string &tablePrefix,
	const string &objType,
	bool increment,
	string &errStr,
	int64_t &val);

bool ResetChangesetUidCounts(pqxx::transaction_base *work, 
	const string &parentPrefix, const string &tablePrefix, 
	string &errStr);

bool GetNextObjectIds(pqxx::transaction_base *work, 
	const string &tablePrefix,
	map<string, int64_t> &nextIdMap,
	string &errStr);

bool UpdateNextObjectIds(pqxx::transaction_base *work, 
	const string &tablePrefix,
	const map<string, int64_t> &nextIdMap,
	const map<string, int64_t> &nextIdMapOriginal,
	string &errStr);

bool UpdateNextIdsOfType(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &objType,
	const string &tableActivePrefix, 
	const string &tableStaticPrefix,
	std::string &errStr);

#endif //_DB_IDS_H
