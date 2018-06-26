#ifndef _DB_IDS_H
#define _DB_IDS_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"

bool GetMaxObjIdLiveOrOld(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	const std::string &objType, 
	const std::string &field,
	std::string errStr,
	int64_t &val);

bool GetMaxFieldInTable(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tableName,
	const std::string &field,
	std::string &errStr,
	int64_t &val);

bool ClearNextIdValuesById(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const std::string &key);

bool SetNextIdValue(pqxx::connection &c,
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const std::string &objType,
	int64_t value);

bool GetNextId(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const std::string &objType,
	std::string &errStr,
	int64_t &out);

bool GetAllocatedIdFromDb(pqxx::connection &c,
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const std::string &objType,
	bool increment,
	std::string &errStr,
	int64_t &val);

bool ResetChangesetUidCounts(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &parentPrefix, const std::string &tablePrefix, 
	std::string &errStr);

bool GetNextObjectIds(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	std::map<std::string, int64_t> &nextIdMap,
	std::string &errStr);

bool UpdateNextObjectIds(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const std::map<std::string, int64_t> &nextIdMap,
	const std::map<std::string, int64_t> &nextIdMapOriginal,
	std::string &errStr);

bool UpdateNextIdsOfType(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &objType,
	const std::string &tableActivePrefix, 
	const std::string &tableStaticPrefix,
	std::string &errStr);

#endif //_DB_IDS_H
