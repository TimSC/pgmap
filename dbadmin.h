#ifndef _DB_ADMIN_H
#define _DB_ADMIN_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include <string>

bool ResetActiveTables(pqxx::connection &c, pqxx::work *work, 
	const std::string &tableActivePrefix, 
	const std::string &tableStaticPrefix,
	std::string &errStr);

bool DbCreateTables(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbDropTables(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbCopyData(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &filePrefix,
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbCreateIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbRefreshMaxIds(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	std::string &errStr);

bool DbRefreshMaxChangesetUid(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	std::string &errStr);

#endif //_DB_ADMIN_H
