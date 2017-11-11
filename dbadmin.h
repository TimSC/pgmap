#ifndef _DB_ADMIN_H
#define _DB_ADMIN_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include <string>

bool ResetActiveTables(pqxx::connection &c, pqxx::work *work, 
	const std::string &tableActivePrefix, 
	const std::string &tableStaticPrefix,
	std::string &errStr);

bool DbCreateTables(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbDropTables(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	std::string &errStr);

#endif //_DB_ADMIN_H
