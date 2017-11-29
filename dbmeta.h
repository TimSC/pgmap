#ifndef _DBADMIN_H
#define _DBADMIN_H
#include <pqxx/pqxx>
#include <string>

std::string DbGetMetaValue(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &key, 
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbSetMetaValue(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &key, 
	const std::string &value, 
	const std::string &tablePrefix, 
	std::string &errStr);

#endif //_DBADMIN_H
