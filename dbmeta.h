#ifndef _DBADMIN_H
#define _DBADMIN_H
#include <pqxx/pqxx>
#include <string>
#include <map>

std::string DbGetMetaValue(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &key, 
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbSetMetaValue(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &key, 
	const std::string &value, 
	const std::string &tablePrefix, 
	std::string &errStr);

///Every key and value in the metadata table of one set of map tables.
void DbGetMetaValues(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix,
	std::map<std::string, std::string> &valuesOut);

///Removes a key from the metadata table. Returns false if it was not there.
bool DbDeleteMetaValue(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &key,
	const std::string &tablePrefix);

#endif //_DBADMIN_H
