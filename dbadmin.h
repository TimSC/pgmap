#ifndef _DB_ADMIN_H
#define _DB_ADMIN_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include <string>

bool ResetActiveTables(pqxx::connection &c, pqxx::work *work, 
	const std::string &tableActivePrefix, 
	const std::string &tableStaticPrefix,
	std::string &errStr);

#endif //_DB_ADMIN_H
