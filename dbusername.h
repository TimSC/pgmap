#ifndef _DB_USERNAME_H_
#define _DB_USERNAME_H_

#include <pqxx/pqxx> //apt install libpqxx-dev
#include <string>
#include <map>

class DbUsernameLookup
{
private:
	pqxx::connection &c;
	pqxx::transaction_base *work;
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	bool tableStaticExists, tableActiveExists;
	std::map<int, std::string> cache;

public:
	DbUsernameLookup(pqxx::connection &c, pqxx::transaction_base *work, 
		const std::string &tableStaticPrefix,
		const std::string &tableActivePrefix);
	virtual ~DbUsernameLookup();

	std::string Find(int uid);
};

void DbUpsertUsernamePrepare(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix);

void DbUpsertUsername(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	int uid, const std::string &username);

#endif //_DB_USERNAME_H_

