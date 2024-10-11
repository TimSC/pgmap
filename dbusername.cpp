#include "dbusername.h"
#include "dbcommon.h"
#include "dbprepared.h"
#include <iostream>
using namespace std;

const int DB_USERNAME_CACHE_MAX = 10000;

DbUsernameLookup::DbUsernameLookup(pqxx::connection &c, pqxx::transaction_base *work, 
		const std::string &tableStaticPrefix,
		const std::string &tableActivePrefix):
		c(c),
		work(work),
		tableStaticPrefix(tableStaticPrefix),
		tableActivePrefix(tableActivePrefix)
{
	tableStaticExists = false;
	tableActiveExists = false;

	if(this->tableStaticPrefix.length() > 0)
	{
		string tableName = this->tableStaticPrefix+"usernames";
		tableStaticExists = DbCheckTableExists(c, work, tableName);
		if(tableStaticExists)
		{
			stringstream sql;
			sql << "SELECT username FROM " << tableName << " WHERE uid = $1;";
			prepare_deduplicated(c, this->tableStaticPrefix+"getusername", sql.str());
		}
	}

	if(this->tableActivePrefix.length() > 0)
	{
		string tableName = this->tableActivePrefix+"usernames";
		tableActiveExists = DbCheckTableExists(c, work, tableName);
		if(tableActiveExists)
		{
			stringstream sql;
			sql << "SELECT username FROM " << tableName << " WHERE uid = $1;";
			prepare_deduplicated(c, this->tableActivePrefix+"getusername2", sql.str());
		}
	}
	
}

DbUsernameLookup::~DbUsernameLookup()
{

}

std::string DbUsernameLookup::Find(int uid)
{
	if(uid == 0)
		return "";
	auto it = this->cache.find(uid);
	if(it != this->cache.end())
		return it->second;

	if(tableActiveExists)
	{
		pqxx::result result = work->exec_prepared(this->tableActivePrefix+"getusername", uid);
		for (pqxx::result::const_iterator ci = result.begin(); ci != result.end(); ++ci)
		{
			string username = ci[0].as<string>();
			this->cache[uid] = username;
			if (this->cache.size()>DB_USERNAME_CACHE_MAX) cache.clear();
			return username;	
		}
	}

	if(tableStaticExists)
	{
		pqxx::result result = work->exec_prepared(this->tableStaticPrefix+"getusername", uid);
		for (pqxx::result::const_iterator ci = result.begin(); ci != result.end(); ++ci)
		{
			string username = ci[0].as<string>();
			this->cache[uid] = username;
			if (this->cache.size()>DB_USERNAME_CACHE_MAX) cache.clear();
			return username;	
		}
	}

	return "";
}

// ******************************************

void DbUpsertUsernamePrepare(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix)
{
	string insertsql = "INSERT INTO "+c.quote_name(tablePrefix+"usernames")+" (uid, username) VALUES ($1, $2);";
	prepare_deduplicated(c, tablePrefix+"insertusername", insertsql);

	string updatesql = "UPDATE "+ c.quote_name(tablePrefix+"usernames")+" SET username=$2";
	updatesql += " WHERE uid = $1;";
	prepare_deduplicated(c, tablePrefix+"updateusername", updatesql);
}

void DbUpsertUsername(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	int uid, const std::string &username)
{
	pqxx::result result = work->exec_prepared(tablePrefix+"updateusername", uid, username);
	int rowsAffected = result.affected_rows();

	if(rowsAffected == 0)
	{
		work->exec_prepared(tablePrefix+"insertusername", uid, username);
	}
}

