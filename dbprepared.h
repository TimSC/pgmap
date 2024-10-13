#ifndef _DB_PREPARED_H
#define _DB_PREPARED_H

#include <pqxx/pqxx> //apt install libpqxx-dev

void prepare_deduplicated(pqxx::connection &c, std::string key, std::string sql);

#endif //_DB_PREPARED_H
