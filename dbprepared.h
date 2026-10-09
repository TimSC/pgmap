#ifndef _DB_PREPARED_H
#define _DB_PREPARED_H

#include <pqxx/pqxx> //apt install libpqxx-dev

// Prepare a statement once per connection. Prepared statements belong to the
// connection that created them, so each connection is tracked separately.
void prepare_deduplicated(pqxx::connection &c, std::string key, std::string sql);

// Forget what was prepared on a connection. Call when the connection is destroyed.
void forget_prepared(pqxx::connection &c);

#endif //_DB_PREPARED_H
