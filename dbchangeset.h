#ifndef _DB_CHANGESET_H
#define _DB_CHANGESET_H

#include <pqxx/pqxx>
#include <string>
#include "pgmap.h"

bool GetChangesetFromDb(pqxx::work *work, 
	const std::string &tablePrefix,
	int64_t objId,
	class PgChangeset &changesetOut,
	std::string &errStr);

#endif //_DB_CHANGESET_H
