#ifndef _DB_CHANGESET_H
#define _DB_CHANGESET_H

#include <pqxx/pqxx>
#include <string>
#include "pgmap.h"

int GetChangesetFromDb(pqxx::work *work, 
	const std::string &tablePrefix,
	int64_t objId,
	class PgChangeset &changesetOut,
	std::string &errStr);

bool GetChangesetsFromDb(pqxx::work *work, 
	const std::string &tablePrefix,
	const std::string &excludePrefix,
	size_t limit,
	std::vector<class PgChangeset> &changesetOut,
	std::string &errStr);

bool SetChangesetInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &tablePrefix,
	const class PgChangeset &changeset,
	std::string &errStr);

bool CloseChangesetInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &tablePrefix,
	int64_t changesetId,
	int64_t closedTimestamp,
	std::string &errStr);

#endif //_DB_CHANGESET_H
