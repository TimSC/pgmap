#ifndef _DB_CHANGESET_H
#define _DB_CHANGESET_H

#include <pqxx/pqxx>
#include <string>
#include "pgmap.h"

bool GetAllNodesByChangeset(pqxx::work *work, const string &tablePrefix, 
	const string &excludeTablePrefix,
	int64_t changesetId,
	std::shared_ptr<IDataStreamHandler> enc);

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

bool InsertChangesetInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &tablePrefix,
	const class PgChangeset &changeset,
	std::string &errStr);

int UpdateChangesetInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &tablePrefix,
	const class PgChangeset &changeset,
	std::string &errStr);

bool CloseChangesetInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &tablePrefix,
	int64_t changesetId,
	int64_t closedTimestamp,
	size_t &rowsAffectedOut,
	std::string &errStr);

bool CopyChangesetToActiveInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &staticPrefix,
	const std::string &activePrefix,
	int64_t changesetId,
	size_t &rowsAffected,
	std::string &errStrNative);

void FilterObjectsInOsmChange(int filterMode, 
	const class OsmData &dataIn, class OsmData &dataOut);

#endif //_DB_CHANGESET_H
