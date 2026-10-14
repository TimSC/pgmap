#ifndef _DB_ADMIN_H
#define _DB_ADMIN_H

#include <pqxx/pqxx> //apt install libpqxx-dev
#include <string>
#include "pgcommon.h"

bool ResetActiveTables(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tableActivePrefix, 
	const std::string &tableStaticPrefix,
	std::string &errStr);

// Drop known map objects directly, without depending on migration history.
bool DbDropMapTables(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &tablePrefix, std::string &errStr);

bool DbSetSchemaVersion(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &parentPrefix, 
	const std::string &tablePrefix, 
	int targetVer, bool latest, std::string &errStr);

// Reads a map file (.osm, .o5m, .pbf or .json, optionally .gz) straight into
// the map tables of tablePrefix, which should be empty.
bool DbImportMapFile(pqxx::connection &c, pqxx::transaction_base *work,
	int verbose, const std::string &filename, const std::string &tablePrefix,
	std::string &errStr);
// The latest schema version of the map tables
const int MAP_SCHEMA_VERSION = 15;
// The version of the indices that DbCreateIndices builds on the main map
// tables. It is recorded separately from the schema version, as the indices
// are built in a step of their own (see dbadmin.cpp), but numbered with it:
// index version 15 is the set of indices for schema version 15. When the
// schema version is raised, this is raised with it, whether or not the
// indices change, so that the admin tool asks for "Create indicies" to be run.
const int MAP_INDEX_VERSION = MAP_SCHEMA_VERSION;
// The version a table set's indices were last brought up to; 0 if never recorded
int DbGetIndexVersion(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix);
// A sentence saying a table set's indices are behind, or nothing if they are not
std::string DbIndexVersionNote(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix);
bool DbCreateIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbRefreshMaxIds(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	std::string &errStr);

bool DbRefreshMaxChangesetUid(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	std::string &errStr);

bool DbGenerateUsernameTable(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	std::string &errStr);

bool DbApplyDiffs(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tableStaticPrefix, 
	const std::string &tableModPrefix, 
	const std::string &tableTestPrefix, 
	const std::string &diffPath, 
	class PgCommon *pgCommon,
	std::string &errStr);

void DbCheckNodesExistForAllWays(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	const std::string &nodeStaticPrefix, 
	const std::string &nodeActivePrefix);

void DbCheckObjectIdTables(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, const std::string &edition, const std::string &objType);

int DbUpdateWayBboxes(pqxx::connection &c, pqxx::transaction_base *work,
    int verbose,
	const std::string &tablePrefix, 
	class PgCommon *adminObj,
	std::string &errStr,
	bool onlyMissing = false);

int DbUpdateRelationBboxes(pqxx::connection &c, pqxx::transaction_base *work,
    int verbose,
	const std::string &tablePrefix, 
	class PgCommon *adminObj,
	std::string &errStr);

bool DbCreateBboxIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tablePrefix, 
	std::string &errStr);

bool DbDropBboxIndices(pqxx::connection &c, pqxx::transaction_base *work, 
	int verbose, 
	const std::string &tablePrefix, 
	std::string &errStr);

#endif //_DB_ADMIN_H

