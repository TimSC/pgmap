#ifndef _DB_CHANGESET_H
#define _DB_CHANGESET_H

#include <pqxx/pqxx>
#include <string>
#include <expat.h>
#include "pgmap.h"

bool GetAllNodesByChangeset(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	int64_t changesetId,
	std::shared_ptr<IDataStreamHandler> enc);

bool GetAllWaysByChangeset(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	int64_t changesetId,
	std::shared_ptr<IDataStreamHandler> enc);

bool GetAllRelationsByChangeset(pqxx::connection &c, pqxx::transaction_base *work, class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	int64_t changesetId,
	std::shared_ptr<IDataStreamHandler> enc);

int GetChangesetFromDb(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	class DbUsernameLookup &usernames,
	int64_t objId,
	class PgChangeset &changesetOut,
	std::string &errStr);

bool GetChangesetsFromDb(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const std::string &excludePrefix,
	class DbUsernameLookup &usernames,
	const class PgChangesetQuery &query,
	std::vector<class PgChangeset> &changesetOut,
	std::string &errStr);

///Sets the created, modified and deleted counts of each changeset from the
///edit activity table, leaving zero where no activity was recorded.
void DbGetChangesetChangeCounts(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix,
	std::vector<class PgChangeset> &changesets);

///Counts a user's changesets in one set of tables, leaving out any that are
///also in the tables named by excludePrefix.
int64_t DbCountChangesets(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix,
	const std::string &excludePrefix,
	int64_t user_uid);

bool InsertChangesetInDb(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const class PgChangeset &changeset,
	std::string &errStr);

int UpdateChangesetInDb(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const class PgChangeset &changeset,
	std::string &errStr);

int DbExpandChangesetBbox(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	int64_t cid,
	const std::vector<double> &bbox,
	std::string &errStr);

bool CloseChangesetInDb(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	int64_t changesetId,
	int64_t closedTimestamp,
	size_t &rowsAffectedOut,
	std::string &errStr);

bool CloseChangesetsOlderThanInDb(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	int64_t whereBeforeTimestamp,
	int64_t closedTimestamp,
	size_t &rowsAffectedOut,
	std::string &errStr);

bool CopyChangesetToActiveInDb(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &staticPrefix,
	const std::string &activePrefix,
	class DbUsernameLookup &usernames,
	int64_t changesetId,
	size_t &rowsAffected,
	std::string &errStrNative);

void FilterObjectsInOsmChange(int filterMode, 
	const class OsmData &dataIn, class OsmData &dataOut);

class OsmChangesetsDecodeString
{
private:
	bool firstParseCall;
	XML_Parser parser;
	int xmlDepth;
	TagMap currentTags;
	class PgChangeset currentChangeset;

public:
	bool parseCompletedOk;
	std::string errString;
	std::vector<class PgChangeset> outChangesets;

	OsmChangesetsDecodeString();
	virtual ~OsmChangesetsDecodeString();
	
	void StartElement(const XML_Char *name, const XML_Char **atts);
	void EndElement(const XML_Char *name);
	bool DecodeSubString(const char *xml, size_t len, bool done);
	void XmlAttsToMap(const XML_Char **atts, std::map<std::string, std::string> &attribs);
};

#endif //_DB_CHANGESET_H
