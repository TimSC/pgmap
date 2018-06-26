#ifndef _DB_CHANGESET_H
#define _DB_CHANGESET_H

#include <pqxx/pqxx>
#include <string>
#include "pgmap.h"

bool GetAllNodesByChangeset(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	int64_t changesetId,
	std::shared_ptr<IDataStreamHandler> enc);

bool GetAllWaysByChangeset(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	int64_t changesetId,
	std::shared_ptr<IDataStreamHandler> enc);

bool GetAllRelationsByChangeset(pqxx::connection &c, pqxx::transaction_base *work, const std::string &tablePrefix, 
	const std::string &excludeTablePrefix,
	int64_t changesetId,
	std::shared_ptr<IDataStreamHandler> enc);

int GetChangesetFromDb(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	int64_t objId,
	class PgChangeset &changesetOut,
	std::string &errStr);

bool GetChangesetsFromDb(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	const std::string &excludePrefix,
	size_t limit,
	int64_t user_uid,
	std::vector<class PgChangeset> &changesetOut,
	std::string &errStr);

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

bool CloseChangesetInDb(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	int64_t changesetId,
	int64_t closedTimestamp,
	size_t &rowsAffectedOut,
	std::string &errStr);

bool CopyChangesetToActiveInDb(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &staticPrefix,
	const std::string &activePrefix,
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
