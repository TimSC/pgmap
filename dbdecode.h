#ifndef _DB_DECODE_H
#define _DB_DECODE_H

#include <pqxx/pqxx>
#include <string>
#include <set>
#include "util.h"
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"
#include "dbusername.h"

struct MetaDataCols
{
	int versionCol;
	int timestampCol;
	int changesetCol;
	int uidCol;
	int usernameCol;
	int visibleCol;
};

void DecodeMetadata(const pqxx::result::const_iterator &c, const MetaDataCols &metaDataCols, class MetaData &metaData);
void DecodeTags(const pqxx::result::const_iterator &c, int tagsCol, JsonToStringMap &handler);

void DecodeWayMembers(const pqxx::result::const_iterator &c, int membersCol, JsonToWayMembers &handler);
void DecodeRelMembers(const pqxx::result::const_iterator &c, int membersCol, int memberRolesCols, 
	JsonToRelMembers &handler, JsonToRelMemberRoles &roles);

int NodeResultsToEncoder(pqxx::icursorstream &cursor, class DbUsernameLookup &usernames, std::shared_ptr<IDataStreamHandler> enc);
int WayResultsToEncoder(pqxx::icursorstream &cursor, class DbUsernameLookup &usernames, std::shared_ptr<IDataStreamHandler> enc);
void RelationResultsToEncoder(pqxx::icursorstream &cursor, class DbUsernameLookup &usernames, 
	const std::set<int64_t> &skipIds, std::shared_ptr<IDataStreamHandler> enc);

#endif //_DB_DECODE_H
