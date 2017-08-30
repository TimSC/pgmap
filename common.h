#ifndef _COMMON_H
#define _COMMON_H

#include "util.h"
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"

struct MetaDataCols
{
	int versionCol;
	int timestampCol;
	int changesetCol;
	int uidCol;
	int usernameCol;
};

void DecodeMetadata(const pqxx::result::const_iterator &c, const MetaDataCols &metaDataCols, class MetaData &metaData);
void DecodeTags(const pqxx::result::const_iterator &c, int tagsCol, JsonToStringMap &handler);
void DecodeWayMembers(const pqxx::result::const_iterator &c, int membersCol, JsonToWayMembers &handler);
void DecodeRelMembers(const pqxx::result::const_iterator &c, int membersCol, int memberRolesCols, 
	JsonToRelMembers &handler, JsonToRelMemberRoles &roles);

void DumpNodes(pqxx::connection &dbconn, std::map<string, string> &config, O5mEncode &enc);
void DumpWays(pqxx::connection &dbconn, std::map<string, string> &config, O5mEncode &enc);
void DumpRelations(pqxx::connection &dbconn, std::map<string, string> &config, O5mEncode &enc);

#endif //_COMMON_H

