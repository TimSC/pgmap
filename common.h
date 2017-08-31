#ifndef _COMMON_H
#define _COMMON_H

#include "util.h"
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include <set>

struct MetaDataCols
{
	int versionCol;
	int timestampCol;
	int changesetCol;
	int uidCol;
	int usernameCol;
};

class DataStreamRetainIds : public IDataStreamHandler
{
public:
	std::set<int64_t> nodeIds, wayIds, relationIds;
	IDataStreamHandler &out;

	DataStreamRetainIds(IDataStreamHandler &out);
	DataStreamRetainIds(const DataStreamRetainIds &obj);
	virtual ~DataStreamRetainIds();

	void StoreIsDiff(bool);
	void StoreBounds(double x1, double y1, double x2, double y2);
	void StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon);
	void StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	void StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles);
};

void DecodeMetadata(const pqxx::result::const_iterator &c, const MetaDataCols &metaDataCols, class MetaData &metaData);
void DecodeTags(const pqxx::result::const_iterator &c, int tagsCol, JsonToStringMap &handler);
void DecodeWayMembers(const pqxx::result::const_iterator &c, int membersCol, JsonToWayMembers &handler);
void DecodeRelMembers(const pqxx::result::const_iterator &c, int membersCol, int memberRolesCols, 
	JsonToRelMembers &handler, JsonToRelMemberRoles &roles);

void GetNodesInBbox(pqxx::connection &dbconn, std::map<string, string> &config, 
	const std::vector<double> &bbox, IDataStreamHandler &enc);
void GetWaysThatContainNodes(pqxx::connection &dbconn, std::map<string, string> &config, 
	const std::set<int64_t> &nodeIds, IDataStreamHandler &enc);

void DumpNodes(pqxx::connection &dbconn, std::map<string, string> &config, bool onlyLiveData, IDataStreamHandler &enc);
void DumpWays(pqxx::connection &dbconn, std::map<string, string> &config, bool onlyLiveData, IDataStreamHandler &enc);
void DumpRelations(pqxx::connection &dbconn, std::map<string, string> &config, bool onlyLiveData, IDataStreamHandler &enc);

#endif //_COMMON_H

