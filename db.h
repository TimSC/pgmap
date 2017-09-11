#ifndef _COMMON_H
#define _COMMON_H

#include "util.h"
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/OsmData.h"
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

class DataStreamRetainMemIds : public IDataStreamHandler
{
public:
	std::set<int64_t> nodeIds, wayIds, relationIds;
	IDataStreamHandler &out;

	DataStreamRetainMemIds(IDataStreamHandler &out);
	DataStreamRetainMemIds(const DataStreamRetainMemIds &obj);
	virtual ~DataStreamRetainMemIds();

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

void GetLiveNodesInBbox(pqxx::work &work, const string &tablePrefix, 
	const std::vector<double> &bbox, 
	unsigned int maxNodes,
	IDataStreamHandler &enc);
void GetLiveWaysThatContainNodes(pqxx::work &work, const string &tablePrefix, 
	const std::set<int64_t> &nodeIds, IDataStreamHandler &enc);
void GetLiveRelationsForObjects(pqxx::work &work, const string &tablePrefix, 
	char qtype, const set<int64_t> &qids, const set<int64_t> &skipIds, 
	IDataStreamHandler &enc);

void GetLiveNodesById(pqxx::work &work, const string &tablePrefix, 
	const std::set<int64_t> &nodeIds, IDataStreamHandler &enc);
void GetLiveWaysById(pqxx::work &work, const string &tablePrefix, 
	const std::set<int64_t> &wayIds, IDataStreamHandler &enc);
void GetLiveRelationsById(pqxx::work &work, const string &tablePrefix, 
	const std::set<int64_t> &relationIds, IDataStreamHandler &enc);

void DumpNodes(pqxx::work &work, const string &tablePrefix, bool onlyLiveData, IDataStreamHandler &enc);
void DumpWays(pqxx::work &work, const string &tablePrefix, bool onlyLiveData, IDataStreamHandler &enc);
void DumpRelations(pqxx::work &work, const string &tablePrefix, bool onlyLiveData, IDataStreamHandler &enc);

bool StoreObjects(pqxx::connection &c, pqxx::work &work, 
	const string &tablePrefix, 
	const class OsmData &osmData, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWaysIds,
	std::map<int64_t, int64_t> &createdRelationsIds,
	std::string &errStr);

#endif //_COMMON_H

