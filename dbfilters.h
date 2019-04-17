#ifndef _DB_FILTERS_H
#define _DB_FILTERS_H

#include <set>
#include "cppo5m/OsmData.h"

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

class FilterObjectsUnique : public IDataStreamHandler
{
public:
	FilterObjectsUnique(std::shared_ptr<IDataStreamHandler> enc);
	virtual ~FilterObjectsUnique();

	virtual void Sync();
	virtual void Reset();
	virtual void Finish();

	virtual void StoreIsDiff(bool isDiff);
	virtual void StoreBounds(double x1, double y1, double x2, double y2);
	virtual void StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon);
	virtual void StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	virtual void StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles);

private:
	std::set<int64_t> nodeIds, wayIds, relationIds;
	std::shared_ptr<IDataStreamHandler> enc;
};

#endif //_DB_FILTERS_H

