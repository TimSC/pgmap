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

	bool StoreIsDiff(bool);
	bool StoreBounds(double x1, double y1, double x2, double y2);

	bool StoreBbox(const std::vector<double> &bbox);

	bool StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon);
	bool StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	bool StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
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

	bool StoreIsDiff(bool);
	bool StoreBounds(double x1, double y1, double x2, double y2);

	bool StoreBbox(const std::vector<double> &bbox);

	bool StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon);
	bool StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	bool StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles);
};

class FilterObjectsUnique : public IDataStreamHandler
{
public:
	FilterObjectsUnique(std::shared_ptr<IDataStreamHandler> enc);
	virtual ~FilterObjectsUnique();

	virtual bool Sync();
	virtual bool Reset();
	virtual bool Finish();

	virtual bool StoreIsDiff(bool isDiff);
	virtual bool StoreBounds(double x1, double y1, double x2, double y2);

	virtual bool StoreBbox(const std::vector<double> &bbox);

	virtual bool StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon);
	virtual bool StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	virtual bool StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles);

private:
	std::set<int64_t> nodeIds, wayIds, relationIds;
	std::shared_ptr<IDataStreamHandler> enc;
};

#endif //_DB_FILTERS_H

