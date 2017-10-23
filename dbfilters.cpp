#include "dbfilters.h"
using namespace std;

DataStreamRetainIds::DataStreamRetainIds(IDataStreamHandler &outObj) : IDataStreamHandler(), out(outObj)
{

}

DataStreamRetainIds::DataStreamRetainIds(const DataStreamRetainIds &obj) : IDataStreamHandler(), out(obj.out)
{
	nodeIds = obj.nodeIds;
	wayIds = obj.wayIds;
	relationIds = obj.relationIds;
}

DataStreamRetainIds::~DataStreamRetainIds()
{

}

void DataStreamRetainIds::StoreIsDiff(bool diff)
{
	out.StoreIsDiff(diff);
}

void DataStreamRetainIds::StoreBounds(double x1, double y1, double x2, double y2)
{
	out.StoreBounds(x1, y1, x2, y2);
}

void DataStreamRetainIds::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	out.StoreNode(objId, metaData, tags, lat, lon);
	this->nodeIds.insert(objId);
}

void DataStreamRetainIds::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	out.StoreWay(objId, metaData, tags, refs);
	this->wayIds.insert(objId);
}

void DataStreamRetainIds::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	if(refTypeStrs.size() != refIds.size() || refTypeStrs.size() != refRoles.size())
		throw std::invalid_argument("Length of ref vectors must be equal");
	out.StoreRelation(objId, metaData, tags, refTypeStrs, refIds, refRoles);
	this->relationIds.insert(objId);
}

// ******************************

DataStreamRetainMemIds::DataStreamRetainMemIds(IDataStreamHandler &outObj) : IDataStreamHandler(), out(outObj)
{

}

DataStreamRetainMemIds::DataStreamRetainMemIds(const DataStreamRetainMemIds &obj) : IDataStreamHandler(), out(obj.out)
{
	nodeIds = obj.nodeIds;
	wayIds = obj.wayIds;
	relationIds = obj.relationIds;
}

DataStreamRetainMemIds::~DataStreamRetainMemIds()
{

}

void DataStreamRetainMemIds::StoreIsDiff(bool diff)
{
	out.StoreIsDiff(diff);
}

void DataStreamRetainMemIds::StoreBounds(double x1, double y1, double x2, double y2)
{
	out.StoreBounds(x1, y1, x2, y2);
}

void DataStreamRetainMemIds::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	out.StoreNode(objId, metaData, tags, lat, lon);
}

void DataStreamRetainMemIds::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	out.StoreWay(objId, metaData, tags, refs);
	for(size_t i=0; i < refs.size(); i++)
		this->nodeIds.insert(refs[i]);
}

void DataStreamRetainMemIds::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	if(refTypeStrs.size() != refIds.size() || refTypeStrs.size() != refRoles.size())
		throw std::invalid_argument("Length of ref vectors must be equal");
	out.StoreRelation(objId, metaData, tags, refTypeStrs, refIds, refRoles);
	for(size_t i=0; i < refTypeStrs.size(); i++)
	{
		if(refTypeStrs[i] == "node")
			this->nodeIds.insert(objId);
		else if(refTypeStrs[i] == "way")
			this->wayIds.insert(objId);
		else if(refTypeStrs[i] == "relation")
			this->relationIds.insert(objId);
		else
			throw runtime_error("Unknown member type in relation");
	}
}

