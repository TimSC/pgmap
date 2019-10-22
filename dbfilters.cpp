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

bool DataStreamRetainIds::StoreIsDiff(bool diff)
{
	return out.StoreIsDiff(diff);
}

bool DataStreamRetainIds::StoreBounds(double x1, double y1, double x2, double y2)
{
	return out.StoreBounds(x1, y1, x2, y2);
}

bool DataStreamRetainIds::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	this->nodeIds.insert(objId);
	return out.StoreNode(objId, metaData, tags, lat, lon);
}

bool DataStreamRetainIds::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	this->wayIds.insert(objId);
	return out.StoreWay(objId, metaData, tags, refs);
}

bool DataStreamRetainIds::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	if(refTypeStrs.size() != refIds.size() || refTypeStrs.size() != refRoles.size())
		throw std::invalid_argument("Length of ref vectors must be equal");
	this->relationIds.insert(objId);
	return out.StoreRelation(objId, metaData, tags, refTypeStrs, refIds, refRoles);
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

bool DataStreamRetainMemIds::StoreIsDiff(bool diff)
{
	return out.StoreIsDiff(diff);
}

bool DataStreamRetainMemIds::StoreBounds(double x1, double y1, double x2, double y2)
{
	return out.StoreBounds(x1, y1, x2, y2);
}

bool DataStreamRetainMemIds::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	return out.StoreNode(objId, metaData, tags, lat, lon);
}

bool DataStreamRetainMemIds::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	for(size_t i=0; i < refs.size(); i++)
		this->nodeIds.insert(refs[i]);
	return out.StoreWay(objId, metaData, tags, refs);
}

bool DataStreamRetainMemIds::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	if(refTypeStrs.size() != refIds.size() || refTypeStrs.size() != refRoles.size())
		throw std::invalid_argument("Length of ref vectors must be equal");
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
	return out.StoreRelation(objId, metaData, tags, refTypeStrs, refIds, refRoles);
}

// ****************************************************

FilterObjectsUnique::FilterObjectsUnique(std::shared_ptr<IDataStreamHandler> enc): enc(enc)
{

}

FilterObjectsUnique::~FilterObjectsUnique()
{

}

bool FilterObjectsUnique::Sync()
{
	return enc->Sync();
}

bool FilterObjectsUnique::Reset()
{
	return enc->Reset();
}

bool FilterObjectsUnique::Finish()
{
	return enc->Finish();
}

bool FilterObjectsUnique::StoreIsDiff(bool isDiff)
{
	return enc->StoreIsDiff(isDiff);
}

bool FilterObjectsUnique::StoreBounds(double x1, double y1, double x2, double y2)
{
	return enc->StoreBounds(x1, y1, x2, y2);
}

bool FilterObjectsUnique::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	auto it = this->nodeIds.find(objId);
	if(it == this->nodeIds.end())
	{
		this->nodeIds.insert(objId);
		return enc->StoreNode(objId, metaData, 
			tags, lat, lon);
	}
	return false;
}

bool FilterObjectsUnique::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	auto it = this->wayIds.find(objId);
	if(it == this->wayIds.end())
	{
		this->wayIds.insert(objId);
		return enc->StoreWay(objId, metaData, 
			tags, refs);
	}
	return false;
}

bool FilterObjectsUnique::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	auto it = this->relationIds.find(objId);
	if(it == this->relationIds.end())
	{
		this->relationIds.insert(objId);
		return enc->StoreRelation(objId, metaData, tags, 
			refTypeStrs, refIds, 
			refRoles);
	}
	return false;
}

