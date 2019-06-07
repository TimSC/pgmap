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
	out.StoreIsDiff(diff);
	return false;
}

bool DataStreamRetainIds::StoreBounds(double x1, double y1, double x2, double y2)
{
	out.StoreBounds(x1, y1, x2, y2);
	return false;
}

bool DataStreamRetainIds::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	out.StoreNode(objId, metaData, tags, lat, lon);
	this->nodeIds.insert(objId);
	return false;
}

bool DataStreamRetainIds::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	out.StoreWay(objId, metaData, tags, refs);
	this->wayIds.insert(objId);
	return false;
}

bool DataStreamRetainIds::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	if(refTypeStrs.size() != refIds.size() || refTypeStrs.size() != refRoles.size())
		throw std::invalid_argument("Length of ref vectors must be equal");
	out.StoreRelation(objId, metaData, tags, refTypeStrs, refIds, refRoles);
	this->relationIds.insert(objId);
	return false;
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
	out.StoreIsDiff(diff);
	return false;
}

bool DataStreamRetainMemIds::StoreBounds(double x1, double y1, double x2, double y2)
{
	out.StoreBounds(x1, y1, x2, y2);
	return false;
}

bool DataStreamRetainMemIds::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	out.StoreNode(objId, metaData, tags, lat, lon);
	return false;
}

bool DataStreamRetainMemIds::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	out.StoreWay(objId, metaData, tags, refs);
	for(size_t i=0; i < refs.size(); i++)
		this->nodeIds.insert(refs[i]);
	return false;
}

bool DataStreamRetainMemIds::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
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
	return false;
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
	enc->Sync();
	return false;
}

bool FilterObjectsUnique::Reset()
{
	enc->Reset();
	return false;
}

bool FilterObjectsUnique::Finish()
{
	enc->Finish();
	return false;
}

bool FilterObjectsUnique::StoreIsDiff(bool isDiff)
{
	enc->StoreIsDiff(isDiff);
	return false;
}

bool FilterObjectsUnique::StoreBounds(double x1, double y1, double x2, double y2)
{
	enc->StoreBounds(x1, y1, x2, y2);
	return false;
}

bool FilterObjectsUnique::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	auto it = this->nodeIds.find(objId);
	if(it == this->nodeIds.end())
	{
		enc->StoreNode(objId, metaData, 
			tags, lat, lon);
		this->nodeIds.insert(objId);
	}
	return false;
}

bool FilterObjectsUnique::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	auto it = this->wayIds.find(objId);
	if(it == this->wayIds.end())
	{
		enc->StoreWay(objId, metaData, 
			tags, refs);
		this->wayIds.insert(objId);
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
		enc->StoreRelation(objId, metaData, tags, 
			refTypeStrs, refIds, 
			refRoles);
		this->relationIds.insert(objId);
	}
	return false;
}

