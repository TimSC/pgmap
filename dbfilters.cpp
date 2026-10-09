#include "dbfilters.h"
using namespace std;

DataStreamRetainIds::DataStreamRetainIds(IDataStreamHandler &outObj) : out(outObj)
{

}

void DataStreamRetainIds::StoreIsDiff(bool diff)
{
	out.StoreIsDiff(diff);
}

void DataStreamRetainIds::StoreBounds(const Bounds &bounds)
{
	out.StoreBounds(bounds);
}

void DataStreamRetainIds::StoreNode(const OsmNode &node)
{
	this->nodeIds.insert(node.objId);
	out.StoreNode(node);
}

void DataStreamRetainIds::StoreWay(const OsmWay &way)
{
	this->wayIds.insert(way.objId);
	out.StoreWay(way);
}

void DataStreamRetainIds::StoreRelation(const OsmRelation &relation)
{
	this->relationIds.insert(relation.objId);
	out.StoreRelation(relation);
}

// ******************************

DataStreamRetainMemIds::DataStreamRetainMemIds(IDataStreamHandler &outObj) : out(outObj)
{

}

void DataStreamRetainMemIds::StoreIsDiff(bool diff)
{
	out.StoreIsDiff(diff);
}

void DataStreamRetainMemIds::StoreBounds(const Bounds &bounds)
{
	out.StoreBounds(bounds);
}

void DataStreamRetainMemIds::StoreNode(const OsmNode &node)
{
	out.StoreNode(node);
}

void DataStreamRetainMemIds::StoreWay(const OsmWay &way)
{
	for(size_t i=0; i < way.refs.size(); i++)
		this->nodeIds.insert(way.refs[i]);
	out.StoreWay(way);
}

void DataStreamRetainMemIds::StoreRelation(const OsmRelation &relation)
{
	for(const RelationMember &member : relation.members)
	{
		switch(member.type)
		{
		case ObjectType::Node: this->nodeIds.insert(member.ref); break;
		case ObjectType::Way: this->wayIds.insert(member.ref); break;
		case ObjectType::Relation: this->relationIds.insert(member.ref); break;
		}
	}
	out.StoreRelation(relation);
}

// ****************************************************

FilterObjectsUnique::FilterObjectsUnique(std::shared_ptr<IDataStreamHandler> enc): enc(enc)
{

}

void FilterObjectsUnique::Reset()
{
	enc->Reset();
}

void FilterObjectsUnique::Finish()
{
	enc->Finish();
}

void FilterObjectsUnique::StoreIsDiff(bool isDiff)
{
	enc->StoreIsDiff(isDiff);
}

void FilterObjectsUnique::StoreBounds(const Bounds &bounds)
{
	enc->StoreBounds(bounds);
}

void FilterObjectsUnique::StoreNode(const OsmNode &node)
{
	if(this->nodeIds.insert(node.objId).second)
		enc->StoreNode(node);
}

void FilterObjectsUnique::StoreWay(const OsmWay &way)
{
	if(this->wayIds.insert(way.objId).second)
		enc->StoreWay(way);
}

void FilterObjectsUnique::StoreRelation(const OsmRelation &relation)
{
	if(this->relationIds.insert(relation.objId).second)
		enc->StoreRelation(relation);
}
