#ifndef _DB_FILTERS_H
#define _DB_FILTERS_H

#include <memory>
#include <set>
#include "cppo5m/model.h"

///Passes a stream through, remembering the IDs of the objects in it.
class DataStreamRetainIds : public IDataStreamHandler
{
public:
	std::set<int64_t> nodeIds, wayIds, relationIds;
	IDataStreamHandler &out;

	DataStreamRetainIds(IDataStreamHandler &out);

	void StoreIsDiff(bool) override;
	void StoreBounds(const Bounds &bounds) override;
	void StoreNode(const OsmNode &node) override;
	void StoreWay(const OsmWay &way) override;
	void StoreRelation(const OsmRelation &relation) override;
};

///Passes a stream through, remembering the IDs of the members of its ways and relations.
class DataStreamRetainMemIds : public IDataStreamHandler
{
public:
	std::set<int64_t> nodeIds, wayIds, relationIds;
	IDataStreamHandler &out;

	DataStreamRetainMemIds(IDataStreamHandler &out);

	void StoreIsDiff(bool) override;
	void StoreBounds(const Bounds &bounds) override;
	void StoreNode(const OsmNode &node) override;
	void StoreWay(const OsmWay &way) override;
	void StoreRelation(const OsmRelation &relation) override;
};

///Passes a stream through, dropping objects whose type and ID were already seen.
class FilterObjectsUnique : public IDataStreamHandler
{
public:
	FilterObjectsUnique(std::shared_ptr<IDataStreamHandler> enc);

	void Reset() override;
	void Finish() override;
	void StoreIsDiff(bool isDiff) override;
	void StoreBounds(const Bounds &bounds) override;
	void StoreNode(const OsmNode &node) override;
	void StoreWay(const OsmWay &way) override;
	void StoreRelation(const OsmRelation &relation) override;

private:
	std::set<int64_t> nodeIds, wayIds, relationIds;
	std::shared_ptr<IDataStreamHandler> enc;
};

#endif //_DB_FILTERS_H
