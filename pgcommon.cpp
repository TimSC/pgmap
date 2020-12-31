#include "pgcommon.h"
#include "dbquery.h"
#include <stdexcept>
#include <iostream>
using namespace std;

// ************************************************

PgWork::PgWork()
{

}

PgWork::PgWork(pqxx::transaction_base *workIn):
	work(workIn)
{
	
}

PgWork::PgWork(const PgWork &obj)
{
	*this = obj;
}

PgWork::~PgWork()
{
	work.reset();
}

PgWork& PgWork::operator=(const PgWork &obj)
{
	work = obj.work;
	return *this;
}

// *************************************************

PgCommon::PgCommon(shared_ptr<pqxx::connection> dbconnIn,
	const string &tableStaticPrefixIn, 
	const string &tableActivePrefixIn,
	std::shared_ptr<class PgWork> sharedWorkIn,
	const std::string &shareMode):

	sharedWork(sharedWorkIn),
	dbUsernameLookup(*dbconnIn.get(), sharedWork->work.get(), tableStaticPrefixIn, tableActivePrefixIn)
{
	dbconn = dbconnIn;
	tableStaticPrefix = tableStaticPrefixIn;
	tableActivePrefix = tableActivePrefixIn;
	this->shareMode = shareMode;
}

PgCommon::~PgCommon()
{

}

bool PgCommon::IsAdminMode()
{
	return false;
}

void PgCommon::GetWaysForNodes(const std::set<int64_t> &objectIds, 
	std::shared_ptr<IDataStreamHandler> out)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	GetLiveWaysThatContainNodes(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableStaticPrefix, this->tableActivePrefix, objectIds, out);

	GetLiveWaysThatContainNodes(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, "", objectIds, out);
}

void PgCommon::GetRelationsForObjs(const std::string &type, const std::set<int64_t> &objectIds, 
	std::shared_ptr<IDataStreamHandler> out)
{
	if(type.size() == 0)
		throw invalid_argument("Type string cannot be zero length");
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	set<int64_t> empty;
	std::set<int64_t>::const_iterator it = objectIds.begin();
	while(it != objectIds.end())
	{
		GetLiveRelationsForObjects(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableStaticPrefix, 
			this->tableActivePrefix, 
			type[0], objectIds, it, 1000, empty, out);
	}
	it = objectIds.begin();
	while(it != objectIds.end())
	{
		GetLiveRelationsForObjects(*dbconn, work.get(), this->dbUsernameLookup,
			this->tableActivePrefix, "", 
			type[0], objectIds, it, 1000, empty, out);
	}
}

void PgCommon::GetAffectedParents(std::shared_ptr<class OsmData> inputObjects,
	std::shared_ptr<class OsmData> outputObjects)
{
	GetAffectedParents2(*inputObjects.get(), outputObjects);
}

void PgCommon::GetAffectedParents2(const class OsmData &inputObjects,
	std::shared_ptr<class OsmData> outAffectedObjects)
{
	if(!IsAdminMode() && this->shareMode != "ACCESS SHARE" && this->shareMode != "EXCLUSIVE")
		throw runtime_error("Database must be locked in ACCESS SHARE or EXCLUSIVE mode");

	//Get IDs of objects already known
	std::set<int64_t> inputNodeIds;
	//outAffectedObjects->nodes = inputObjects->nodes;
	for(size_t i=0; i<inputObjects.nodes.size(); i++)
		inputNodeIds.insert(inputObjects.nodes[i].objId);

	std::set<int64_t> inputWayIds;
	for(size_t i=0; i<inputObjects.ways.size(); i++)
		inputWayIds.insert(inputObjects.ways[i].objId);

	std::set<int64_t> inputRelationIds;
	for(size_t i=0; i<inputObjects.relations.size(); i++)
		inputRelationIds.insert(inputObjects.relations[i].objId);

	//For all nodes affected, find all parent ways and relations affected
	std::shared_ptr<class OsmData> parentObjs(new class OsmData());
	GetWaysForNodes(inputNodeIds, parentObjs);
	for(size_t i=0; i<parentObjs->ways.size(); i++)
	{
		class OsmWay &way = parentObjs->ways[i];
		auto it = inputWayIds.find(way.objId);
		if(it == inputWayIds.end())
		{
			inputWayIds.insert(way.objId);
			outAffectedObjects->ways.push_back(way);
		}
	}
	parentObjs->Clear();

	//For all ways affected, find all parent relations affected
	GetRelationsForObjs("node", inputNodeIds, parentObjs);
	GetRelationsForObjs("way", inputWayIds, parentObjs);
	for(size_t i=0; i<parentObjs->relations.size(); i++)
	{
		class OsmRelation &relation = parentObjs->relations[i];
		auto it = inputRelationIds.find(relation.objId);
		if(it == inputRelationIds.end())
		{
			inputRelationIds.insert(relation.objId);
			outAffectedObjects->relations.push_back(relation);
		}
	}

	//For all relations affected, recusively find parent relations
	std::set<int64_t> currentRelationIds = inputRelationIds;
	int depth = 0;
	while(currentRelationIds.size() > 0 and depth < 10)
	{
		std::shared_ptr<class OsmData> parentRelationObjs(new class OsmData());
		GetRelationsForObjs("relation", currentRelationIds, parentRelationObjs);
		
		currentRelationIds.clear();
		for(size_t i=0; i<parentRelationObjs->relations.size(); i++)
		{
			class OsmRelation &relation = parentRelationObjs->relations[i];
			auto it = inputRelationIds.find(relation.objId);
			if(it == inputRelationIds.end())
			{
				outAffectedObjects->relations.push_back(relation);
				currentRelationIds.insert(relation.objId);
				inputRelationIds.insert(relation.objId);
			}
		}
		depth ++;
	}

}

void PgCommon::GetObjectBboxes(const std::string &type, const std::set<int64_t> &objectIds,
	std::map<int64_t, vector<double> > &out)
{
	std::shared_ptr<pqxx::transaction_base> work(this->sharedWork->work);
	if(!work)
		throw runtime_error("Transaction has been deleted");

	GetVisibleObjectBboxesById(*dbconn, work.get(), this->dbUsernameLookup,
		this->tableActivePrefix, type, objectIds, out);
}

// **********************************************
