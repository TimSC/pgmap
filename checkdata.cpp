#include "cppGzip/DecodeGzip.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/OsmData.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include "dbjson.h"
#include "util.h"
#include "pgmap.h"
using namespace std;

class DataChecker : public IDataStreamHandler
{
private:
	map<int64_t, vector<int64_t> > nodeIdVers, wayIdVers, relationIdVers;
	std::shared_ptr<class PgTransaction> transaction;

	void NodesExists(const std::set<int64_t> &objectIds,
		std::set<int64_t> &exists, std::set<int64_t> &notExists);
	void WaysExists(const std::set<int64_t> &objectIds,
		std::set<int64_t> &exists, std::set<int64_t> &notExists);
	void RelationsExists(const std::set<int64_t> &objectIds,
		std::set<int64_t> &exists, std::set<int64_t> &notExists);

public:
	DataChecker(std::shared_ptr<class PgTransaction> transactionIn);
	virtual ~DataChecker();

	virtual bool Sync() {return false;};
	virtual bool Reset() {return false;};
	virtual bool Finish();

	virtual bool StoreIsDiff(bool) {return false;};
	virtual bool StoreBounds(double x1, double y1, double x2, double y2);
	virtual bool StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon);
	virtual bool StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	virtual bool StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles);

};

DataChecker::DataChecker(std::shared_ptr<class PgTransaction> transactionIn):
	transaction(transactionIn)
{

}

DataChecker::~DataChecker()
{

}

bool DataChecker::Finish()
{
	for(map<int64_t, vector<int64_t> >::iterator it = nodeIdVers.begin(); it != nodeIdVers.end(); it++)
	{
		if(it->second.size() > 1)
			cout << "Node " << it->first << " occurs " << it->second.size() << " times" << endl;
	}
	for(map<int64_t, vector<int64_t> >::iterator it = wayIdVers.begin(); it != wayIdVers.end(); it++)
	{
		if(it->second.size() > 1)
			cout << "Way " << it->first << " occurs " << it->second.size() << " times" << endl;
	}
	for(map<int64_t, vector<int64_t> >::iterator it = relationIdVers.begin(); it != relationIdVers.end(); it++)
	{
		if(it->second.size() > 1)
			cout << "Relation " << it->first << " occurs " << it->second.size() << " times" << endl;
	}
	return false;
}

bool DataChecker::StoreBounds(double x1, double y1, double x2, double y2)
{
	cout << "bounds " << x1 <<","<< y1 <<","<< x2 <<","<< y2 << endl;
	return false;
}

bool DataChecker::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	if(!transaction)
	{
		map<int64_t, vector<int64_t> >::iterator it = nodeIdVers.find(objId);
		if(it == nodeIdVers.end())
		{
			vector<int64_t> vers;
			vers.push_back(metaData.version);
			nodeIdVers[objId] = vers;
		}
		else
			it->second.push_back(metaData.version);
	}

	if(lat < -90 or lat > 90)
		cout << "Node " << objId << " lat out of range " << lat << endl;
	if(lon < -180 or lon > 180)
		cout << "Node " << objId << " lon out of range " << lon << endl;
	return false;
}

bool DataChecker::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	if(!transaction)
	{
		map<int64_t, vector<int64_t> >::iterator it = wayIdVers.find(objId);
		if(it == wayIdVers.end())
		{
			vector<int64_t> vers;
			vers.push_back(metaData.version);
			wayIdVers[objId] = vers;
		}
		else
		{
			it->second.push_back(metaData.version);
		}
	}

	//Check for too few nodes
	if (refs.size() < 2)
		cout << "Too few nodes in way: " << objId <<endl;

	//Check for missing nodes
	std::set<int64_t> nodeIds(refs.begin(), refs.end()), nodeExists, nodeNotExists;
	this->NodesExists(nodeIds, nodeExists, nodeNotExists);
	
	for(auto it=nodeNotExists.begin(); it!=nodeNotExists.end(); it++)
	{
		cout << "Node " << (*it) << " does not exist, but referenced by way: ";
		cout << objId << endl;
	}
	return false;
}

bool DataChecker::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	if(!transaction)
	{
		map<int64_t, vector<int64_t> >::iterator it = relationIdVers.find(objId);
		if(it == relationIdVers.end())
		{
			vector<int64_t> vers;
			vers.push_back(metaData.version);
			relationIdVers[objId] = vers;
		}
		else
			it->second.push_back(metaData.version);
	}

	if(refTypeStrs.size() != refIds.size() or refTypeStrs.size() != refRoles.size())
		throw runtime_error("Relation refs have inconsistent lengths");

	//Check for too few nodes
	if (refTypeStrs.size() == 0)
		cout << "Too few members in relation: " << objId <<endl;

	//Check for missing members
	std::set<int64_t> refNodeIds, refWayIds, refRelationIds, refExists, refNotExists;
	for(size_t i=0; i<refTypeStrs.size(); i++)
	{
		if(refTypeStrs[i] == "node")
			refNodeIds.insert(refIds[i]);
		else if(refTypeStrs[i] == "way")
			refWayIds.insert(refIds[i]);
		else if(refTypeStrs[i] == "relation")
			refRelationIds.insert(refIds[i]);
	}

	this->NodesExists(refNodeIds, refExists, refNotExists);	
	for(auto it=refNotExists.begin(); it!=refNotExists.end(); it++)
		cout << "Node " << (*it) << " does not exist, but referenced by relation: " << objId << endl;

	refExists.clear();
	refNotExists.clear();
	this->WaysExists(refWayIds, refExists, refNotExists);	
	for(auto it=refNotExists.begin(); it!=refNotExists.end(); it++)
		cout << "Way " << (*it) << " does not exist, but referenced by relation: " << objId << endl;

	refExists.clear();
	refNotExists.clear();
	this->RelationsExists(refRelationIds, refExists, refNotExists);	
	for(auto it=refNotExists.begin(); it!=refNotExists.end(); it++)
		cout << "Relation " << (*it) << " does not exist, but referenced by relation: " << objId << endl;

	return false;
}

void DataChecker::NodesExists(const std::set<int64_t> &objectIds,
	std::set<int64_t> &exists, std::set<int64_t> &notExists)
{
	if(objectIds.size()==0) return;

	if(!transaction)
	{
		for(auto it=objectIds.begin(); it!=objectIds.end(); it++)
		{
			if(nodeIdVers.find(*it) != nodeIdVers.end())
				exists.insert(*it);
			else
				notExists.insert(*it);
		}
	}
	else
	{
		shared_ptr<class OsmData> osmData(new class OsmData());
		transaction->GetObjectsById("node", objectIds, osmData);
		std::set<int64_t> inDatabase;
		for(size_t i=0; i<osmData->nodes.size(); i++)
			inDatabase.insert(osmData->nodes[i].objId);

		for(auto it=objectIds.begin(); it!=objectIds.end(); it++)
		{
			if(inDatabase.find(*it) != inDatabase.end())
				exists.insert(*it);
			else
				notExists.insert(*it);
		}
	}
}

void DataChecker::WaysExists(const std::set<int64_t> &objectIds,
	std::set<int64_t> &exists, std::set<int64_t> &notExists)
{
	if(objectIds.size()==0) return;

	if(!transaction)
	{
		for(auto it=objectIds.begin(); it!=objectIds.end(); it++)
		{
			if(wayIdVers.find(*it) != wayIdVers.end())
				exists.insert(*it);
			else
				notExists.insert(*it);
		}
	}
	else
	{
		shared_ptr<class OsmData> osmData(new class OsmData());
		transaction->GetObjectsById("way", objectIds, osmData);
		std::set<int64_t> inDatabase;
		for(size_t i=0; i<osmData->ways.size(); i++)
			inDatabase.insert(osmData->ways[i].objId);

		for(auto it=objectIds.begin(); it!=objectIds.end(); it++)
		{
			if(inDatabase.find(*it) != inDatabase.end())
				exists.insert(*it);
			else
				notExists.insert(*it);
		}
	}
}

void DataChecker::RelationsExists(const std::set<int64_t> &objectIds,
	std::set<int64_t> &exists, std::set<int64_t> &notExists)
{
	if(objectIds.size()==0) return;

	if(!transaction)
	{
		for(auto it=objectIds.begin(); it!=objectIds.end(); it++)
		{
			if(relationIdVers.find(*it) != relationIdVers.end())
				exists.insert(*it);
			else
				notExists.insert(*it);
		}
	}
	else
	{
		shared_ptr<class OsmData> osmData(new class OsmData());
		transaction->GetObjectsById("relation", objectIds, osmData);
		std::set<int64_t> inDatabase;
		for(size_t i=0; i<osmData->relations.size(); i++)
			inDatabase.insert(osmData->relations[i].objId);

		for(auto it=objectIds.begin(); it!=objectIds.end(); it++)
		{
			if(inDatabase.find(*it) != inDatabase.end())
				exists.insert(*it);
			else
				notExists.insert(*it);
		}
	}
}

int main(int argc, char **argv)
{
	std::shared_ptr<class PgTransaction> transaction;

	if(argc > 1)
	{
		string argv1 = argv[1];
		if (argv1!="db")
		{
			shared_ptr<class IDataStreamHandler> dataChecker(new class DataChecker(transaction));

			LoadOsmFromFile(argv[1], dataChecker);
		}
		else
		{
			cout << "Reading settings from config.cfg" << endl;
			std::map<string, string> config;
			ReadSettingsFile("config.cfg", config);

			string cstr = GeneratePgConnectionString(config);
			class PgMap pgMap(cstr, config["dbtableprefix"], config["dbtablemodifyprefix"], 
				config["dbtablemodifyprefix"], config["dbtabletestprefix"]);

			transaction = pgMap.GetTransaction("ACCESS SHARE");
			shared_ptr<class IDataStreamHandler> dataChecker(new class DataChecker(transaction));

			transaction->Dump(false, false, true, true, dataChecker);
		}
	}
	else
	{
		cout << "Please specify filename as program argument or use 'db' to scan database" << endl;
	}

	cout << "All done!" << endl;
}

