#include "cppGzip/DecodeGzip.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/OsmData.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include "dbjson.h"
#include "util.h"
using namespace std;

class DataChecker : public IDataStreamHandler
{
private:
	map<int64_t, vector<int64_t> > nodeIdVers, wayIdVers, relationIdVers;
	map<int64_t, vector<int64_t> > nodeMemForWays;
public:
	DataChecker();
	virtual ~DataChecker();

	virtual void Sync() {};
	virtual void Reset() {};
	virtual void Finish();

	virtual void StoreIsDiff(bool) {};
	virtual void StoreBounds(double x1, double y1, double x2, double y2);
	virtual void StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon);
	virtual void StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	virtual void StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles);

};

DataChecker::DataChecker()
{

}

DataChecker::~DataChecker()
{

}

void DataChecker::Finish()
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
	for(map<int64_t, vector<int64_t> >::iterator it = nodeMemForWays.begin(); it != nodeMemForWays.end(); it++)
	{
		map<int64_t, vector<int64_t> >::iterator it2 = nodeIdVers.find(it->first);
		if(it2 == nodeIdVers.end())
		{
			cout << "Node " << it->first << " does not exist, but referenced by ways: ";
			for(size_t i=0; i<it->second.size(); i++)
				cout << it->second[i] << "," << endl;
			cout << endl;
		}
	}
	for(map<int64_t, vector<int64_t> >::iterator it = relationIdVers.begin(); it != relationIdVers.end(); it++)
	{
		if(it->second.size() > 1)
			cout << "Relation " << it->first << " occurs " << it->second.size() << " times" << endl;
	}
}

virtual void DataChecker::StoreBounds(double x1, double y1, double x2, double y2)
{
	cout << "bounds " << x1 <<","<< y1 <<","<< x2 <<","<< y2 << endl;
}

void DataChecker::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
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

	if(lat < -90 or lat > 90)
		cout << "Node " << objId << " lat out of range " << lat << endl;
	if(lon < -180 or lon > 180)
		cout << "Node " << objId << " lon out of range " << lon << endl;
}

void DataChecker::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	map<int64_t, vector<int64_t> >::iterator it = wayIdVers.find(objId);
	if(it == wayIdVers.end())
	{
		vector<int64_t> vers;
		vers.push_back(metaData.version);
		wayIdVers[objId] = vers;
	}
	else
		it->second.push_back(metaData.version);

	for(size_t i=0; i<refs.size(); i++)
	{
		int64_t nid = refs[i];
		it = nodeMemForWays.find(nid);
		if(it == nodeMemForWays.end())
		{
			vector<int64_t> objIds;
			objIds.push_back(objId);
			wayIdVers[objId] = objIds;
		}
		else
			it->second.push_back(objId);
	}
}

void DataChecker::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
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

	//TODO check relation members exist
}

int main(int argc, char **argv)
{
	cout << "Reading settings from config.cfg" << endl;
	std::map<string, string> config;
	ReadSettingsFile("config.cfg", config);

	shared_ptr<class IDataStreamHandler> dataChecker(new class DataChecker());
	if(argc > 1)
		LoadOsmFromFile(argv[1], dataChecker);	

	cout << "All done!" << endl;
}

