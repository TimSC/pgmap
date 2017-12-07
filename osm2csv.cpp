#include "cppGzip/DecodeGzip.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/OsmData.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include "dbjson.h"
#include "util.h"
using namespace std;

/*
select * from pg_stat_activity;

SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM andorra_nodes WHERE geom && ST_MakeEnvelope(1.5020099, 42.5228903, 1.540173, 42.555443, 4326);
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO microcosm;

CREATE INDEX nodes_gin ON nodes USING GIN (tags);
SELECT * FROM nodes WHERE tags ? 'amenity' LIMIT 10;

Import from fosm.org 2015 dump
       table_name        | row_estimate | toast_bytes | table_bytes  |   total    |   index    |   toast    |   table    
-------------------------+--------------+-------------+--------------+------------+------------+------------+------------
 planet_nodes            |  1.36598e+09 |      131072 | 166346784768 | 252 GB     | 98 GB      | 128 kB     | 155 GB
 planet_ways             |  1.23554e+08 |   548945920 |  48414597120 | 48 GB      | 2647 MB    | 524 MB     | 45 GB
 planet_relations        |  1.39281e+06 |    14221312 |    653623296 | 1082 MB    | 445 MB     | 14 MB      | 623 MB
 planet_way_mems         |   1.6108e+09 |             |  84069392384 | 112 GB     | 34 GB      |            | 78 GB
 planet_relation_mems_w  |   1.2219e+07 |             |    637747200 | 870 MB     | 262 MB     |            | 608 MB
 planet_relation_mems_r  |       112434 |             |      5898240 | 8256 kB    | 2496 kB    |            | 5760 kB
 planet_relation_mems_n  |  1.78653e+06 |             |     93265920 | 127 MB     | 38 MB      |            | 89 MB

COPY planet_relations TO PROGRAM 'gzip > /home/postgres/dumprelations.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');
*/

inline string Int64ToStr(int64_t val)
{
	stringstream ss;
	ss << val;
	return ss.str();
}

class CsvStore : public IDataStreamHandler
{
private:
	std::filebuf livenodeFile, livewayFile, liverelationFile, oldnodeFile, oldwayFile, oldrelationFile;
	std::filebuf nodeIdsFile, wayIdsFile, relationIdsFile;
	std::filebuf wayMembersFile, relationMemNodesFile, relationMemWaysFile, relationMemRelsFile;
	std::shared_ptr<class EncodeGzip> livenodeFileGzip, livewayFileGzip, liverelationFileGzip, oldnodeFileGzip, oldwayFileGzip, oldrelationFileGzip;
	std::shared_ptr<class EncodeGzip> nodeIdsFileGzip, wayIdsFileGzip, relationIdsFileGzip;
	std::shared_ptr<class EncodeGzip> wayMembersFileGzip, relationMemNodesFileGzip, relationMemWaysFileGzip, relationMemRelsFileGzip;

public:
	CsvStore(const std::string &outPrefix);
	virtual ~CsvStore();

	virtual void Sync() {};
	virtual void Reset() {};
	virtual void Finish();

	virtual void StoreIsDiff(bool) {};
	virtual void StoreBounds(double x1, double y1, double x2, double y2) {};
	virtual void StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon);
	virtual void StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs);
	virtual void StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles);

};

CsvStore::CsvStore(const std::string &outPrefix)
{
	cout << outPrefix+"livenodes.csv.gz" << endl;
	livenodeFile.open(outPrefix+"livenodes.csv.gz", std::ios::out | std::ios::binary);
	if(!livenodeFile.is_open()) throw runtime_error("Error opening output");
	livenodeFileGzip.reset(new class EncodeGzip(livenodeFile));
	livewayFile.open(outPrefix+"liveways.csv.gz", std::ios::out | std::ios::binary);
	livewayFileGzip.reset(new class EncodeGzip(livewayFile));
	liverelationFile.open(outPrefix+"liverelations.csv.gz", std::ios::out | std::ios::binary);
	liverelationFileGzip.reset(new class EncodeGzip(liverelationFile));

	oldnodeFile.open(outPrefix+"oldnodes.csv.gz", std::ios::out | std::ios::binary);
	oldnodeFileGzip.reset(new class EncodeGzip(oldnodeFile));
	oldwayFile.open(outPrefix+"oldways.csv.gz", std::ios::out | std::ios::binary);
	oldwayFileGzip.reset(new class EncodeGzip(oldwayFile));
	oldrelationFile.open(outPrefix+"oldrelations.csv.gz", std::ios::out | std::ios::binary);
	oldrelationFileGzip.reset(new class EncodeGzip(oldrelationFile));

	nodeIdsFile.open(outPrefix+"nodeids.csv.gz", std::ios::out | std::ios::binary);
	nodeIdsFileGzip.reset(new class EncodeGzip(nodeIdsFile));
	wayIdsFile.open(outPrefix+"wayids.csv.gz", std::ios::out | std::ios::binary);
	wayIdsFileGzip.reset(new class EncodeGzip(wayIdsFile));
	relationIdsFile.open(outPrefix+"relationids.csv.gz", std::ios::out | std::ios::binary);
	relationIdsFileGzip.reset(new class EncodeGzip(relationIdsFile));

	wayMembersFile.open(outPrefix+"waymems.csv.gz", std::ios::out | std::ios::binary);
	wayMembersFileGzip.reset(new class EncodeGzip(wayMembersFile));
	relationMemNodesFile.open(outPrefix+"relationmems-n.csv.gz", std::ios::out | std::ios::binary);
	relationMemNodesFileGzip.reset(new class EncodeGzip(relationMemNodesFile));
	relationMemWaysFile.open(outPrefix+"relationmems-w.csv.gz", std::ios::out | std::ios::binary);
	relationMemWaysFileGzip.reset(new class EncodeGzip(relationMemWaysFile));
	relationMemRelsFile.open(outPrefix+"relationmems-r.csv.gz", std::ios::out | std::ios::binary);
	relationMemRelsFileGzip.reset(new class EncodeGzip(relationMemRelsFile));
}

CsvStore::~CsvStore()
{
	livenodeFileGzip.reset();
	livenodeFile.close();
	livewayFileGzip.reset();
	livewayFile.close();
	liverelationFileGzip.reset();
	liverelationFile.close();

	oldnodeFileGzip.reset();
	oldnodeFile.close();
	oldwayFileGzip.reset();
	oldwayFile.close();
	oldrelationFileGzip.reset();
	oldrelationFile.close();

	nodeIdsFileGzip.reset();
	nodeIdsFile.close();
	wayIdsFileGzip.reset();
	wayIdsFile.close();
	relationIdsFileGzip.reset();
	relationIdsFile.close();

	wayMembersFileGzip.reset();
	wayMembersFile.close();
	relationMemNodesFileGzip.reset();
	relationMemNodesFile.close();
	relationMemWaysFileGzip.reset();
	relationMemWaysFile.close();
	relationMemRelsFileGzip.reset();
	relationMemRelsFile.close();
}

void CsvStore::Finish()
{

}

void CsvStore::StoreNode(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, double lat, double lon)
{
	string tagsJson;
	EncodeTags(tags, tagsJson);
	StrReplaceAll(tagsJson, "\"", "\"\"");
	
	string usernameStr = metaData.username;
	if(metaData.username.size() > 0)
	{
		StrReplaceAll(usernameStr, "\"", "\"\"");
		usernameStr = "\""+usernameStr+"\"";
	}
	else
		usernameStr = "NULL";
	string uidStr;
	if(metaData.uid!=0)
		uidStr = Int64ToStr(metaData.uid);
	else
		uidStr = "NULL";
	string changesetStr = Int64ToStr(metaData.changeset);
	if(metaData.changeset==0)
		changesetStr="NULL";
	string timestampStr = Int64ToStr(metaData.timestamp);
	if(metaData.timestamp==0)
		timestampStr="NULL";
	string visibleStr = metaData.visible ? "true" : "false";
	std::string changesetIndex="NULL";
	
	stringstream ss;
	ss.precision(9);
	if(metaData.current and metaData.visible)
	{
		ss << objId <<","<< changesetStr <<","<< changesetIndex <<","<< usernameStr <<","<< uidStr <<","<< \
			timestampStr <<","<< metaData.version <<",\"" << tagsJson << "\",SRID=4326;POINT("<<lon<<" "<<lat<<")\n";
		string row(ss.str());
		this->livenodeFileGzip->sputn(row.c_str(), row.size());
	}
	else
	{
		ss << objId <<","<< changesetStr <<","<< changesetIndex <<","<< usernameStr <<","<< uidStr <<","<< visibleStr <<","<<\
			timestampStr <<","<< metaData.version <<",\"" << tagsJson << "\",SRID=4326;POINT("<<lon<<" "<<lat<<")\n";
		string row(ss.str());
		this->oldnodeFileGzip->sputn(row.c_str(), row.size());
	}

	string objIdStr = Int64ToStr(objId);
	objIdStr += "\n";
	this->nodeIdsFileGzip->sputn(objIdStr.c_str(), objIdStr.size());
}

void CsvStore::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	string tagsJson;
	EncodeTags(tags, tagsJson);
	StrReplaceAll(tagsJson, "\"", "\"\"");
	
	string refsJson;
	EncodeInt64Vec(refs, refsJson);
	StrReplaceAll(refsJson, "\"", "\"\"");

	string usernameStr = metaData.username;
	if(metaData.username.size() > 0)
	{
		StrReplaceAll(usernameStr, "\"", "\"\"");
		usernameStr = "\""+usernameStr+"\"";
	}
	else
		usernameStr = "NULL";
	string uidStr;
	if(metaData.uid!=0)
		uidStr = Int64ToStr(metaData.uid);
	else
		uidStr = "NULL";
	string changesetStr = Int64ToStr(metaData.changeset);
	if(metaData.changeset==0)
		changesetStr="NULL";
	string timestampStr = Int64ToStr(metaData.timestamp);
	if(metaData.timestamp==0)
		timestampStr="NULL";
	string visibleStr = metaData.visible ? "true" : "false";
	std::string changesetIndex="NULL";
	
	stringstream ss;
	if(metaData.current and metaData.visible)
	{
		ss << objId <<","<< changesetStr <<","<< changesetIndex <<","<< usernameStr <<","<< uidStr <<","<< \
			timestampStr <<","<< metaData.version <<",\"" << tagsJson << "\",\""<<refsJson<<"\"\n";
		string row(ss.str());
		this->livewayFileGzip->sputn(row.c_str(), row.size());
	}
	else
	{
		ss << objId <<","<< changesetStr <<","<< changesetIndex <<","<< usernameStr <<","<< uidStr <<","<< visibleStr <<","<<\
			timestampStr <<","<< metaData.version <<",\"" << tagsJson << "\",\""<<refsJson<<"\"\n";
		string row(ss.str());
		this->oldwayFileGzip->sputn(row.c_str(), row.size());
	}

	string objIdStr = Int64ToStr(objId);
	objIdStr += "\n";
	this->wayIdsFileGzip->sputn(objIdStr.c_str(), objIdStr.size());

	for(size_t i=0; i<refs.size(); i++)
	{
		stringstream ss2;
		ss2 << objId <<","<< metaData.version <<","<< i <<","<< refs[i] << "\n";
		string memStr(ss2.str());
		this->wayMembersFileGzip->sputn(memStr.c_str(), memStr.size());
	}
}

void CsvStore::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	string tagsJson;
	EncodeTags(tags, tagsJson);
	StrReplaceAll(tagsJson, "\"", "\"\"");
	
	string refsJson;
	EncodeRelationMems(refTypeStrs, refIds, refsJson);
	StrReplaceAll(refsJson, "\"", "\"\"");

	string refRolesJson;
	EncodeStringVec(refRoles, refRolesJson);
	StrReplaceAll(refRolesJson, "\"", "\"\"");

	string usernameStr = metaData.username;
	if(metaData.username.size() > 0)
	{
		StrReplaceAll(usernameStr, "\"", "\"\"");
		usernameStr = "\""+usernameStr+"\"";
	}
	else
		usernameStr = "NULL";
	string uidStr;
	if(metaData.uid!=0)
		uidStr = Int64ToStr(metaData.uid);
	else
		uidStr = "NULL";
	string changesetStr = Int64ToStr(metaData.changeset);
	if(metaData.changeset==0)
		changesetStr="NULL";
	string timestampStr = Int64ToStr(metaData.timestamp);
	if(metaData.timestamp==0)
		timestampStr="NULL";
	string visibleStr = metaData.visible ? "true" : "false";
	std::string changesetIndex="NULL";
	
	stringstream ss;
	if(metaData.current and metaData.visible)
	{
		ss << objId <<","<< changesetStr <<","<< changesetIndex <<","<< usernameStr <<","<< uidStr <<","<< \
			timestampStr <<","<< metaData.version <<",\"" << tagsJson << "\",\""<<refsJson<<"\",\""<<refRolesJson<<"\"\n";
		string row(ss.str());
		this->liverelationFileGzip->sputn(row.c_str(), row.size());
	}
	else
	{
		ss << objId <<","<< changesetStr <<","<< changesetIndex <<","<< usernameStr <<","<< uidStr <<","<< visibleStr <<","<<\
			timestampStr <<","<< metaData.version <<",\"" << tagsJson << "\",\""<<refsJson<<"\",\""<<refRolesJson<<"\"\n";
		string row(ss.str());
		this->oldrelationFileGzip->sputn(row.c_str(), row.size());
	}

	string objIdStr = Int64ToStr(objId);
	objIdStr += "\n";
	this->relationIdsFileGzip->sputn(objIdStr.c_str(), objIdStr.size());

	for(size_t i=0; i<refIds.size(); i++)
	{
		const std::string &refTypeStr = refTypeStrs[i];
		stringstream ss2;
		ss2 << objId <<","<< metaData.version <<","<< i <<","<< refIds[i] << "\n";
		string memStr(ss2.str());

		if(refTypeStr == "node")
		{
			this->relationMemNodesFileGzip->sputn(memStr.c_str(), memStr.size());
		}
		else if(refTypeStr == "way")
		{
			this->relationMemWaysFileGzip->sputn(memStr.c_str(), memStr.size());
		}
		else if(refTypeStr == "relation")
		{
			this->relationMemRelsFileGzip->sputn(memStr.c_str(), memStr.size());
		}
	}
}

int main(int argc, char **argv)
{
	cout << "Reading settings from config.cfg" << endl;
	std::map<string, string> config;
	ReadSettingsFile("config.cfg", config);

	//Perform gzip decoding
	std::filebuf fb;
	fb.open(config["dump_path"], std::ios::in | std::ios::binary);
	if(!fb.is_open())
	{
		cout << "Error opening input file " << config["dump_path"] << endl;
		exit(0);
	}
	if(fb.in_avail() == 0)
	{
		cout << "Error reading from input file " << config["dump_path"] << endl;
		exit(0);
	}
	cout << "Reading from input " << config["dump_path"] << endl;

	class DecodeGzip decodeGzip(fb);
	if(decodeGzip.in_avail() == 0)
	{
		cout << "Error reading from input file" << endl;
		exit(0);
	}

	cout << "Writing output to " << config["csv_absolute_path"] << endl;
	shared_ptr<class IDataStreamHandler> csvStore(new class CsvStore(config["csv_absolute_path"]));
	LoadFromO5m(decodeGzip, csvStore);

	csvStore.reset();
	cout << "All done!" << endl;
}

