#include "cppGzip/DecodeGzip.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/OsmData.h"
#include <iostream>
#include <fstream>
#include "dbjson.h"
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

//https://stackoverflow.com/a/15372760/4288232
void StrReplaceAll( string &s, const string &search, const string &replace ) {
    for( size_t pos = 0; ; pos += replace.length() ) {
        // Locate the substring to replace
        pos = s.find( search, pos );
        if( pos == string::npos ) break;
        // Replace by erasing and inserting
        s.erase( pos, search.length() );
        s.insert( pos, replace );
    }
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
	livenodeFile.open(outPrefix+"livenodes.csv.gz", std::ios::out | std::ios::binary);
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
	//cout << "node" << endl;

	string tagsJson;
	EncodeTags(tags, tagsJson);
	StrReplaceAll(tagsJson, "\"","\"\"");
	tagsJson += "\n";
	
	livenodeFileGzip->sputn(tagsJson.c_str(), tagsJson.size());
}

void CsvStore::StoreWay(int64_t objId, const class MetaData &metaData, 
	const TagMap &tags, const std::vector<int64_t> &refs)
{
	//cout << "way" << endl;
}

void CsvStore::StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
	const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
	const std::vector<std::string> &refRoles)
{
	//cout << "relation" << endl;
}

/*
class CsvStore(object):


	def FuncStoreNode(self, objectId, metaData, tags, pos):
		version, timestamp, changeset, uid, username, visible, current = metaData
		tagDump = json.dumps(tags)
		tagDump = tagDump.replace('"', '""')
		tagDump = tagDump.replace('\u00b0', '')
		if username is not None:
			username = '"'+username.replace('"', '""')+'"'
		else:
			username = "NULL"
		if uid is None:
			uid = "NULL"
		if changeset is None:
			changeset = "NULL"
		if timestamp is not None:
			timestamp = timestamp.strftime("%s")
		else:
			timestamp = "NULL"
		if visible is None: visible = True
		if current is None: current = True
		changesetIndex = "NULL"
		
		if current and visible:
			li = u'{0},{3},{4},{5},{6},{7},{8},\"{9}\",SRID=4326;POINT({1} {2})\n'. \
				format(objectId, pos[1], pos[0], changeset, changesetIndex, username, uid, \
				timestamp, version, tagDump).encode("UTF-8")
			self.livenodeFile.write(li)
		else:
			li = u'{0},{3},{4},{5},{6},{7},{8},{9},\"{10}\",SRID=4326;POINT({1} {2})\n'. \
				format(objectId, pos[1], pos[0], changeset, changesetIndex, username, uid, visible, \
				timestamp, version, tagDump).encode("UTF-8")
			self.oldnodeFile.write(li)

		self.nodeIdsFile.write("{0}\n".format(objectId))

	def FuncStoreWay(self, objectId, metaData, tags, refs):
		version, timestamp, changeset, uid, username, visible, current = metaData
		tagDump = json.dumps(tags)
		tagDump = tagDump.replace('"', '""')
		if username is not None:
			username = '"'+username.replace('"', '""')+'"'
		else:
			username = "NULL"
		if uid is None:
			uid = "NULL"
		if changeset is None:
			changeset = "NULL"
		if timestamp is not None:
			timestamp = timestamp.strftime("%s")
		else:
			timestamp = "NULL"

		memDump= json.dumps(refs)
		if visible is None: visible = True
		if current is None: current = True
		changesetIndex = "NULL"

		if current and visible:
			li = u'{0},{1},{2},{3},{4},{5},{6},\"{7}\",\"{8}\"\n'. \
				format(objectId, changeset, changesetIndex, username, uid, \
				timestamp, version, tagDump, memDump).encode("UTF-8")
			self.livewayFile.write(li)
		else:
			li = u'{0},{1},{2},{3},{4},{5},{6},{7},\"{8}\",\"{9}\"\n'. \
				format(objectId, changeset, changesetIndex, username, uid, visible, \
				timestamp, version, tagDump, memDump).encode("UTF-8")
			self.oldwayFile.write(li)

		self.wayIdsFile.write("{0}\n".format(objectId))
		for i, mem in enumerate(refs):
			self.wayMembersFile.write("{0},{1},{2},{3}\n".format(objectId, version, i, mem))

	def FuncStoreRelation(self, objectId, metaData, tags, refs):
		version, timestamp, changeset, uid, username, visible, current = metaData
		tagDump = json.dumps(tags)
		tagDump = tagDump.replace('"', '""')
		if username is not None:
			username = u'"'+username.replace(u'"', u'""')+u'"'
		else:
			username = u"NULL"
		if uid is None:
			uid = "NULL"
		if changeset is None:
			changeset = "NULL"
		if timestamp is not None:
			timestamp = timestamp.strftime("%s")
		else:
			timestamp = "NULL"

		memDump= json.dumps([mem[:2] for mem in refs])
		memDump = memDump.replace('"', '""')
		rolesDump= json.dumps([mem[2] for mem in refs])
		rolesDump = rolesDump.replace('"', '""')
		if visible is None: visible = True
		if current is None: current = True
		changesetIndex = "NULL"

		if current and visible:
			li = u'{0},{1},{2},{3},{4},{5},{6},\"{7}\",\"{8}\",\"{9}\"\n'. \
				format(objectId, changeset, changesetIndex, username, uid, \
				timestamp, version, tagDump, memDump, rolesDump).encode("UTF-8")
			self.liverelationFile.write(li)
		else:
			li = u'{0},{1},{2},{3},{4},{5},{6},{7},\"{8}\",\"{9}\",\"{10}\"\n'. \
				format(objectId, changeset, changesetIndex, username, uid, visible, \
				timestamp, version, tagDump, memDump, rolesDump).encode("UTF-8")
			self.oldrelationFile.write(li)

		self.relationIdsFile.write("{0}\n".format(objectId))
		for i, (memTy, memId, memRole) in enumerate(refs):
			if memTy == "node":
				self.relationMemNodesFile.write("{0},{1},{2},{3}\n".format(objectId, version, i, memId))
			elif memTy == "way":
				self.relationMemWaysFile.write("{0},{1},{2},{3}\n".format(objectId, version, i, memId))
			elif memTy == "relation":
				self.relationMemRelsFile.write("{0},{1},{2},{3}\n".format(objectId, version, i, memId))
*/

int main(int argc, char **argv)
{
	//Perform gzip decoding
	std::filebuf fb;
	fb.open("/home/tim/dev/osm2pgcopy/fosm-portsmouth-2017.o5m.gz", std::ios::in | std::ios::binary);

	class DecodeGzip decodeGzip(fb);

	shared_ptr<class IDataStreamHandler> csvStore(new class CsvStore("/home/tim/dev/osm2pgcopy/test-"));
	LoadFromO5m(decodeGzip, csvStore);

	csvStore.reset();
	cout << "All done!" << endl;
}

