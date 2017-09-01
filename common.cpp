#include "common.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include "util.h"
#include <assert.h>
#include <time.h>
using namespace std;

// *********** Filters and decorators *****************

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

// ****************** Encoding, decoding and converting of objects *****************

void DecodeMetadata(const pqxx::result::const_iterator &c, const MetaDataCols &metaDataCols, class MetaData &metaData)
{
	metaData.version = c[metaDataCols.versionCol].as<uint64_t>();
	metaData.timestamp = c[metaDataCols.timestampCol].as<int64_t>();
	metaData.changeset = c[metaDataCols.changesetCol].as<int64_t>();
	if (c[metaDataCols.uidCol].is_null())
		metaData.uid = 0;
	else
		metaData.uid = c[metaDataCols.uidCol].as<uint64_t>();
	if (c[metaDataCols.usernameCol].is_null())
		metaData.username = "";
	else
		metaData.username = c[metaDataCols.usernameCol].c_str();
}

void DecodeTags(const pqxx::result::const_iterator &c, int tagsCol, JsonToStringMap &handler)
{
	handler.tagMap.clear();
	string tagsJson = c[tagsCol].as<string>();
	if (tagsJson != "{}")
	{
		Reader reader;
		StringStream ss(tagsJson.c_str());
		reader.Parse(ss, handler);
	}
}

void DecodeWayMembers(const pqxx::result::const_iterator &c, int membersCol, JsonToWayMembers &handler)
{
	handler.refs.clear();
	string memsJson = c[membersCol].as<string>();
	if (memsJson != "{}")
	{
		Reader reader;
		StringStream ss(memsJson.c_str());
		reader.Parse(ss, handler);
	}
}

void DecodeRelMembers(const pqxx::result::const_iterator &c, int membersCol, int memberRolesCols, 
	JsonToRelMembers &handler, JsonToRelMemberRoles &roles)
{
	handler.refTypeStrs.clear();
	handler.refIds.clear();
	roles.refRoles.clear();

	string memsJson = c[membersCol].as<string>();
	if (memsJson != "{}")
	{
		Reader reader;
		StringStream ss(memsJson.c_str());
		reader.Parse(ss, handler);
	}

	memsJson = c[memberRolesCols].as<string>();
	if (memsJson != "{}")
	{
		Reader reader;
		StringStream ss(memsJson.c_str());
		reader.Parse(ss, roles);
	}
}

void NodeResultsToEncoder(pqxx::icursorstream &cursor, IDataStreamHandler &enc)
{
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap tagHandler;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	bool verbose = false;

	for ( size_t batch = 0; true; batch ++ )
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read
		if(batch == 0 and verbose)
		{
			size_t numCols = rows.columns();
			for(size_t i = 0; i < numCols; i++)
			{
				cout << i << "\t" << rows.column_name(i) << "\t" << (unsigned int)rows.column_type((pqxx::tuple::size_type)i) << endl;
			}
		}

		MetaDataCols metaDataCols;

		int idCol = rows.column_number("id");
		metaDataCols.changesetCol = rows.column_number("changeset");
		metaDataCols.usernameCol = rows.column_number("username");
		metaDataCols.uidCol = rows.column_number("uid");
		int visibleCol = rows.column_number("visible");
		metaDataCols.timestampCol = rows.column_number("timestamp");
		metaDataCols.versionCol = rows.column_number("version");
		int currentCol = rows.column_number("current");
		int tagsCol = rows.column_number("tags");
		int latCol = rows.column_number("lat");
		int lonCol = rows.column_number("lon");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

			bool visible = c[visibleCol].as<bool>();
			bool current = c[currentCol].as<bool>();
			assert(visible && current);

			int64_t objId = c[idCol].as<int64_t>();
			double lat = atof(c[latCol].c_str());
			double lon = atof(c[lonCol].c_str());

			DecodeMetadata(c, metaDataCols, metaData);
			
			DecodeTags(c, tagsCol, tagHandler);

			count ++;
			if(count % 1000000 == 0 and verbose)
				cout << count << " nodes" << endl;

			double timeNow = (double)clock() / CLOCKS_PER_SEC;
			if (timeNow - lastUpdateTime > 30.0)
			{
				if(verbose)
					cout << (count - lastUpdateCount)/30.0 << " nodes/sec" << endl;
				lastUpdateCount = count;
				lastUpdateTime = timeNow;
			}

			enc.StoreNode(objId, metaData, tagHandler.tagMap, lat, lon);
		}
	}
}

void WayResultsToEncoder(pqxx::icursorstream &cursor, IDataStreamHandler &enc)
{
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap tagHandler;
	JsonToWayMembers wayMemHandler;
	const std::vector<int64_t> refs;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	bool verbose = false;
	for ( size_t batch = 0; true; batch ++ )
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read
		if(batch == 0 and verbose)
		{
			size_t numCols = rows.columns();
			for(size_t i = 0; i < numCols; i++)
			{
				cout << i << "\t" << rows.column_name(i) << "\t" << (unsigned int)rows.column_type((pqxx::tuple::size_type)i) << endl;
			}
		}

		MetaDataCols metaDataCols;

		int idCol = rows.column_number("id");
		metaDataCols.changesetCol = rows.column_number("changeset");
		metaDataCols.usernameCol = rows.column_number("username");
		metaDataCols.uidCol = rows.column_number("uid");
		int visibleCol = rows.column_number("visible");
		metaDataCols.timestampCol = rows.column_number("timestamp");
		metaDataCols.versionCol = rows.column_number("version");
		int currentCol = rows.column_number("current");
		int tagsCol = rows.column_number("tags");
		int membersCol = rows.column_number("members");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

			bool visible = c[visibleCol].as<bool>();
			bool current = c[currentCol].as<bool>();
			assert(visible && current);

			int64_t objId = c[idCol].as<int64_t>();

			DecodeMetadata(c, metaDataCols, metaData);
			
			DecodeTags(c, tagsCol, tagHandler);

			DecodeWayMembers(c, membersCol, wayMemHandler);

			count ++;
			if(count % 1000000 == 0 and verbose)
				cout << count << " ways" << endl;

			double timeNow = (double)clock() / CLOCKS_PER_SEC;
			if (timeNow - lastUpdateTime > 30.0)
			{
				if(verbose)
					cout << (count - lastUpdateCount)/30.0 << " ways/sec" << endl;
				lastUpdateCount = count;
				lastUpdateTime = timeNow;
			}

			enc.StoreWay(objId, metaData, tagHandler.tagMap, wayMemHandler.refs);
		}

	}
}

void RelationResultsToEnc(pqxx::icursorstream &cursor, const set<int64_t> &skipIds, IDataStreamHandler &enc)
{
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap tagHandler;
	JsonToRelMembers relMemHandler;
	JsonToRelMemberRoles relMemRolesHandler;
	std::vector<std::string> refRoles;
	const std::vector<int64_t> refs;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	bool verbose = false;
	for ( size_t batch = 0; true; batch ++ )
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read
		if(batch == 0 and verbose)
		{
			size_t numCols = rows.columns();
			for(size_t i = 0; i < numCols; i++)
			{
				cout << i << "\t" << rows.column_name(i) << "\t" << (unsigned int)rows.column_type((pqxx::tuple::size_type)i) << endl;
			}
		}

		MetaDataCols metaDataCols;

		int idCol = rows.column_number("id");
		metaDataCols.changesetCol = rows.column_number("changeset");
		metaDataCols.usernameCol = rows.column_number("username");
		metaDataCols.uidCol = rows.column_number("uid");
		int visibleCol = rows.column_number("visible");
		metaDataCols.timestampCol = rows.column_number("timestamp");
		metaDataCols.versionCol = rows.column_number("version");
		int currentCol = rows.column_number("current");
		int tagsCol = rows.column_number("tags");
		int membersCol = rows.column_number("members");
		int membersRolesCol = rows.column_number("memberroles");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

			bool visible = c[visibleCol].as<bool>();
			bool current = c[currentCol].as<bool>();
			assert(visible && current);

			int64_t objId = c[idCol].as<int64_t>();
			if(skipIds.find(objId) != skipIds.end())
				continue;

			DecodeMetadata(c, metaDataCols, metaData);
			
			DecodeTags(c, tagsCol, tagHandler);

			DecodeRelMembers(c, membersCol, membersRolesCol, 
				relMemHandler, relMemRolesHandler);

			count ++;
			if(count % 1000000 == 0 and verbose)
				cout << count << " relations" << endl;

			double timeNow = (double)clock() / CLOCKS_PER_SEC;
			if (timeNow - lastUpdateTime > 30.0)
			{
				if(verbose)
					cout << (count - lastUpdateCount)/30.0 << " relations/sec" << endl;
				lastUpdateCount = count;
				lastUpdateTime = timeNow;
			}

			enc.StoreRelation(objId, metaData, tagHandler.tagMap, 
				relMemHandler.refTypeStrs, relMemHandler.refIds, relMemRolesHandler.refRoles);
		}
	}
}

// ************* Basic API methods ***************

void GetLiveNodesInBbox(pqxx::work &work, std::map<string, string> &config, 
	const std::vector<double> &bbox, IDataStreamHandler &enc)
{
	if(bbox.size() != 4)
		throw invalid_argument("Bbox has wrong length");

	stringstream sql;
	sql << "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << config["dbtableprefix"];
	sql << "livenodes WHERE geom && ST_MakeEnvelope(";
	sql << bbox[0] <<","<< bbox[1] <<","<< bbox[2] <<","<< bbox[3] << ", 4326);";

	pqxx::icursorstream cursor( work, sql.str(), "nodesinbbox", 1000 );	

	NodeResultsToEncoder(cursor, enc);
}

void GetLiveWaysThatContainNodes(pqxx::work &work, std::map<string, string> &config, 
	const std::set<int64_t> &nodeIds, IDataStreamHandler &enc)
{
	string wayTable = config["dbtableprefix"] + "liveways";
	string wayMemTable = config["dbtableprefix"] + "way_mems";
	int step = 1000;	

	auto it=nodeIds.begin();
	while(it != nodeIds.end())
	{
		stringstream sqlFrags;
		int count = 0;
		for(; it != nodeIds.end() && count < step; it++)
		{
			if(count >= 1)
				sqlFrags << " OR ";
			sqlFrags << wayMemTable << ".member = " << *it;
			count ++;
		}

		string sql = "SELECT "+wayTable+".* FROM "+wayMemTable+" INNER JOIN "+wayTable+" ON "+wayMemTable+".id = "+wayTable+".id AND "+wayMemTable+".version = "+wayTable+".version WHERE ("+sqlFrags.str()+");";

		pqxx::icursorstream cursor( work, sql, "wayscontainingnodes", 1000 );	

		WayResultsToEncoder(cursor, enc);
	}
}

void GetLiveNodesById(pqxx::work &work, std::map<string, string> &config, 
	const std::set<int64_t> &nodeIds, IDataStreamHandler &enc)
{
	string nodeTable = config["dbtableprefix"] + "livenodes";
	int step = 1000;

	auto it=nodeIds.begin();
	while(it != nodeIds.end())
	{
		stringstream sqlFrags;
		int count = 0;
		for(; it != nodeIds.end() && count < step; it++)
		{
			if(count >= 1)
				sqlFrags << " OR ";
			sqlFrags << "id = " << *it;
			count ++;
		}

		string sql = "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM "+ nodeTable
			+ " WHERE ("+sqlFrags.str()+");";

		pqxx::icursorstream cursor( work, sql, "nodecursor", 1000 );	

		NodeResultsToEncoder(cursor, enc);
	}
}

void GetLiveRelationsForObjects(pqxx::work &work, std::map<string, string> &config, 
	char qtype, const set<int64_t> &qids, const set<int64_t> &skipIds, 
	IDataStreamHandler &enc)
{
	string relTable = config["dbtableprefix"] + "liverelations";
	string relMemTable = config["dbtableprefix"] + "relation_mems_" + qtype;
	cout << relMemTable << endl;
	int step = 1000;	

	auto it=qids.begin();
	while(it != qids.end())
	{
		stringstream sqlFrags;
		int count = 0;
		for(; it != qids.end() && count < step; it++)
		{
			if(count >= 1)
				sqlFrags << " OR ";
			sqlFrags << relMemTable << ".member = " << *it;
			count ++;
		}

		string sql = "SELECT "+relTable+".* FROM "+relMemTable+" INNER JOIN "+relTable+" ON "+relMemTable+".id = "+relTable+".id AND "+relMemTable+".version = "+relTable+".version WHERE ("+sqlFrags.str()+");";

		pqxx::icursorstream cursor( work, sql, "relationscontainingobjects", 1000 );	

		RelationResultsToEnc(cursor, skipIds, enc);
	}
}

// ************* Dump specific code *************

void DumpNodes(pqxx::work &work, std::map<string, string> &config, bool onlyLiveData, IDataStreamHandler &enc)
{
	cout << "Dump nodes" << endl;
	stringstream sql;
	if(onlyLiveData)
	{
		sql << "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
		sql << config["dbtableprefix"];
		sql << "livenodes;";
	}
	else
	{
		sql << "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
		sql << config["dbtableprefix"];
		sql << "nodes;";
	}

	pqxx::icursorstream cursor( work, sql.str(), "nodecursor", 1000 );	

	NodeResultsToEncoder(cursor, enc);
}

void DumpWays(pqxx::work &work, std::map<string, string> &config, bool onlyLiveData, IDataStreamHandler &enc)
{
	cout << "Dump ways" << endl;
	stringstream sql;
	sql << "SELECT * FROM ";
	sql << config["dbtableprefix"];
	sql << "ways";
	if(onlyLiveData)
		sql << " WHERE visible=true and current=true";
	sql << ";";

	pqxx::icursorstream cursor( work, sql.str(), "waycursor", 1000 );	

	WayResultsToEncoder(cursor, enc);
}

void DumpRelations(pqxx::work &work, std::map<string, string> &config, bool onlyLiveData, IDataStreamHandler &enc)
{
	cout << "Dump relations" << endl;
	stringstream sql;
	sql << "SELECT * FROM ";
	sql << config["dbtableprefix"];
	sql << "relations";
	if(onlyLiveData)
		sql << " WHERE visible=true and current=true";
	sql << ";";

	pqxx::icursorstream cursor( work, sql.str(), "relationcursor", 1000 );	

	set<int64_t> empty;
	RelationResultsToEnc(cursor, empty, enc);
}

