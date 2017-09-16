#include "db.h"
#include <rapidjson/writer.h> //rapidjson-dev
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

void EncodeTags(const TagMap &tagmap, string &out)
{
	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer);
	writer.StartObject();
	for(auto it=tagmap.begin(); it!=tagmap.end(); it++)
	{
		writer.Key(it->first.c_str());
		writer.String(it->second.c_str());
	}
	writer.EndObject();
	out = buffer.GetString();
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

int NodeResultsToEncoder(pqxx::icursorstream &cursor, std::shared_ptr<IDataStreamHandler> enc)
{
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap tagHandler;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	bool verbose = false;

	pqxx::result rows;
	cursor.get(rows);
	if ( rows.empty() ) return 0; // nothing left to read

	MetaDataCols metaDataCols;

	int idCol = rows.column_number("id");
	metaDataCols.changesetCol = rows.column_number("changeset");
	metaDataCols.usernameCol = rows.column_number("username");
	metaDataCols.uidCol = rows.column_number("uid");
	//int visibleCol = rows.column_number("visible");
	metaDataCols.timestampCol = rows.column_number("timestamp");
	metaDataCols.versionCol = rows.column_number("version");
	//int currentCol = rows.column_number("current");
	int tagsCol = rows.column_number("tags");
	int latCol = rows.column_number("lat");
	int lonCol = rows.column_number("lon");

	for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

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

		enc->StoreNode(objId, metaData, tagHandler.tagMap, lat, lon);
	}
	return count;
}

int WayResultsToEncoder(pqxx::icursorstream &cursor, std::shared_ptr<IDataStreamHandler> enc)
{
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap tagHandler;
	JsonToWayMembers wayMemHandler;
	const std::vector<int64_t> refs;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	bool verbose = false;

	pqxx::result rows;
	cursor.get(rows);
	if ( rows.empty() ) return 0; // nothing left to read

	MetaDataCols metaDataCols;

	int idCol = rows.column_number("id");
	metaDataCols.changesetCol = rows.column_number("changeset");
	metaDataCols.usernameCol = rows.column_number("username");
	metaDataCols.uidCol = rows.column_number("uid");
	//int visibleCol = rows.column_number("visible");
	metaDataCols.timestampCol = rows.column_number("timestamp");
	metaDataCols.versionCol = rows.column_number("version");
	//int currentCol = rows.column_number("current");
	int tagsCol = rows.column_number("tags");
	int membersCol = rows.column_number("members");

	for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

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

		enc->StoreWay(objId, metaData, tagHandler.tagMap, wayMemHandler.refs);
	}
	return count;
}

void RelationResultsToEncoder(pqxx::icursorstream &cursor, const set<int64_t> &skipIds, std::shared_ptr<IDataStreamHandler> enc)
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
		//int visibleCol = rows.column_number("visible");
		metaDataCols.timestampCol = rows.column_number("timestamp");
		metaDataCols.versionCol = rows.column_number("version");
		//int currentCol = rows.column_number("current");
		int tagsCol = rows.column_number("tags");
		int membersCol = rows.column_number("members");
		int membersRolesCol = rows.column_number("memberroles");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

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

			enc->StoreRelation(objId, metaData, tagHandler.tagMap, 
				relMemHandler.refTypeStrs, relMemHandler.refIds, relMemRolesHandler.refRoles);
		}
	}
}

// *********** Convert to database SQL *************

bool NodesToDatabase(pqxx::connection &c, pqxx::work *work, const string &tablePrefix, 
	const std::vector<class OsmNode> &nodes, 
	std::map<int64_t, int64_t> &createdNodeIds,
	map<string, int64_t> &nextIdMap,
	std::string &errStr)
{
	char trueStr[] = "true";
	char falseStr[] = "true";
	auto it = nextIdMap.find("node");
	int64_t &nextNodeId = it->second;

	for(size_t i=0; i<nodes.size(); i++)
	{
		const class OsmNode &node = nodes[i];
		int64_t objId = node.objId;
		int64_t version = node.metaData.version;

		//Get existing node object (if any)
		string checkExistingSql = "SELECT id, changeset, username, uid, timestamp, version, tags, geom, ST_X(geom) as lon, ST_Y(geom) AS lat FROM "+ tablePrefix
			+ "livenodes WHERE (id=$1);";
		pqxx::result r;
		try
		{
			c.prepare(tablePrefix+"checknodeexists", checkExistingSql);
			r = work->prepared(tablePrefix+"checknodeexists")(objId).exec();
		}
		catch (const pqxx::sql_error &e)
		{
			errStr = e.what();
			return false;
		}
		catch (const std::exception &e)
		{
			errStr = e.what();
			return false;
		}

		bool foundExisting = r.size() > 0;
		int64_t currentVersion = -1;
		if(foundExisting)
		{
			const pqxx::result::tuple row = r[0];
			currentVersion = row[5].as<int64_t>();
			cout << "version " << currentVersion << endl;
		}

		//Check if we need to delete object from live table
		if(foundExisting && version >= currentVersion && !node.metaData.visible)
		{
			string checkExistingSql = "DELETE FROM "+ tablePrefix
				+ "livenodes WHERE (id=$1);";
			try
			{
				c.prepare(tablePrefix+"deletelivenode", checkExistingSql);
				work->prepared(tablePrefix+"deletelivenode")(objId).exec();
			}
			catch (const pqxx::sql_error &e)
			{
				errStr = e.what();
				return false;
			}
			catch (const std::exception &e)
			{
				errStr = e.what();
				return false;
			}
		}

		//Check if we need to copy row to history table
		if(foundExisting && version > currentVersion)
		{
			const pqxx::result::tuple row = r[0];

			//Insert into live table
			stringstream ss;
			ss << "INSERT INTO "<< tablePrefix <<"oldnodes (id, changeset, username, uid, timestamp, version, tags, visible, current, geom) VALUES ";
			ss << "($1,$2,$3,$4,$5,$6,$7,$8,$9,$10);";

			try
			{
				c.prepare(tablePrefix+"copyoldnode", ss.str());
				work->prepared(tablePrefix+"copyoldnode")(row["id"].as<int64_t>())(row[1].as<int64_t>())(row[2].as<string>())\
					(row[3].as<int64_t>())(row[4].as<int64_t>())(row[5].as<int64_t>())(row[6].as<string>())(true)(false)(row[7].as<string>()).exec();
			}
			catch (const pqxx::sql_error &e)
			{
				errStr = e.what();
				return false;
			}
			catch (const std::exception &e)
			{
				errStr = e.what();
				return false;
			}
		}

		if(node.metaData.visible && (!foundExisting || version >= currentVersion))
		{
			if(!foundExisting)
			{
				//Insert into live table
				stringstream ss;
				ss << "INSERT INTO "<< tablePrefix <<"livenodes (id, changeset, username, uid, timestamp, version, tags, geom) VALUES ";
				string tagsJson;
				EncodeTags(node.tags, tagsJson);

				if(objId < 0)
				{
					if(node.metaData.version != 1)
					{
						errStr = "Cannot assign a new node to any version but 1.";
						return false;
					}

					//Assign a new ID
					objId = nextNodeId;
					createdNodeIds[node.objId] = nextNodeId;
					nextNodeId ++;
				}

				ss << "($1,$2,$3,$4,$5,$6,$7,ST_GeometryFromText($8, 4326));";
				cout << ss.str() << "," << objId << "," << node.metaData.version << endl;

				stringstream wkt;
				wkt.precision(9);
				wkt << "POINT(" << node.lon <<" "<< node.lat << ")";

				try
				{
					c.prepare(tablePrefix+"insertnode", ss.str());
					work->prepared(tablePrefix+"insertnode")(objId)(node.metaData.changeset)(node.metaData.username)\
						(node.metaData.uid)(node.metaData.timestamp)(node.metaData.version)(tagsJson)(wkt.str()).exec();
				}
				catch (const pqxx::sql_error &e)
				{
					errStr = e.what();
					return false;
				}
				catch (const std::exception &e)
				{
					errStr = e.what();
					return false;
				}
			}
			else
			{
				//Update row in place
				stringstream ss;
				ss << "UPDATE "<< tablePrefix <<"livenodes SET changeset=$1, username=$2, uid=$3, timestamp=$4, version=$5, tags=$6, geom=ST_GeometryFromText($7, 4326) WHERE id = $8;";
				string tagsJson;
				EncodeTags(node.tags, tagsJson);

				if(objId < 0)
				{
					errStr = "We should never have to assign an ID and SQL update the live table.";
					return false;
				}

				cout << ss.str() << "," << objId << "," << node.metaData.version << endl;

				stringstream wkt;
				wkt.precision(9);
				wkt << "POINT(" << node.lon <<" "<< node.lat << ")";

				try
				{
					c.prepare(tablePrefix+"updatenode", ss.str());
					work->prepared(tablePrefix+"updatenode")(node.metaData.changeset)(node.metaData.username)\
						(node.metaData.uid)(node.metaData.timestamp)(node.metaData.version)(tagsJson)(wkt.str())(objId).exec();
				}
				catch (const pqxx::sql_error &e)
				{
					errStr = e.what();
					return false;
				}
				catch (const std::exception &e)
				{
					errStr = e.what();
					return false;
				}
			}
		}
		else
		{
			//Insert into history table
			stringstream ss;
			ss << "INSERT INTO "<< tablePrefix <<"oldnodes (id, changeset, username, uid, timestamp, version, tags, visible, current, geom) VALUES ";
			string tagsJson;
			EncodeTags(node.tags, tagsJson);

			if(objId < 0)
			{
				if(node.metaData.version != 1)
				{
					errStr = "Cannot assign a new node to any version but 1.";
					return false;
				}

				//Assign a new ID
				objId = nextNodeId;
				createdNodeIds[node.objId] = nextNodeId;
				nextNodeId ++;
			}

			ss << "($1,$2,$3,$4,$5,$6,$7,$8,$9,ST_GeometryFromText($10, 4326));";
			cout << ss.str() << "," << objId << "," << node.metaData.version << endl;

			stringstream wkt;
			wkt.precision(9);
			wkt << "POINT(" << node.lon <<" "<< node.lat << ")";

			try
			{
				c.prepare(tablePrefix+"insertoldnode", ss.str());
				work->prepared(tablePrefix+"insertoldnode")(objId)(node.metaData.changeset)(node.metaData.username)\
					(node.metaData.uid)(node.metaData.timestamp)(node.metaData.version)(tagsJson)(node.metaData.visible)(false)(wkt.str()).exec();
			}
			catch (const pqxx::sql_error &e)
			{
				errStr = e.what();
				return false;
			}
			catch (const std::exception &e)
			{
				errStr = e.what();
				return false;
			}

		}
	}
	return true;
}

void WaysToDatabase(pqxx::connection &c, pqxx::work *work, const string &tablePrefix, const std::vector<class OsmWay> &ways, std::map<int64_t, int64_t> &createdWayIds)
{
	for(size_t i=0; i<ways.size(); i++)
	{
		const class OsmWay &way = ways[i];
	}
}

void RelationsToDatabase(pqxx::connection &c, pqxx::work *work, const string &tablePrefix, const std::vector<class OsmRelation> &relations, std::map<int64_t, int64_t> &createdRelationIds)
{
	for(size_t i=0; i<relations.size(); i++)
	{
		const class OsmRelation &relation = relations[i];
	}
}

// ************* Next ID functions **************

bool GetNextObjectIds(pqxx::work *work, 
	const string &tablePrefix,
	map<string, int64_t> &nextIdMap,
	string &errStr)
{
	nextIdMap.clear();
	stringstream sstr;
	sstr << "SELECT RTRIM(id), maxid FROM "<< tablePrefix <<"nextids;";

	pqxx::result r;
	try{
		r = work->exec(sstr.str());
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = errStr;
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = errStr;
		return false;
	}

	for (unsigned int rownum=0; rownum < r.size(); ++rownum)
	{
		const pqxx::result::tuple row = r[rownum];
		string id;
		int64_t maxid = 0;
		for (unsigned int colnum=0; colnum < row.size(); ++colnum)
		{
			const pqxx::result::field field = row[colnum];
			if(field.num()==0)
			{
				id = pqxx::to_string(field);
			}
			else if(field.num()==1)
				maxid = field.as<int64_t>();
		}
		nextIdMap[id] = maxid;
	}
	return true;
}

bool UpdateNextObjectIds(pqxx::work *work, 
	const string &tablePrefix,
	const map<string, int64_t> &nextIdMap,
	const map<string, int64_t> &nextIdMapOriginal,
	string &errStr)
{
	//Update next IDs
	for(auto it=nextIdMap.begin(); it != nextIdMap.end(); it++)
	{
		auto it2 = nextIdMapOriginal.find(it->first);
		assert(it2 != nextIdMapOriginal.end());
		if(it->second != it2->second)
		{
			stringstream ss;
			ss << "UPDATE "<< tablePrefix <<"nextids SET maxid = "<< it->second <<" WHERE id='node';"; 

			try
			{
				work->exec(ss.str());
			}
			catch (const pqxx::sql_error &e)
			{
				errStr = errStr;
				return false;
			}
			catch (const std::exception &e)
			{
				errStr = errStr;
				return false;
			}
		}
	}
	return true;
}

// ************* Basic API methods ***************

std::shared_ptr<pqxx::icursorstream> LiveNodesInBboxStart(pqxx::work *work, const string &tablePrefix, 
	const std::vector<double> &bbox, 
	unsigned int maxNodes)
{
	if(bbox.size() != 4)
		throw invalid_argument("Bbox has wrong length");

	stringstream sql;
	sql << "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << tablePrefix;
	sql << "livenodes WHERE geom && ST_MakeEnvelope(";
	sql << bbox[0] <<","<< bbox[1] <<","<< bbox[2] <<","<< bbox[3] << ", 4326)";
	if(maxNodes > 0)
		sql << " LIMIT " << maxNodes;
	sql <<";";

	return std::shared_ptr<pqxx::icursorstream>(new pqxx::icursorstream( *work, sql.str(), "nodesinbbox", 1000 ));
}

int LiveNodesInBboxContinue(std::shared_ptr<pqxx::icursorstream> cursor, std::shared_ptr<IDataStreamHandler> enc)
{
	pqxx::icursorstream *c = cursor.get();
	return NodeResultsToEncoder(*c, enc);
}

void GetLiveWaysThatContainNodes(pqxx::work *work, const string &tablePrefix, 
	const std::set<int64_t> &nodeIds, std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = tablePrefix + "liveways";
	string wayMemTable = tablePrefix + "way_mems";
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

		pqxx::icursorstream cursor( *work, sql, "wayscontainingnodes", 1000 );	

		int records = 1;
		while (records>0)
			records = WayResultsToEncoder(cursor, enc);
	}
}

void GetLiveNodesById(pqxx::work *work, const string &tablePrefix, 
	const std::set<int64_t> &nodeIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string nodeTable = tablePrefix + "livenodes";

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

	pqxx::icursorstream cursor( *work, sql, "nodecursor", 1000 );	

	count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, enc);
}

void GetLiveRelationsForObjects(pqxx::work *work, const string &tablePrefix, 
	char qtype, const set<int64_t> &qids, 
	set<int64_t>::const_iterator &it, size_t step,
	const set<int64_t> &skipIds, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	string relTable = tablePrefix + "liverelations";
	string relMemTable = tablePrefix + "relation_mems_" + qtype;

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

	pqxx::icursorstream cursor( *work, sql, "relationscontainingobjects", 1000 );	

	RelationResultsToEncoder(cursor, skipIds, enc);
}

void GetLiveWaysById(pqxx::work *work, const string &tablePrefix, 
	const std::set<int64_t> &wayIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = tablePrefix + "liveways";

	stringstream sqlFrags;
	int count = 0;
	for(; it != wayIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << "id = " << *it;
		count ++;
	}

	string sql = "SELECT * FROM "+ wayTable
		+ " WHERE ("+sqlFrags.str()+");";

	pqxx::icursorstream cursor( *work, sql, "waycursor", 1000 );	

	set<int64_t> empty;
	int records = 1;
	while(records > 0)
		records = WayResultsToEncoder(cursor, enc);
}

void GetLiveRelationsById(pqxx::work *work, const string &tablePrefix, 
	const std::set<int64_t> &relationIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string relationTable = tablePrefix + "liverelations";

	stringstream sqlFrags;
	int count = 0;
	for(; it != relationIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << "id = " << *it;
		count ++;
	}

	string sql = "SELECT * FROM "+ relationTable
		+ " WHERE ("+sqlFrags.str()+");";

	pqxx::icursorstream cursor( *work, sql, "relationcursor", 1000 );	

	set<int64_t> empty;
	RelationResultsToEncoder(cursor, empty, enc);
}

// ************* Dump specific code *************

void DumpNodes(pqxx::work *work, const string &tablePrefix, bool onlyLiveData, std::shared_ptr<IDataStreamHandler> enc)
{
	if(!onlyLiveData)
		throw runtime_error("Not implemented");
	cout << "Dump nodes" << endl;

	stringstream sql;
	sql << "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << tablePrefix;
	sql << "livenodes;";

	pqxx::icursorstream cursor( *work, sql.str(), "nodecursor", 1000 );	

	int count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, enc);
}

void DumpWays(pqxx::work *work, const string &tablePrefix, bool onlyLiveData, std::shared_ptr<IDataStreamHandler> enc)
{
	if(!onlyLiveData)
		throw runtime_error("Not implemented");
	cout << "Dump ways" << endl;
	stringstream sql;
	sql << "SELECT * FROM ";
	sql << tablePrefix;
	sql << "liveways";
	sql << ";";

	pqxx::icursorstream cursor( *work, sql.str(), "waycursor", 1000 );	

	int count = 1;
	while (count > 0)
		count = WayResultsToEncoder(cursor, enc);
}

void DumpRelations(pqxx::work *work, const string &tablePrefix, bool onlyLiveData, std::shared_ptr<IDataStreamHandler> enc)
{
	if(!onlyLiveData)
		throw runtime_error("Not implemented");
	cout << "Dump relations" << endl;
	stringstream sql;
	sql << "SELECT * FROM ";
	sql << tablePrefix;
	sql << "liverelations";
	sql << ";";

	pqxx::icursorstream cursor( *work, sql.str(), "relationcursor", 1000 );	

	set<int64_t> empty;
	RelationResultsToEncoder(cursor, empty, enc);
}

bool StoreObjects(pqxx::connection &c, pqxx::work *work, 
	const string &tablePrefix, 
	const class OsmData &osmData, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	std::string &errStr)
{
	map<string, int64_t> nextIdMapOriginal, nextIdMap;
	bool ok = GetNextObjectIds(work, tablePrefix, nextIdMapOriginal, errStr);
	if(!ok)
		return false;
	nextIdMap = nextIdMapOriginal;

	ok = NodesToDatabase(c, work, tablePrefix, osmData.nodes, createdNodeIds, nextIdMap, errStr);
	if(!ok)
		return false;
	WaysToDatabase(c, work, tablePrefix, osmData.ways, createdWayIds);
	RelationsToDatabase(c, work, tablePrefix, osmData.relations, createdRelationIds);

	ok = UpdateNextObjectIds(work, tablePrefix, nextIdMap, nextIdMapOriginal, errStr);
	if(!ok)
		return false;
	return true;
}

