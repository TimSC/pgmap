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
	if(refTypeStrs.size() != refIds.size() || refTypeStrs.size() != refRoles.size())
		throw std::invalid_argument("Length of ref vectors must be equal");
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
		//if(objId == 314382649)
		//	throw std::runtime_error("Not the dreaded 314382649");
		double lat = atof(c[latCol].c_str());
		double lon = atof(c[lonCol].c_str());

		DecodeMetadata(c, metaDataCols, metaData);
		
		DecodeTags(c, tagsCol, tagHandler);
		count ++;

		double timeNow = (double)clock() / CLOCKS_PER_SEC;
		if (timeNow - lastUpdateTime > 30.0)
		{
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

		double timeNow = (double)clock() / CLOCKS_PER_SEC;
		if (timeNow - lastUpdateTime > 30.0)
		{
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
			if(relMemHandler.refTypeStrs.size() != relMemHandler.refIds.size() ||
				relMemHandler.refTypeStrs.size() != relMemRolesHandler.refRoles.size())
			{
				throw runtime_error("Decoded relation has inconsistent member data");
			}

			count ++;

			double timeNow = (double)clock() / CLOCKS_PER_SEC;
			if (timeNow - lastUpdateTime > 30.0)
			{
				lastUpdateCount = count;
				lastUpdateTime = timeNow;
			}

			enc->StoreRelation(objId, metaData, tagHandler.tagMap, 
				relMemHandler.refTypeStrs, relMemHandler.refIds, relMemRolesHandler.refRoles);
		}
	}
}

// *********** Convert to database SQL *************

bool ObjectsToDatabase(pqxx::connection &c, pqxx::work *work, const string &tablePrefix, 
	const std::string &typeStr,
	const std::vector<const class OsmObject *> &objPtrs, 
	std::map<int64_t, int64_t> &createdNodeIds,
	map<string, int64_t> &nextIdMap,
	std::string &errStr,
	int verbose)
{
	char trueStr[] = "true";
	char falseStr[] = "true";
	auto it = nextIdMap.find(typeStr);
	int64_t &nextObjId = it->second;

	for(size_t i=0; i<objPtrs.size(); i++)
	{
		const class OsmObject *osmObject = objPtrs[i];
		const class OsmNode *nodeObject = dynamic_cast<const class OsmNode *>(osmObject);
		const class OsmWay *wayObject = dynamic_cast<const class OsmWay *>(osmObject);
		const class OsmRelation *relationObject = dynamic_cast<const class OsmRelation *>(osmObject);
		if(typeStr == "node" && nodeObject == nullptr)
			throw invalid_argument("Object type not node as expected");
		else if(typeStr == "way" && wayObject == nullptr)
			throw invalid_argument("Object type not way as expected");
		else if(typeStr == "relation" && relationObject == nullptr)
			throw invalid_argument("Object type not relation as expected");
		int64_t objId = osmObject->objId;
		int64_t version = osmObject->metaData.version;

		//Convert spatial and member data to appropriate formats
		stringstream wkt;
		stringstream refsJson;
		stringstream rolesJson;
		if(nodeObject != nullptr)
		{
			wkt.precision(9);
			wkt << "POINT(" << nodeObject->lon <<" "<< nodeObject->lat << ")";
		}
		else if(wayObject != nullptr)
		{
			refsJson << "[";
			for(size_t j=0;j < wayObject->refs.size(); j++)
			{
				if(j != 0)
					refsJson << ", ";
				refsJson << wayObject->refs[j];
			}
			refsJson << "]";
		}
		else if(relationObject != nullptr)
		{
			if(relationObject->refTypeStrs.size() != relationObject->refIds.size() || relationObject->refTypeStrs.size() != relationObject->refRoles.size())
				throw std::invalid_argument("Length of ref vectors must be equal");

			refsJson << "[";
			for(size_t j=0;j < relationObject->refIds.size(); j++)
			{
				if(j != 0)
					refsJson << ", ";
				refsJson << "[\"" << relationObject->refTypeStrs[j] << "\"," << relationObject->refIds[j] << "]";
			}
			refsJson << "]";

			rolesJson << "[";
			for(size_t j=0;j < relationObject->refRoles.size(); j++)
			{
				if(j != 0)
					rolesJson << ", ";
				rolesJson << "\"" << relationObject->refRoles[j] << "\"";
			}
			rolesJson << "]";
		}

		//Get existing object object in live table (if any)
		string checkExistingLiveSql = "SELECT * FROM "+ tablePrefix + "live"+typeStr+"s WHERE (id=$1);";
		pqxx::result r;
		try
		{
			if(verbose >= 1)
				cout << checkExistingLiveSql << " " << objId << endl;
			c.prepare(tablePrefix+"checkobjexists"+typeStr, checkExistingLiveSql);
			r = work->prepared(tablePrefix+"checkobjexists"+typeStr)(objId).exec();
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
			currentVersion = row["version"].as<int64_t>();
		}

		//Get existing object version in old table (if any)
		string checkExistingOldSql = "SELECT MAX(version) FROM "+ tablePrefix + "old"+typeStr+"s WHERE (id=$1);";
		pqxx::result r2;
		try
		{
			if(verbose >= 1)
				cout << checkExistingOldSql << " " << objId << endl;
			c.prepare(tablePrefix+"checkoldobjexists"+typeStr, checkExistingOldSql);
			r2 = work->prepared(tablePrefix+"checkoldobjexists"+typeStr)(objId).exec();
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

		bool foundOld = false;
		int64_t oldVersion = -1;
		const pqxx::result::tuple row = r2[0];
		const pqxx::result::field field = row[0];
		if(!field.is_null())
		{
			oldVersion = field.as<int64_t>();
			foundOld = true;
		}

		//Check if we need to delete object from live table
		if(foundExisting && version >= currentVersion && !osmObject->metaData.visible)
		{
			string deletedLiveSql = "DELETE FROM "+ tablePrefix
				+ "live"+ typeStr +"s WHERE (id=$1);";
			try
			{
				if(verbose >= 1)
					cout << deletedLiveSql << " " << objId << endl;
				c.prepare(tablePrefix+"deletelive"+typeStr, deletedLiveSql);
				work->prepared(tablePrefix+"deletelive"+typeStr)(objId).exec();
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

		//Check if we need to copy row from live to history table
		if(foundExisting && version > currentVersion)
		{
			const pqxx::result::tuple row = r[0];

			//Insert into live table
			stringstream ss;
			ss << "INSERT INTO "<< tablePrefix <<"old"<<typeStr<<"s (id, changeset, username, uid, timestamp, version, tags, visible, current";
			if(nodeObject != nullptr)
				ss << ", geom";
			else if(wayObject != nullptr)
				ss << ", members";
			else if(relationObject != nullptr)
				ss << ", members, memberroles";
			ss << ") VALUES ";
			ss << "($1,$2,$3,$4,$5,$6,$7,$8,$9";
			if(nodeObject != nullptr || wayObject != nullptr)
				ss << ",$10";
			else if (relationObject != nullptr)
				ss << ",$10,$11";
			ss << ") ON CONFLICT DO NOTHING;";

			try
			{
				if(nodeObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"copyoldnode", ss.str());
					work->prepared(tablePrefix+"copyoldnode")(row["id"].as<int64_t>())(row["changeset"].as<int64_t>())(row["username"].as<string>())\
						(row["uid"].as<int64_t>())(row["timestamp"].as<int64_t>())(row["version"].as<int64_t>())\
						(row["tags"].as<string>())(true)(false)(row["geom"].as<string>()).exec();
				}
				else if(wayObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"copyoldway", ss.str());
					work->prepared(tablePrefix+"copyoldway")(row["id"].as<int64_t>())(row["changeset"].as<int64_t>())(row["username"].as<string>())\
						(row["uid"].as<int64_t>())(row["timestamp"].as<int64_t>())(row["version"].as<int64_t>())\
						(row["tags"].as<string>())(true)(false)(row["members"].as<string>()).exec();
				}
				else if(relationObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"copyoldrelation", ss.str());
					work->prepared(tablePrefix+"copyoldrelation")(row["id"].as<int64_t>())(row["changeset"].as<int64_t>())(row["username"].as<string>())\
						(row["uid"].as<int64_t>())(row["timestamp"].as<int64_t>())(row["version"].as<int64_t>())\
						(row["tags"].as<string>())(true)(false)(row["members"].as<string>())(row["memberroles"].as<string>()).exec();
				}
			}
			catch (const pqxx::sql_error &e)
			{
				stringstream ss2;
				ss2 << e.what() << ":" << e.query() << ":" << ss.str();
				errStr = ss2.str();
				return false;
			}
			catch (const std::exception &e)
			{
				stringstream ss2;
				ss2 << e.what() << ";" << ss.str() << endl;
				errStr = ss2.str();
				return false;
			}
		}

		//Check if this is the latest version and visible
		if(osmObject->metaData.visible && (!foundExisting || version >= currentVersion) && (!foundOld || version >= oldVersion))
		{
			if(!foundExisting)
			{
				//Insert into live table
				stringstream ss;
				ss << "INSERT INTO "<< tablePrefix <<"live"<<typeStr<<"s (id, changeset, username, uid, timestamp, version, tags";
				if(nodeObject != nullptr)
					ss << ", geom";
				else if(wayObject != nullptr)
					ss << ", members";
				else if(relationObject != nullptr)
					ss << ", members, memberroles";

				ss << ") VALUES ";

				string tagsJson;
				EncodeTags(osmObject->tags, tagsJson);

				if(objId <= 0)
				{
					if(osmObject->metaData.version != 1)
					{
						errStr = "Cannot assign a new node to any version but one.";
						return false;
					}

					//Assign a new ID
					objId = nextObjId;
					createdNodeIds[osmObject->objId] = nextObjId;
					nextObjId ++;
				}

				ss << "($1,$2,$3,$4,$5,$6,$7";
				if(nodeObject != nullptr)
					ss << ",ST_GeometryFromText($8, 4326)";
				else if(wayObject != nullptr)
					ss << ",$8";
				else if (relationObject != nullptr)
					ss << ",$8,$9";
				ss << ");";

				try
				{
					if(nodeObject != nullptr)
					{
						if(verbose >= 1)
							cout << ss.str() << endl;
						c.prepare(tablePrefix+"insertnode", ss.str());
						work->prepared(tablePrefix+"insertnode")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)(tagsJson)(wkt.str()).exec();
					}
					else if(wayObject != nullptr)
					{
						if(verbose >= 1)
							cout << ss.str() << endl;
						c.prepare(tablePrefix+"insertway", ss.str());
						work->prepared(tablePrefix+"insertway")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)(tagsJson)(refsJson.str()).exec();
					}
					else if(relationObject != nullptr)
					{
						if(verbose >= 1)
							cout << ss.str() << endl;
						c.prepare(tablePrefix+"insertrelation", ss.str());
						work->prepared(tablePrefix+"insertrelation")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
							(tagsJson)(refsJson.str())(rolesJson.str()).exec();
					}
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

				stringstream ssi;
				ssi << "INSERT INTO "<< tablePrefix << typeStr << "ids (id) VALUES ($1) ON CONFLICT DO NOTHING;";

				if(verbose >= 1)
					cout << ssi.str() << endl;
				c.prepare(tablePrefix+"insert"+typeStr+"ids", ssi.str());
				work->prepared(tablePrefix+"insert"+typeStr+"ids")(objId).exec();

			}
			else
			{
				//Update row in place
				stringstream ss;
				ss << "UPDATE "<< tablePrefix <<"live"<<typeStr<<"s SET changeset=$1, username=$2, uid=$3, timestamp=$4, version=$5, tags=$6";
				if(nodeObject != nullptr)
					ss << ", geom=ST_GeometryFromText($8, 4326)";
				else if(wayObject != nullptr)
					ss << ", members=$8";
				else if(relationObject != nullptr)
					ss << ", members=$8, memberroles=$9";
				ss << " WHERE id = $7;";
				string tagsJson;
				EncodeTags(osmObject->tags, tagsJson);

				if(objId < 0)
				{
					errStr = "We should never have to assign an ID and SQL update the live table.";
					return false;
				}

				try
				{
					if(nodeObject != nullptr)
					{
						if(verbose >= 1)
							cout << ss.str() << endl;
						c.prepare(tablePrefix+"updatenode", ss.str());
						work->prepared(tablePrefix+"updatenode")(osmObject->metaData.changeset)(osmObject->metaData.username)\
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)(tagsJson)(objId)(wkt.str()).exec();
					}
					else if(wayObject != nullptr)
					{
						if(verbose >= 1)
							cout << ss.str() << endl;
						c.prepare(tablePrefix+"updateway", ss.str());
						work->prepared(tablePrefix+"updateway")(osmObject->metaData.changeset)(osmObject->metaData.username)\
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)(tagsJson)(objId)(refsJson.str()).exec();
					}
					else if(relationObject != nullptr)
					{
						if(verbose >= 1)
							cout << ss.str() << endl;
						c.prepare(tablePrefix+"updaterelation", ss.str());
						work->prepared(tablePrefix+"updaterelation")(osmObject->metaData.changeset)(osmObject->metaData.username)\
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
							(tagsJson)(objId)(refsJson.str())(rolesJson.str()).exec();
					}

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
			ss << "INSERT INTO "<< tablePrefix <<"old"<<typeStr<<"s (id, changeset, username, uid, timestamp, version, tags, visible, current";
			if(nodeObject != nullptr)
				ss << ", geom";
			else if(wayObject != nullptr)
				ss << ", members";
			else if(relationObject != nullptr)
				ss << ", members, memberroles";
			ss << ") VALUES ";
			string tagsJson;
			EncodeTags(osmObject->tags, tagsJson);

			if(objId <= 0)
			{
				if(osmObject->metaData.version != 1)
				{
					errStr = "Cannot assign a new node to any version but 1.";
					return false;
				}

				//Assign a new ID
				objId = nextObjId;
				createdNodeIds[osmObject->objId] = nextObjId;
				nextObjId ++;
			}

			ss << "($1,$2,$3,$4,$5,$6,$7,$8,$9";
			if(nodeObject != nullptr)
				ss << ",ST_GeometryFromText($10, 4326)";
			else if(wayObject != nullptr)
				ss << ",$10";
			else if(relationObject != nullptr)
				ss << ",$10,$11";
			ss << ") ON CONFLICT DO NOTHING;";

			try
			{	
				if(nodeObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"insertoldnode", ss.str());
					work->prepared(tablePrefix+"insertoldnode")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
						(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
						(tagsJson)(osmObject->metaData.visible)(false)(wkt.str()).exec();
				}
				else if(wayObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"insertoldway", ss.str());
					work->prepared(tablePrefix+"insertoldway")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
						(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
						(tagsJson)(osmObject->metaData.visible)(false)(refsJson.str()).exec();
				}
				else if(relationObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"insertoldrelation", ss.str());
					work->prepared(tablePrefix+"insertoldrelation")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
						(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
						(tagsJson)(osmObject->metaData.visible)(false)(refsJson.str())(rolesJson.str()).exec();
				}

			}
			catch (const pqxx::sql_error &e)
			{
				stringstream ss2;
				ss2 << e.what() << ":" << e.query() << ":" << ss.str();
				errStr = ss2.str();
				return false;
			}
			catch (const std::exception &e)
			{
				stringstream ss2;
				ss2 << e.what() << ";" << ss.str() << endl;
				errStr = ss2.str();
				return false;
			}

			stringstream ssi;
			ssi << "INSERT INTO "<< tablePrefix << typeStr << "ids (id) VALUES ($1) ON CONFLICT DO NOTHING;";

			if(verbose >= 1)
				cout << ssi.str() << endl;
			c.prepare(tablePrefix+"insert"+typeStr+"ids", ssi.str());
			work->prepared(tablePrefix+"insert"+typeStr+"ids")(objId).exec();
		}

		if(wayObject != nullptr)
		{
			//Update way member table
			size_t j=0;
			while(j < wayObject->refs.size())
			{
				stringstream sswm;
				size_t insertSize = 0;
				sswm << "INSERT INTO "<< tablePrefix << "way_mems (id, version, index, member) VALUES ";

				size_t initialj = j;
				for(; j<wayObject->refs.size() && insertSize < 1000; j++)
				{
					if(j!=initialj)
						sswm << ",";
					sswm << "("<<objId<<","<<wayObject->metaData.version<<","<<j<<","<<wayObject->refs[j]<<")";
					insertSize ++;
				}
				sswm << ";";

				try
				{
					if(verbose >= 1)
						cout << sswm.str() << endl;
					work->exec(sswm.str());
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
		else if(relationObject != nullptr)
		{
			//Update relation member tables
			for(size_t j=0;j < relationObject->refIds.size(); j++)
			{
				stringstream ssrm;
				ssrm << "INSERT INTO "<< tablePrefix << "relation_mems_"<< relationObject->refTypeStrs[j][0] << " (id, version, index, member) VALUES ";
				ssrm << "("<<objId<<","<<relationObject->metaData.version<<","<<j<<","<<relationObject->refIds[j]<<");";

				try
				{
					if(verbose >= 1)
						cout << ssrm.str() << endl;
					work->exec(ssrm.str());
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
	}

	return true;
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
		errStr = e.what();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
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
		if(it2 != nextIdMapOriginal.end())
		{
			//Only bother updating if value has changed
			if(it->second != it2->second)
			{
				stringstream ss;
				ss << "UPDATE "<< tablePrefix <<"nextids SET maxid = "<< it->second <<" WHERE id='"<<it->first<<"';"; 

				try
				{
					work->exec(ss.str());
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
			//Insert into table
			stringstream ss;
			ss << "INSERT INTO "<< tablePrefix <<"nextids (id, maxid) VALUES ('"<<it->first<<"',"<<it->second<<");";

			try
			{
				work->exec(ss.str());
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

// ************* Basic API methods ***************

std::shared_ptr<pqxx::icursorstream> LiveNodesInBboxStart(pqxx::work *work, const string &tablePrefix, 
	const std::vector<double> &bbox, 
	const string &excludeTablePrefix)
{
	if(bbox.size() != 4)
		throw invalid_argument("Bbox has wrong length");
	string liveNodeTable = tablePrefix + "livenodes";
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "nodeids";

	stringstream sql;
	sql.precision(9);
	sql << "SELECT "<<liveNodeTable<<".*, ST_X("<<liveNodeTable<<".geom) as lon, ST_Y("<<liveNodeTable<<".geom) AS lat";
	sql << " FROM ";
	sql << liveNodeTable;
	if(excludeTable.size() > 0)
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<liveNodeTable<<".id = "<<excludeTable<<".id";
	sql << " WHERE "<<liveNodeTable<<".geom && ST_MakeEnvelope(";
	sql << bbox[0] <<","<< bbox[1] <<","<< bbox[2] <<","<< bbox[3] << ", 4326)";
	if(excludeTable.size() > 0)
		sql << " AND "<<excludeTable<<".id IS NULL";
	sql <<";";

	return std::shared_ptr<pqxx::icursorstream>(new pqxx::icursorstream( *work, sql.str(), "nodesinbbox", 1000 ));
}

int LiveNodesInBboxContinue(std::shared_ptr<pqxx::icursorstream> cursor, std::shared_ptr<IDataStreamHandler> enc)
{
	pqxx::icursorstream *c = cursor.get();
	return NodeResultsToEncoder(*c, enc);
}

void GetLiveWaysThatContainNodes(pqxx::work *work, const string &tablePrefix, 
	const string &excludeTablePrefix,
	const std::set<int64_t> &nodeIds, std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = tablePrefix + "liveways";
	string wayMemTable = tablePrefix + "way_mems";
	int step = 1000;
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "wayids";

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

		string sql = "SELECT "+wayTable+".*";
		if(excludeTable.size() > 0)
			sql += ", "+excludeTable+".id";

		sql += " FROM "+wayMemTable+" INNER JOIN "+wayTable+" ON "\
			+wayMemTable+".id = "+wayTable+".id AND "+wayMemTable+".version = "+wayTable+".version";
		if(excludeTable.size() > 0)
			sql += " LEFT JOIN "+excludeTable+" ON "+wayTable+".id = "+excludeTable+".id";

		sql += " WHERE ("+sqlFrags.str()+")";
		if(excludeTable.size() > 0)
			sql += " AND "+excludeTable+".id IS NULL";
		sql += ";";

		pqxx::icursorstream cursor( *work, sql, "wayscontainingnodes", 1000 );	

		int records = 1;
		while (records>0)
			records = WayResultsToEncoder(cursor, enc);
	}
}

void GetLiveNodesById(pqxx::work *work, const string &tablePrefix, 
	const string &excludeTablePrefix, 
	const std::set<int64_t> &nodeIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string nodeTable = tablePrefix + "livenodes";
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "nodeids";

	stringstream sqlFrags;
	int count = 0;
	for(; it != nodeIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << nodeTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+ nodeTable;
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+nodeTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "nodecursor", 1000 );	

	count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, enc);
}

void GetLiveRelationsForObjects(pqxx::work *work, const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	char qtype, const set<int64_t> &qids, 
	set<int64_t>::const_iterator &it, size_t step,
	const set<int64_t> &skipIds, 
	std::shared_ptr<IDataStreamHandler> enc)
{
	string relTable = tablePrefix + "liverelations";
	string relMemTable = tablePrefix + "relation_mems_" + qtype;
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "relationids";

	stringstream sqlFrags;
	int count = 0;
	for(; it != qids.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << relMemTable << ".member = " << *it;
		count ++;
	}

	string sql = "SELECT "+relTable+".*";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+relMemTable+" INNER JOIN "+relTable+" ON "+relMemTable+".id = "+relTable+".id AND "+relMemTable+".version = "+relTable+".version";
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+relTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "relationscontainingobjects", 1000 );	

	RelationResultsToEncoder(cursor, skipIds, enc);
}

void GetLiveWaysById(pqxx::work *work, const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &wayIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = tablePrefix + "liveways";
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "wayids";

	stringstream sqlFrags;
	int count = 0;
	for(; it != wayIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << wayTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+ wayTable;
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+wayTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "waycursor", 1000 );	

	set<int64_t> empty;
	int records = 1;
	while(records > 0)
		records = WayResultsToEncoder(cursor, enc);
}

void GetLiveRelationsById(pqxx::work *work, const string &tablePrefix, 
	const std::string &excludeTablePrefix, 
	const std::set<int64_t> &relationIds, std::set<int64_t>::const_iterator &it, 
	size_t step, std::shared_ptr<IDataStreamHandler> enc)
{
	string relationTable = tablePrefix + "liverelations";
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "relationids";

	stringstream sqlFrags;
	int count = 0;
	for(; it != relationIds.end() && count < step; it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << relationTable << ".id = " << *it;
		count ++;
	}

	string sql = "SELECT *";
	if(excludeTable.size() > 0)
		sql += ", "+excludeTable+".id";

	sql += " FROM "+ relationTable;
	if(excludeTable.size() > 0)
		sql += " LEFT JOIN "+excludeTable+" ON "+relationTable+".id = "+excludeTable+".id";

	sql += " WHERE ("+sqlFrags.str()+")";
	if(excludeTable.size() > 0)
		sql += " AND "+excludeTable+".id IS NULL";
	sql += ";";

	pqxx::icursorstream cursor( *work, sql, "relationcursor", 1000 );	

	set<int64_t> empty;
	RelationResultsToEncoder(cursor, empty, enc);
}

// ************* Dump specific code *************

void DumpNodes(pqxx::work *work, const string &tablePrefix, 
	const string &excludeTablePrefix,
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string liveNodeTable = tablePrefix + "livenodes";
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "nodeids";

	//Discourage sequential scans of tables, since they is not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	/*
		                                                    QUERY PLAN                                                        
	--------------------------------------------------------------------------------------------------------------------------
	 Merge Anti Join  (cost=1.01..50174008.28 rows=1365191502 width=8)
	   Merge Cond: (planet_livenodes.id = planet_mod_nodeids.id)
	   ->  Index Only Scan using planet_livenodes_pkey on planet_livenodes  (cost=0.58..35528317.86 rows=1368180352 width=8)
	   ->  Index Only Scan using planet_mod_nodeids_pkey on planet_mod_nodeids  (cost=0.43..11707757.01 rows=2988850 width=8)
	(4 rows)
	*/

	stringstream sql;
	sql << "SELECT "<< liveNodeTable << ".*, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << liveNodeTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<liveNodeTable<<".id = "<<excludeTable<<".id";
		sql << " WHERE "<<excludeTable<<".id IS NULL";
	}
	if(order)
		sql << " ORDER BY " << liveNodeTable << ".id";
	sql << ";";

	pqxx::icursorstream cursor( *work, sql.str(), "nodecursor", 1000 );	

	int count = 1;
	while(count > 0)
		count = NodeResultsToEncoder(cursor, enc);
}

void DumpWays(pqxx::work *work, const string &tablePrefix, 
	const string &excludeTablePrefix,
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string wayTable = tablePrefix + "liveways";
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "wayids";

	//Discourage sequential scans of tables, since they is not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << wayTable << ".* FROM ";
	sql << wayTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<wayTable<<".id = "<<excludeTable<<".id";
		sql << " WHERE "<<excludeTable<<".id IS NULL";
	}
	if(order)
		sql << " ORDER BY " << wayTable << ".id";
	sql << ";";

	pqxx::icursorstream cursor( *work, sql.str(), "waycursor", 1000 );	

	int count = 1;
	while (count > 0)
		count = WayResultsToEncoder(cursor, enc);
}

void DumpRelations(pqxx::work *work, const string &tablePrefix, 
	const string &excludeTablePrefix, 
	bool order,
	std::shared_ptr<IDataStreamHandler> enc)
{
	string relationTable = tablePrefix + "liverelations";
	string excludeTable;
	if(excludeTablePrefix.size() > 0)
		excludeTable = excludeTablePrefix + "relationids";

	//Discourage sequential scans of tables, since they is not necessary and we want to avoid doing a sort
	work->exec("set enable_seqscan to off;");

	stringstream sql;
	sql << "SELECT " << relationTable << ".* FROM ";
	sql << relationTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN "<<excludeTable<<" ON "<<relationTable<<".id = "<<excludeTable<<".id";
		sql << " WHERE "<<excludeTable<<".id IS NULL";
	}
	if(order)
		sql << " ORDER BY " << relationTable << ".id";
	sql << ";";

	pqxx::icursorstream cursor( *work, sql.str(), "relationcursor", 1000 );	

	set<int64_t> empty;
	RelationResultsToEncoder(cursor, empty, enc);
}

bool StoreObjects(pqxx::connection &c, pqxx::work *work, 
	const string &tablePrefix, 
	class OsmData osmData, 
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

	//Store nodes
	std::vector<const class OsmObject *> objPtrs;
	for(size_t i=0; i<osmData.nodes.size(); i++)
		objPtrs.push_back(&osmData.nodes[i]);
	ok = ObjectsToDatabase(c, work, tablePrefix, "node", objPtrs, createdNodeIds, nextIdMap, errStr, 0);
	if(!ok)
		return false;

	//Update numbering for created nodes used in ways and relations
	for(size_t i=0; i<osmData.ways.size(); i++)
	{
		class OsmWay &way = osmData.ways[i];
		for(size_t j=0; j<way.refs.size(); j++)
		{
			if(way.refs[j] > 0) continue;
			std::map<int64_t, int64_t>::iterator it = createdNodeIds.find(way.refs[j]);
			if(it == createdNodeIds.end())
			{
				stringstream ss;
				ss << "Way "<< way.objId << " depends on undefined node " << way.refs[j];
				errStr = ss.str();
				return false;
			}
			way.refs[j] = it->second;
		}
	}

	for(size_t i=0; i<osmData.relations.size(); i++)
	{
		class OsmRelation &rel = osmData.relations[i];
		for(size_t j=0; j<rel.refIds.size(); j++)
		{
			if(rel.refTypeStrs[j] != "node" or rel.refIds[j] > 0) continue;
			std::map<int64_t, int64_t>::iterator it = createdNodeIds.find(rel.refIds[j]);
			if(it == createdNodeIds.end())
			{
				stringstream ss;
				ss << "Relation "<< rel.objId << " depends on undefined node " << rel.refIds[j];
				errStr = ss.str();
				return false;
			}
			rel.refIds[j] = it->second;
		}
	}

	//Store ways
	objPtrs.clear();
	for(size_t i=0; i<osmData.ways.size(); i++)
		objPtrs.push_back(&osmData.ways[i]);
	ok = ObjectsToDatabase(c, work, tablePrefix, "way", objPtrs, createdWayIds, nextIdMap, errStr, 0);
	if(!ok)
		return false;

	//Update numbering for created ways used in relations
	for(size_t i=0; i<osmData.relations.size(); i++)
	{
		class OsmRelation &rel = osmData.relations[i];
		for(size_t j=0; j<rel.refIds.size(); j++)
		{
			if(rel.refTypeStrs[j] != "way" or rel.refIds[j] > 0) continue;
			std::map<int64_t, int64_t>::iterator it = createdWayIds.find(rel.refIds[j]);
			if(it == createdNodeIds.end())
			{
				stringstream ss;
				ss << "Relation "<< rel.objId << " depends on undefined way " << rel.refIds[j];
				errStr = ss.str();
				return false;
			}
			rel.refIds[j] = it->second;
		}
	}

	//Store relations one by one (since one relation can depend on another)		
	for(size_t i=0; i<osmData.relations.size(); i++)
	{
		//Check refs for the relation we are about to add
		class OsmRelation &rel = osmData.relations[i];
		for(size_t j=0; j<rel.refIds.size(); j++)
		{
			if(rel.refTypeStrs[j] != "relation" or rel.refIds[j] > 0) continue;
			std::map<int64_t, int64_t>::iterator it = createdRelationIds.find(rel.refIds[j]);
			if(it == createdNodeIds.end())
			{
				stringstream ss;
				ss << "Relation "<< rel.objId << " depends on undefined way " << rel.refIds[j];
				errStr = ss.str();
				return false;
			}
			rel.refIds[j] = it->second;
		}

		//Add to database
		objPtrs.clear();
		objPtrs.push_back(&osmData.relations[i]);

		ok = ObjectsToDatabase(c, work, tablePrefix, "relation", objPtrs, createdRelationIds, nextIdMap, errStr, 0);
		if(!ok)
			return false;
	}

	ok = UpdateNextObjectIds(work, tablePrefix, nextIdMap, nextIdMapOriginal, errStr);
	if(!ok)
		return false;
	return true;
}

bool ClearTable(pqxx::work *work, const string &tableName, std::string &errStr)
{
	try
	{
		string deleteSql = "DELETE FROM "+ tableName + ";";
		work->exec(deleteSql);
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
	return true;
}

bool ResetActiveTables(pqxx::connection &c, pqxx::work *work, 
	const string &tableActivePrefix, 
	const string &tableStaticPrefix,
	std::string &errStr)
{
	bool ok = ClearTable(work, tableActivePrefix + "livenodes", errStr);      if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "liveways", errStr);            if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "liverelations", errStr);       if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "oldnodes", errStr);            if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "oldways", errStr);             if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "oldrelations", errStr);        if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "nodeids", errStr);             if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "wayids", errStr);              if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "relationids", errStr);         if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "way_mems", errStr);            if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "relation_mems_n", errStr);     if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "relation_mems_w", errStr);     if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "relation_mems_r", errStr);     if(!ok) return false;
	ok = ClearTable(work, tableActivePrefix + "nextids", errStr);             if(!ok) return false;

	map<string, int64_t> nextIdMap;
	ok = GetNextObjectIds(work, 
		tableStaticPrefix,
		nextIdMap,
		errStr);
	if(!ok) return false;

	map<string, int64_t> emptyIdMap;
	ok = UpdateNextObjectIds(work, tableActivePrefix, nextIdMap, emptyIdMap, errStr);

	return ok;	
}

