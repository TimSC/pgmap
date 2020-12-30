#include "dbstore.h"
#include "dbids.h"
#include "dbcommon.h"
#include "dbjson.h"
#include "dbusername.h"
#include "dbquery.h"
#include "util.h"
using namespace std;

#if PQXX_VERSION_MAJOR >= 6
#define pqxxfield pqxx::field
#define pqxxrow pqxx::row
#else
#define pqxxfield pqxx::result::field
#define pqxxrow pqxx::result::tuple
#endif

bool ObjectsToDatabase(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
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

	int majorVer=0, minorVer=0;
	DbGetVersion(c, work, majorVer, minorVer);
	bool ocdnSupported = true;
	string ocdn = " ON CONFLICT DO NOTHING";
	if(majorVer < 9 || (majorVer == 9 && minorVer <= 3))
	{
		ocdn = "";
		ocdnSupported = false;
	}

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
		string refsJson;
		string rolesJson;
		if(nodeObject != nullptr)
		{
			wkt.precision(9);
			wkt << "POINT(" << nodeObject->lon <<" "<< nodeObject->lat << ")";
		}
		else if(wayObject != nullptr)
		{
			EncodeInt64Vec(wayObject->refs, refsJson);
		}
		else if(relationObject != nullptr)
		{
			if(relationObject->refTypeStrs.size() != relationObject->refIds.size() || relationObject->refTypeStrs.size() != relationObject->refRoles.size())
				throw std::invalid_argument("Length of ref vectors must be equal");

			EncodeRelationMems(relationObject->refTypeStrs, relationObject->refIds, refsJson);
			EncodeStringVec(relationObject->refRoles, rolesJson);
		}

		//Get existing object object in live table (if any)
		string checkExistingLiveSql = "SELECT * FROM "+ c.quote_name(tablePrefix + "live"+typeStr+"s")+" WHERE (id=$1);";
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
			const pqxxrow row = r[0];
			currentVersion = row["version"].as<int64_t>();
		}

		//Get existing object version in old table (if any)
		string checkExistingOldSql = "SELECT MAX(version) FROM "+ c.quote_name(tablePrefix + "old"+typeStr+"s") + " WHERE (id=$1);";
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
		const pqxxrow row = r2[0];
		const pqxxfield field = row[0];
		if(!field.is_null())
		{
			oldVersion = field.as<int64_t>();
			foundOld = true;
		}

		//Check if we need to delete object from live table
		if(foundExisting && version >= currentVersion && !osmObject->metaData.visible)
		{
			string deletedLiveSql = "DELETE FROM "+ c.quote_name(tablePrefix
				+ "live"+ typeStr +"s") + " WHERE (id=$1);";
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
			const pqxxrow row = r[0];

			//Insert into live table
			stringstream ss;
			ss << "INSERT INTO "<< c.quote_name(tablePrefix+"old"+typeStr+"s") + " (id, changeset, changeset_index, username, uid, timestamp, version, tags, visible";
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
			ss << ") " << ocdn << ";";

			try
			{
				if(nodeObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"copyoldnode", ss.str());

					pqxx::prepare::invocation invoc = work->prepared(tablePrefix+"copyoldnode");
					BindVal<int64_t>(invoc, row["id"]);
					BindVal<int64_t>(invoc, row["changeset"]);
					BindVal<int64_t>(invoc, row["changeset_index"]);
					BindVal<string>(invoc, row["username"]);
					BindVal<int64_t>(invoc, row["uid"]);
					BindVal<int64_t>(invoc, row["timestamp"]);
					BindVal<int64_t>(invoc, row["version"]);
					BindVal<string>(invoc, row["tags"]);
					invoc(true);
					BindVal<string>(invoc, row["geom"]);

					invoc.exec();
				}
				else if(wayObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"copyoldway", ss.str());

					pqxx::prepare::invocation invoc = work->prepared(tablePrefix+"copyoldway");
					BindVal<int64_t>(invoc, row["id"]);
					BindVal<int64_t>(invoc, row["changeset"]);
					BindVal<int64_t>(invoc, row["changeset_index"]);
					BindVal<string>(invoc, row["username"]);
					BindVal<int64_t>(invoc, row["uid"]);
					BindVal<int64_t>(invoc, row["timestamp"]);
					BindVal<int64_t>(invoc, row["version"]);
					BindVal<string>(invoc, row["tags"]);
					invoc(true);
					BindVal<string>(invoc, row["members"]);

					invoc.exec();
				}
				else if(relationObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"copyoldrelation", ss.str());
	
					pqxx::prepare::invocation invoc = work->prepared(tablePrefix+"copyoldrelation");
					BindVal<int64_t>(invoc, row["id"]);
					BindVal<int64_t>(invoc, row["changeset"]);
					BindVal<int64_t>(invoc, row["changeset_index"]);
					BindVal<string>(invoc, row["username"]);
					BindVal<int64_t>(invoc, row["uid"]);
					BindVal<int64_t>(invoc, row["timestamp"]);
					BindVal<int64_t>(invoc, row["version"]);
					BindVal<string>(invoc, row["tags"]);
					invoc(true);
					BindVal<string>(invoc, row["members"]);
					BindVal<string>(invoc, row["memberroles"]);

					invoc.exec();
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
				ss << "INSERT INTO "<< c.quote_name(tablePrefix+"live"+typeStr+"s") << " (id, changeset, username, uid, timestamp, version, tags";
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
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)(tagsJson)(refsJson).exec();
					}
					else if(relationObject != nullptr)
					{
						if(verbose >= 1)
							cout << ss.str() << endl;
						c.prepare(tablePrefix+"insertrelation", ss.str());
						work->prepared(tablePrefix+"insertrelation")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
							(tagsJson)(refsJson)(rolesJson).exec();
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

				//Update existing id lists (nodeids, wayids, relationids)
				stringstream ssi;
				ssi << "INSERT INTO "<< c.quote_name(tablePrefix+typeStr+"ids") << " (id) VALUES ($1) "<<ocdn<<";";
				if(verbose >= 1)
					cout << ssi.str() << endl;
				c.prepare(tablePrefix+"insert"+typeStr+"ids", ssi.str());

				if(ocdnSupported)
				{
					work->prepared(tablePrefix+"insert"+typeStr+"ids")(objId).exec();
				}
				else
				{
					//For support of older postgreSQL
					//Check if id already exists in table
					stringstream ssi2;
					ssi2 << "SELECT COUNT(id) FROM "<< c.quote_name(tablePrefix+typeStr+"ids") << " WHERE id=$1;";
					c.prepare(tablePrefix+"insert"+typeStr+"idexists", ssi2.str());
					pqxx::result r = work->prepared(tablePrefix+"insert"+typeStr+"idexists")(objId).exec();
					
					if(r.size() == 0)
						throw runtime_error("No data returned from db unexpectedly");
					const pqxxrow row = r[0];
					int64_t count = row[0].as<int64_t>();

					//Insert id value if not already in table
					if(count == 0)
						work->prepared(tablePrefix+"insert"+typeStr+"ids")(objId).exec();
				}
			}
			else
			{
				//Update row in place
				stringstream ss;
				ss << "UPDATE "<< c.quote_name(tablePrefix+"live"+typeStr+"s") << " SET changeset=$1, username=$2, uid=$3, timestamp=$4, version=$5, tags=$6";
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
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)(tagsJson)(objId)(refsJson).exec();
					}
					else if(relationObject != nullptr)
					{
						if(verbose >= 1)
							cout << ss.str() << endl;
						c.prepare(tablePrefix+"updaterelation", ss.str());
						work->prepared(tablePrefix+"updaterelation")(osmObject->metaData.changeset)(osmObject->metaData.username)\
							(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
							(tagsJson)(objId)(refsJson)(rolesJson).exec();
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
			ss << "INSERT INTO "<< c.quote_name(tablePrefix+"old"+typeStr+"s")+" (id, changeset, username, uid, timestamp, version, tags, visible";
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

			ss << "($1,$2,$3,$4,$5,$6,$7,$8";
			if(nodeObject != nullptr)
				ss << ",ST_GeometryFromText($9, 4326)";
			else if(wayObject != nullptr)
				ss << ",$9";
			else if(relationObject != nullptr)
				ss << ",$9,$10";
			ss << ") "<< ocdn << ";";

			try
			{	
				if(nodeObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"insertoldnode", ss.str());
					work->prepared(tablePrefix+"insertoldnode")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
						(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
						(tagsJson)(osmObject->metaData.visible)(wkt.str()).exec();
				}
				else if(wayObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"insertoldway", ss.str());
					work->prepared(tablePrefix+"insertoldway")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
						(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
						(tagsJson)(osmObject->metaData.visible)(refsJson).exec();
				}
				else if(relationObject != nullptr)
				{
					if(verbose >= 1)
						cout << ss.str() << endl;
					c.prepare(tablePrefix+"insertoldrelation", ss.str());
					work->prepared(tablePrefix+"insertoldrelation")(objId)(osmObject->metaData.changeset)(osmObject->metaData.username)\
						(osmObject->metaData.uid)(osmObject->metaData.timestamp)(osmObject->metaData.version)\
						(tagsJson)(osmObject->metaData.visible)(refsJson)(rolesJson).exec();
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
			ssi << "INSERT INTO "<< c.quote_name(tablePrefix+typeStr+"ids") +" (id) VALUES ($1) "<< ocdn << ";";

			if(verbose >= 1)
				cout << ssi.str() << endl;
			c.prepare(tablePrefix+"insert"+typeStr+"ids2", ssi.str());
			if(ocdnSupported)
			{
				work->prepared(tablePrefix+"insert"+typeStr+"ids2")(objId).exec();
			}
			else
			{
				//For support of older postgreSQL
				//Check if id already exists in table
				stringstream ssi2;
				ssi2 << "SELECT COUNT(id) FROM "<< c.quote_name(tablePrefix+typeStr+"ids") << " WHERE id=$1;";
				c.prepare(tablePrefix+"insert"+typeStr+"idexists2", ssi2.str());
				pqxx::result r = work->prepared(tablePrefix+"insert"+typeStr+"idexists2")(objId).exec();
			
				if(r.size() == 0)
					throw runtime_error("No data returned from db unexpectedly");
				const pqxxrow row = r[0];
				int64_t count = row[0].as<int64_t>();

				//Insert id value if not already in table
				if(count == 0)
					work->prepared(tablePrefix+"insert"+typeStr+"ids2")(objId).exec();
			}
		}

		if(wayObject != nullptr)
		{
			//Update way member table
			size_t j=0;
			while(j < wayObject->refs.size())
			{
				stringstream sswm;
				size_t insertSize = 0;
				sswm << "INSERT INTO "<< c.quote_name(tablePrefix+"way_mems") + " (id, version, index, member) VALUES ";

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
				ssrm << "INSERT INTO "<< c.quote_name(tablePrefix+"relation_mems_"+relationObject->refTypeStrs[j][0]) << " (id, version, index, member) VALUES ";
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

bool StoreObjects(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tablePrefix, 
	class OsmData osmData, 
	std::map<int64_t, int64_t> &createdNodeIds, 
	std::map<int64_t, int64_t> &createdWayIds,
	std::map<int64_t, int64_t> &createdRelationIds,
	std::string &errStr)
{
	map<string, int64_t> nextIdMapOriginal, nextIdMap;
	bool ok = GetNextObjectIds(c, work, tablePrefix, nextIdMapOriginal, errStr);
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

	ok = UpdateNextObjectIds(c, work, tablePrefix, nextIdMap, nextIdMapOriginal, errStr);
	if(!ok)
		return false;
	return true;
}

int UpdateWayBboxesById(pqxx::connection &c, pqxx::transaction_base *work,
	const std::set<int64_t> &wayIds,
    int verbose,
	const std::string &tablePrefix, 
	std::string &errStr)
{
	stringstream sqlFrags;
	int count = 0;
	string wayTable = tablePrefix+"liveways";
	for(std::set<int64_t>::const_iterator it = wayIds.begin(); it != wayIds.end(); it++)
	{
		if(count >= 1)
			sqlFrags << " OR ";
		sqlFrags << wayTable << ".id = " << *it;
		count ++;

		if(count > 100)
		{
			string sql = "UPDATE "+tablePrefix+"liveways SET bbox=ST_Envelope(ST_Union(ARRAY(SELECT geom FROM "+tablePrefix+"visiblenodes WHERE "+tablePrefix+"visiblenodes.id::bigint = ANY(ARRAY(SELECT jsonb_array_elements("+tablePrefix+"liveways.members))::text[]::bigint[])))) WHERE ("+sqlFrags.str()+");";
			//cout << sql << endl;

			work->exec(sql);

			sqlFrags.str("");
			count = 0;
		}
	}

	if(count > 0)
	{
		string sql = "UPDATE "+tablePrefix+"liveways SET bbox=ST_Envelope(ST_Union(ARRAY(SELECT geom FROM "+tablePrefix+"visiblenodes WHERE "+tablePrefix+"visiblenodes.id::bigint = ANY(ARRAY(SELECT jsonb_array_elements("+tablePrefix+"liveways.members))::text[]::bigint[])))) WHERE ("+sqlFrags.str()+");";
		//cout << sql << endl;

		work->exec(sql);
	}

	return 0;	
}

int UpdateRelationBboxesById(pqxx::connection &conn, pqxx::transaction_base *work,
	const std::set<int64_t> &objectIds,
    int verbose,
	const std::string &tablePrefix, 
	std::string &errStr)
{
	class DbUsernameLookup dbUsernameLookup(conn, work, "", ""); //Don't care about accurate usernames
	std::set<int64_t> pendingRelIds = objectIds;
	int passNum = 0;

	while(pendingRelIds.size() > 0 and passNum < 10)
	{
		std::set<int64_t> nextPassIds;
		//cout << "UpdateRelationBboxesById pass " << passNum << ", pending " << pendingRelIds.size() << endl;

		for(auto it1 = pendingRelIds.begin(); it1 != pendingRelIds.end(); it1 ++)
		{
			std::set<int64_t> singleObjId;
			singleObjId.insert(*it1);
			auto firstRecord = singleObjId.begin();

			std::shared_ptr<class OsmData> relObjs(new class OsmData());
			GetVisibleObjectsById(conn, work, dbUsernameLookup,
				tablePrefix,
				"relation",
				singleObjId, 
				firstRecord, 1000, relObjs);
			
			if(relObjs->relations.size()==0)
				continue;
			OsmRelation &rel = relObjs->relations[0];
	
			std::set<int64_t> memNodeIds, memWayIds, memRelIds;
			for(size_t i=0; i<rel.refTypeStrs.size(); i++)
			{
				string &memType = rel.refTypeStrs[i];
				if(memType == "node") memNodeIds.insert(rel.refIds[i]);
				if(memType == "way") memWayIds.insert(rel.refIds[i]);
				if(memType == "relation") memRelIds.insert(rel.refIds[i]);
			}

			//Check of member relations have already been processed
			bool dependsOnPending = false;
			for(auto it2=memRelIds.begin(); it2!=memRelIds.end(); it2++)
			{
				dependsOnPending = pendingRelIds.find(*it2) != pendingRelIds.end();
				if(dependsOnPending) break;
			}
			if(dependsOnPending)
			{
				nextPassIds.insert(rel.objId);
				continue;
			}

			std::map<int64_t, vector<double> > memBboxesOfType1, memBboxesOfType2, memBboxesOfType3;
			std::vector<vector<double> > memBboxes;
			GetVisibleObjectBboxesById(conn, work, dbUsernameLookup,
				tablePrefix, "node", memNodeIds, memBboxesOfType1);
			for(auto it=memBboxesOfType1.begin(); it!=memBboxesOfType1.end(); it++)
			{
				vector<double> &bb = it->second; 
				memBboxes.push_back(bb);
			}

			GetVisibleObjectBboxesById(conn, work, dbUsernameLookup,
				tablePrefix, "way", memWayIds, memBboxesOfType2);
			for(auto it=memBboxesOfType2.begin(); it!=memBboxesOfType2.end(); it++)
			{
				vector<double> &bb = it->second; 
				memBboxes.push_back(bb);
			}

			GetVisibleObjectBboxesById(conn, work, dbUsernameLookup,
				tablePrefix, "relation", memRelIds, memBboxesOfType3);
			for(auto it=memBboxesOfType3.begin(); it!=memBboxesOfType3.end(); it++)
			{
				vector<double> &bb = it->second; 
				memBboxes.push_back(bb);
			}

			std::vector<double> outerBbox;
			FindOuterBbox(memBboxes, outerBbox);

			if(outerBbox.size() == 4)
			{
				stringstream sql;
				sql.precision(9);
				sql << fixed << "UPDATE " << tablePrefix << "liverelations SET bbox=ST_MakeEnvelope("<<outerBbox[0]<<", "\
					<<outerBbox[1]<<", "<<outerBbox[2]<<", "<<outerBbox[3]<<", 4326) WHERE id = "<<rel.objId<<";";
				if(verbose >= 2) cout << sql.str() << endl;

				work->exec(sql);
			}
			else
			{
				stringstream sql;
				sql.precision(9);
				sql << fixed << "UPDATE " << tablePrefix << "liverelations SET bbox=null WHERE id = "<<rel.objId<<";";
				if(verbose >= 2) cout << sql.str() << endl;

				work->exec(sql);
			}
		}

		pendingRelIds = nextPassIds;
		passNum ++;
	}

	return 0;
}


