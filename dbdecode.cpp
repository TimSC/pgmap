#include "dbdecode.h"
using namespace std;

void DecodeMetadata(const pqxx::result::const_iterator &c, const MetaDataCols &metaDataCols, class MetaData &metaData)
{
	if (c[metaDataCols.versionCol].is_null())
		metaData.version = 0;
	else
		metaData.version = c[metaDataCols.versionCol].as<uint64_t>();
	if (c[metaDataCols.timestampCol].is_null())
		metaData.timestamp = 0;
	else
		metaData.timestamp = c[metaDataCols.timestampCol].as<int64_t>();
	if (c[metaDataCols.changesetCol].is_null())
		metaData.changeset = 0;
	else
		metaData.changeset = c[metaDataCols.changesetCol].as<int64_t>();
	if (c[metaDataCols.uidCol].is_null())
		metaData.uid = 0;
	else
		metaData.uid = c[metaDataCols.uidCol].as<uint64_t>();
	if (c[metaDataCols.usernameCol].is_null())
		metaData.username = "";
	else
		metaData.username = c[metaDataCols.usernameCol].c_str();
	metaData.visible = true;
	if(metaDataCols.visibleCol >= 0)
		metaData.visible = c[metaDataCols.visibleCol].as<bool>();
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

int NodeResultsToEncoder(pqxx::icursorstream &cursor, class DbUsernameLookup &usernames, 
	std::shared_ptr<IDataStreamHandler> enc)
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
	metaDataCols.timestampCol = rows.column_number("timestamp");
	metaDataCols.versionCol = rows.column_number("version");
	metaDataCols.visibleCol = -1;
	try
	{
		metaDataCols.visibleCol = rows.column_number("visible");
	}
	catch (invalid_argument &err) {}
	
	int tagsCol = rows.column_number("tags");
	int latCol = rows.column_number("lat");
	int lonCol = rows.column_number("lon");

	for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

		int64_t objId = c[idCol].as<int64_t>();
		double lat = atof(c[latCol].c_str());
		double lon = atof(c[lonCol].c_str());

		DecodeMetadata(c, metaDataCols, metaData);

		if(&usernames != nullptr)
		{
			string username = usernames.Find(metaData.uid);
			if(username.length() > 0)
				metaData.username = username;
		}
		
		DecodeTags(c, tagsCol, tagHandler);
		count ++;

		double timeNow = (double)clock() / CLOCKS_PER_SEC;
		if (timeNow - lastUpdateTime > 30.0)
		{
			lastUpdateCount = count;
			lastUpdateTime = timeNow;
		}

		if(enc)
			enc->StoreNode(objId, metaData, tagHandler.tagMap, lat, lon);
	}
	return count;
}

int WayResultsToEncoder(pqxx::icursorstream &cursor, class DbUsernameLookup &usernames, 
	std::shared_ptr<IDataStreamHandler> enc)
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
	metaDataCols.timestampCol = rows.column_number("timestamp");
	metaDataCols.versionCol = rows.column_number("version");
	metaDataCols.visibleCol = -1;
	try
	{
		metaDataCols.visibleCol = rows.column_number("visible");
	}
	catch (invalid_argument &err) {}

	int tagsCol = rows.column_number("tags");
	int membersCol = rows.column_number("members");

	for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

		int64_t objId = c[idCol].as<int64_t>();

		DecodeMetadata(c, metaDataCols, metaData);

		if(&usernames != nullptr)
		{
			string username = usernames.Find(metaData.uid);
			if(username.length() > 0)
				metaData.username = username;
		}

		DecodeTags(c, tagsCol, tagHandler);

		DecodeWayMembers(c, membersCol, wayMemHandler);
		count ++;

		double timeNow = (double)clock() / CLOCKS_PER_SEC;
		if (timeNow - lastUpdateTime > 30.0)
		{
			lastUpdateCount = count;
			lastUpdateTime = timeNow;
		}

		if(enc)
			enc->StoreWay(objId, metaData, tagHandler.tagMap, wayMemHandler.refs);
	}
	return count;
}

void RelationResultsToEncoder(pqxx::icursorstream &cursor, class DbUsernameLookup &usernames, 
	const set<int64_t> &skipIds, std::shared_ptr<IDataStreamHandler> enc)
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
		metaDataCols.timestampCol = rows.column_number("timestamp");
		metaDataCols.versionCol = rows.column_number("version");
		metaDataCols.visibleCol = -1;
		try
		{
			metaDataCols.visibleCol = rows.column_number("visible");
		}
		catch (invalid_argument &err) {}

		int tagsCol = rows.column_number("tags");
		int membersCol = rows.column_number("members");
		int membersRolesCol = rows.column_number("memberroles");

		for (pqxx::result::const_iterator c = rows.begin(); c != rows.end(); ++c) {

			int64_t objId = c[idCol].as<int64_t>();
			if(skipIds.find(objId) != skipIds.end())
				continue;

			DecodeMetadata(c, metaDataCols, metaData);
			if(&usernames != nullptr)
			{
				string username = usernames.Find(metaData.uid);
				if(username.length() > 0)
					metaData.username = username;
			}
			
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

			if(enc)
				enc->StoreRelation(objId, metaData, tagHandler.tagMap, 
					relMemHandler.refTypeStrs, relMemHandler.refIds, relMemRolesHandler.refRoles);
		}
	}
}

