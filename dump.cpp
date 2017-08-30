//g++ dump.cpp util.cpp cppo5m/o5m.cpp cppo5m/varint.cpp -Wall -lpqxx -o dump
#include "cppo5m/o5m.h"
#include <pqxx/pqxx> //apt install libpqxx-dev
#include <fstream>
#include <iostream>
#include <sstream>
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include <assert.h>
#include <time.h>
using namespace std;

struct MetaDataCols
{
	int versionCol;
	int timestampCol;
	int changesetCol;
	int uidCol;
	int usernameCol;
};

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

void DumpNodes(pqxx::connection &dbconn, std::map<string, string> &config, O5mEncode &enc)
{
	cout << "Dump nodes" << endl;
	stringstream sql;
	sql << "SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM ";
	sql << config["dbtableprefix"];
	sql << "nodes WHERE visible=true and current=true;";

	pqxx::work work(dbconn);
	pqxx::icursorstream cursor( work, sql.str(), "nodecursor", 1000 );	
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap tagHandler;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	for ( size_t batch = 0; true; batch ++ )
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read
		if(batch == 0)
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
			if(count % 1000000 == 0)
				cout << count << " nodes" << endl;

			double timeNow = (double)clock() / CLOCKS_PER_SEC;
			if (timeNow - lastUpdateTime > 30.0)
			{
				cout << (count - lastUpdateCount)/30.0 << " nodes/sec" << endl;
				lastUpdateCount = count;
				lastUpdateTime = timeNow;
			}

			enc.StoreNode(objId, metaData, tagHandler.tagMap, lat, lon);
		}
	}
}

void DumpWays(pqxx::connection &dbconn, std::map<string, string> &config, O5mEncode &enc)
{
	cout << "Dump ways" << endl;
	stringstream sql;
	sql << "SELECT * FROM ";
	sql << config["dbtableprefix"];
	sql << "ways WHERE visible=true and current=true;";

	pqxx::work work(dbconn);
	pqxx::icursorstream cursor( work, sql.str(), "waycursor", 1000 );	
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap tagHandler;
	JsonToWayMembers wayMemHandler;
	const std::vector<int64_t> refs;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	for ( size_t batch = 0; true; batch ++ )
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read
		if(batch == 0)
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
			if(count % 1000000 == 0)
				cout << count << " ways" << endl;

			double timeNow = (double)clock() / CLOCKS_PER_SEC;
			if (timeNow - lastUpdateTime > 30.0)
			{
				cout << (count - lastUpdateCount)/30.0 << " ways/sec" << endl;
				lastUpdateCount = count;
				lastUpdateTime = timeNow;
			}

			enc.StoreWay(objId, metaData, tagHandler.tagMap, wayMemHandler.refs);
		}

	}
}

void DumpRelations(pqxx::connection &dbconn, std::map<string, string> &config, O5mEncode &enc)
{
	cout << "Dump relations" << endl;
	stringstream sql;
	sql << "SELECT * FROM ";
	sql << config["dbtableprefix"];
	sql << "relations WHERE visible=true and current=true;";

	pqxx::work work(dbconn);
	pqxx::icursorstream cursor( work, sql.str(), "relationcursor", 1000 );	
	uint64_t count = 0;
	class MetaData metaData;
	JsonToStringMap tagHandler;
	JsonToRelMembers relMemHandler;
	JsonToRelMemberRoles relMemRolesHandler;
	std::vector<std::string> refRoles;
	const std::vector<int64_t> refs;
	double lastUpdateTime = (double)clock() / CLOCKS_PER_SEC;
	uint64_t lastUpdateCount = 0;
	for ( size_t batch = 0; true; batch ++ )
	{
		pqxx::result rows;
		cursor.get(rows);
		if ( rows.empty() ) break; // nothing left to read
		if(batch == 0)
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

			DecodeMetadata(c, metaDataCols, metaData);
			
			DecodeTags(c, tagsCol, tagHandler);

			DecodeRelMembers(c, membersCol, membersRolesCol, 
				relMemHandler, relMemRolesHandler);

			count ++;
			if(count % 1000000 == 0)
				cout << count << " relations" << endl;

			double timeNow = (double)clock() / CLOCKS_PER_SEC;
			if (timeNow - lastUpdateTime > 30.0)
			{
				cout << (count - lastUpdateCount)/30.0 << " relations/sec" << endl;
				lastUpdateCount = count;
				lastUpdateTime = timeNow;
			}

			enc.StoreRelation(objId, metaData, tagHandler.tagMap, 
				relMemHandler.refTypeStrs, relMemHandler.refIds, relMemRolesHandler.refRoles);
		}

	}
}

int main(int argc, char **argv)
{	
	std::filebuf outfi;
	outfi.open("dump.o5m.gz", std::ios::out);
	EncodeGzip *gzipEnc = new class EncodeGzip(outfi);

	O5mEncode enc(*gzipEnc);
	enc.StoreIsDiff(false);

	string configContent;
	cout << "Reading settings from config.cfg" << endl;
	ReadFileContents("config.cfg", false, configContent);
	std::vector<std::string> lines = split(configContent, '\n');
	std::map<string, string> config;
	for(size_t i=0; i < lines.size(); i++)
	{
		const std::string &line = lines[i];
		std::vector<std::string> parts = split(line, ':');
		if (parts.size() < 2) continue;
		config[parts[0]] = parts[1];
	}
	
	std::stringstream ss;
	ss << "dbname=";
	ss << config["dbname"];
	ss << " user=";
	ss << config["dbuser"];
	ss << " password='";
	ss << config["dbpass"];
	ss << "' hostaddr=";
	ss << config["dbhost"];
	ss << " port=";
	ss << "5432";
	
	pqxx::connection dbconn(ss.str());
	if (dbconn.is_open()) {
		cout << "Opened database successfully: " << dbconn.dbname() << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}
	
	DumpNodes(dbconn, config, enc);

	enc.Reset();

	DumpWays(dbconn, config, enc);

	enc.Reset();
		
	DumpRelations(dbconn, config, enc);

	enc.Finish();
	delete gzipEnc;
	outfi.close();
	dbconn.disconnect ();
	return 0;
}
