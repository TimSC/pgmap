#include "dbchangeset.h"
#include "util.h"
#include "dbstore.h"
#include "dbcommon.h"
using namespace std;

void DecodeRowsToChangesets(pqxx::result &rows, std::vector<class PgChangeset> &changesets)
{
	int idCol = rows.column_number("id");
	int usernameCol = rows.column_number("username");
	int uidCol = rows.column_number("uid");
	int tagsCol = rows.column_number("tags");
	int openTimestampCol = rows.column_number("open_timestamp");
	int closeTimestampCol = rows.column_number("close_timestamp");
	int isOpenCol = rows.column_number("is_open");
	int xminCol = rows.column_number("xmin");
	int xmaxCol = rows.column_number("xmax");
	int yminCol = rows.column_number("ymin");
	int ymaxCol = rows.column_number("ymax");

	Reader reader;
	class JsonToStringMap tagHandler;

	for (unsigned int rownum=0; rownum < rows.size(); ++rownum)
	{
		const pqxx::result::tuple row = rows[rownum];

		class PgChangeset changeset;
		changeset.objId = row[idCol].as<int64_t>();
		if(!row[uidCol].is_null())
			changeset.uid = row[uidCol].as<int64_t>();
		if(!row[openTimestampCol].is_null())
			changeset.open_timestamp = row[openTimestampCol].as<int64_t>();
		if(!row[closeTimestampCol].is_null())
			changeset.close_timestamp = row[closeTimestampCol].as<int64_t>();
		if(!row[usernameCol].is_null())
			changeset.username = row[usernameCol].as<string>();
		changeset.is_open = row[isOpenCol].as<bool>();

		if (!row[tagsCol].is_null())
		{
			tagHandler.tagMap.clear();

			// Value needs to be copied out to string or rapidjson has problems
			string tmp = row[tagsCol].as<string>();
			StringStream ss(tmp.c_str());
			reader.Parse(ss, tagHandler);
			changeset.tags = tagHandler.tagMap;
		}

		if (!row[xminCol].is_null())
		{
			changeset.bbox_set = true;
			changeset.x1 = row[xminCol].as<double>();
			changeset.y1 = row[yminCol].as<double>();
			changeset.x2 = row[xmaxCol].as<double>();
			changeset.y2 = row[ymaxCol].as<double>();
		}

		changesets.push_back(changeset);
	}
}

int GetChangesetFromDb(pqxx::work *work, 
	const std::string &tablePrefix,
	int64_t objId,
	class PgChangeset &changesetOut,
	std::string &errStr)
{
	string changesetTable = tablePrefix + "changesets";

	stringstream sql;
	sql << "SELECT *, ST_XMin(geom) as xmin, ST_XMax(geom) as xmax,";
	sql << " ST_YMin(geom) as ymin, ST_YMax(geom) as ymax FROM " << changesetTable << " WHERE id = " << objId << ";";

	pqxx::result r = work->exec(sql.str());
	if(r.size() < 1)
	{
		errStr = "Changeset not found";
		return -1;
	}

	std::vector<class PgChangeset> changesets;
	DecodeRowsToChangesets(r, changesets);
	changesetOut = changesets[0];
	return 1;
}

bool GetChangesetsFromDb(pqxx::work *work, 
	const std::string &tablePrefix,
	const std::string &excludePrefix,
	size_t limit,
	std::vector<class PgChangeset> &changesetOut,
	std::string &errStr)
{
	string changesetTable = tablePrefix + "changesets";
	string excludeTable;
	if(excludePrefix.size() > 0)
		excludeTable = excludePrefix + "changesets";

	stringstream sql;
	sql << "SELECT "<<changesetTable<<".*, ST_XMin("<<changesetTable<<".geom) as xmin, ST_XMax("<<changesetTable<<".geom) as xmax,";
	sql << " ST_YMin("<<changesetTable<<".geom) as ymin, ST_YMax("<<changesetTable<<".geom) as ymax";
	sql << " FROM " << changesetTable;
	if(excludeTable.size() > 0)
	{
		sql << " LEFT JOIN " << excludeTable << " ON " << changesetTable <<".id = " << excludeTable << ".id";
		sql << " WHERE " << excludeTable << ".id IS NULL";
	}
	sql << " ORDER BY open_timestamp DESC NULLS LAST LIMIT "<<limit<<";";

	pqxx::result r = work->exec(sql.str());

	DecodeRowsToChangesets(r, changesetOut);
	return true;
}

bool SetChangesetInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &tablePrefix,
	const class PgChangeset &changeset,
	std::string &errStr)
{
	//Insert into live table
	stringstream ss;
	ss << "INSERT INTO "<< tablePrefix <<"changesets (id, username, uid, tags, open_timestamp, close_timestamp, is_open, geom)";
	ss << " VALUES ($1,$2,$3,$4,$5,$6,$7,ST_MakeEnvelope($8, $9, $10, $11, 4326));";

	try
	{
		c.prepare(tablePrefix+"insertchangeset", ss.str());

		pqxx::prepare::invocation invoc = work->prepared(tablePrefix+"insertchangeset");
		invoc(changeset.objId);
		if(changeset.username.size() > 0)
			invoc(changeset.username);
		else
			invoc();
		if(changeset.uid != 0)
			invoc(changeset.uid);
		else
			invoc();

		string tagJson;
		EncodeTags(changeset.tags, tagJson);
		invoc(tagJson);

		if(changeset.open_timestamp != 0)
			invoc(changeset.open_timestamp);
		else
			invoc();
		if(changeset.close_timestamp != 0)
			invoc(changeset.close_timestamp);
		else
			invoc();
		invoc(changeset.is_open);
		if(changeset.bbox_set)
		{
			invoc(changeset.x1);
			invoc(changeset.y1);
			invoc(changeset.x2);
			invoc(changeset.y2);
		}
		else
		{
			invoc();
			invoc();
			invoc();
			invoc();
		}

		invoc.exec();
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

int UpdateChangesetInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &tablePrefix,
	const class PgChangeset &changeset,
	std::string &errStr)
{
	//Insert into live table
	stringstream ss;
	ss << "UPDATE "<< tablePrefix <<"changesets SET username=$1, uid=$2, tags=$3, open_timestamp=$4, close_timestamp=$5, ";
	ss << "is_open=$6, geom=ST_MakeEnvelope($7, $8, $9, $10, 4326)";
	ss << " WHERE id = $11;";
	int rowsAffected = 0;

	try
	{
		c.prepare(tablePrefix+"updatechangeset", ss.str());

		pqxx::prepare::invocation invoc = work->prepared(tablePrefix+"updatechangeset");
		invoc(changeset.username);
		invoc(changeset.uid);
		string tagJson;
		EncodeTags(changeset.tags, tagJson);
		invoc(tagJson);
		invoc(changeset.open_timestamp);
		invoc(changeset.close_timestamp);
		invoc(changeset.is_open);
		if(changeset.bbox_set)
		{
			invoc(changeset.x1);
			invoc(changeset.y1);
			invoc(changeset.x2);
			invoc(changeset.y2);
		}
		else
		{
			invoc();
			invoc();
			invoc();
			invoc();
		}
		invoc(changeset.objId);

		pqxx::result result = invoc.exec();
		rowsAffected = result.affected_rows();
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = e.what();
		return -1;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return -1;
	}
	return rowsAffected;
}

bool CloseChangesetInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &tablePrefix,
	int64_t changesetId,
	int64_t closedTimestamp,
	size_t &rowsAffectedOut,
	std::string &errStr)
{
	stringstream ss;
	ss << "UPDATE "<< tablePrefix <<"changesets SET is_open=false WHERE id = "<<changesetId<<";";

	bool ok = DbExec(work, ss.str(), errStr, &rowsAffectedOut);

	return ok;
}

bool CopyChangesetToActiveInDb(pqxx::connection &c, 
	pqxx::work *work, 
	const std::string &staticPrefix,
	const std::string &activePrefix,
	int64_t changesetId,
	size_t &rowsAffected,
	std::string &errStrNative)
{
	class PgChangeset changeset;
	int ret = GetChangesetFromDb(work, 
		staticPrefix,
		changesetId,
		changeset,
		errStrNative);

	if(ret <= 0)
		return ret != 0;

	bool ok = SetChangesetInDb(c, 
		work, 
		activePrefix,
		changeset,
		errStrNative);

	return ok;
}

