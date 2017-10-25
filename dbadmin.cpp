#include "dbadmin.h"
#include "dbids.h"
#include "dbcommon.h"
#include <map>
using namespace std;

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
	ok = ClearTable(work, tableActivePrefix + "changesets", errStr);             if(!ok) return false;

	map<string, int64_t> nextIdMap;
	ok = GetNextObjectIds(work, 
		tableStaticPrefix,
		nextIdMap,
		errStr);
	if(!ok) return false;

	map<string, int64_t> emptyIdMap;
	ok = UpdateNextObjectIds(work, tableActivePrefix, nextIdMap, emptyIdMap, errStr);
	if(!ok) return false;

	//Update next changeset and UIDs
	ok = ResetChangesetUidCounts(work, 
		tableStaticPrefix, tableActivePrefix, 
		errStr);
	if(!ok) return false;

	return ok;	
}

