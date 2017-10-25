#include "dbids.h"
#include "dbcommon.h"

bool GetMaxObjIdLiveOrOld(pqxx::work *work, const string &tablePrefix, 
	const std::string &objType, 
	const std::string &field,
	std::string errStr,
	int64_t &val)
{
	val = 0;
	int64_t val1, val2;
	stringstream ss;
	ss << tablePrefix << "live" << objType << "s";
	bool ok = GetMaxFieldInTable(work, ss.str(), field, errStr, val1);
	if(!ok) return false;
	stringstream ss2;
	ss2 << tablePrefix << "old" << objType << "s";
	ok = GetMaxFieldInTable(work, ss2.str(), field, errStr, val2);
	if(val2 > val1)
		val = val2;
	else
		val = val1;
	return ok;
}

bool GetMaxFieldInTable(pqxx::work *work, 
	const string &tableName,
	const string &field,
	string &errStr,
	int64_t &val)
{
	val = 0;
	stringstream ss;
	ss << "SELECT MAX("<<field<<") FROM "<<tableName<<";";
	pqxx::result r = work->exec(ss.str());
	if(r.size()==0)
	{
		errStr = "No rows returned";
		return false;
	}
	const pqxx::result::tuple row = r[0];
	const pqxx::result::field fieldObj = row[0];
	if(fieldObj.is_null())
		return true; //Return zero
	val = fieldObj.as<int64_t>();
	return true;
}

bool ClearNextIdValues(pqxx::work *work, 
	const string &tablePrefix)
{
	stringstream ss;
	ss << "DELETE FROM "<< tablePrefix <<"nextids;";
	work->exec(ss.str());
	return true;
}

bool SetNextIdValue(pqxx::connection &c,
	pqxx::work *work, 
	const string &tablePrefix,
	const string &objType,
	int64_t value)
{
	stringstream ss;
	ss << "INSERT INTO "<<tablePrefix<<"nextids(id, maxid) VALUES ($1, $2);";

	c.prepare(tablePrefix+"setnextId", ss.str());
	work->prepared(tablePrefix+"setnextId")(objType)(value).exec();
	return true;
}

bool GetNextId(pqxx::work *work, 
	const string &tablePrefix,
	const string &objType,
	string &errStr,
	int64_t &out)
{
	out = -1;
	stringstream sstr;
	sstr << "SELECT maxid FROM "<< tablePrefix <<"nextids WHERE id='"<<objType<<"';";

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

	if(r.size() == 0)
		throw runtime_error("Cannot determine ID to allocate");
	const pqxx::result::tuple row = r[0];
	const pqxx::result::field field = row[0];
	if(field.is_null())
		throw runtime_error("Cannot determine ID to allocate");
	out = row[0].as<int64_t>();
	return true;
}

bool GetAllocatedIdFromDb(pqxx::connection &c,
	pqxx::work *work, 
	const string &tablePrefix,
	const string &objType,
	bool increment,
	string &errStr,
	int64_t &val)
{
	bool ok = GetNextId(work, 
		tablePrefix,
		objType,
		errStr,
		val);
	if(!ok) return false;

	if(increment)
	{
		stringstream ss;
		ss << "UPDATE "<< tablePrefix <<"nextids SET maxid="<< val+1 <<" WHERE id ='"<<objType<<"';";	
		ok = DbExec(work, ss.str(), errStr);
	}
	return ok;
}

bool ResetChangesetUidCounts(pqxx::work *work, 
	const string &parentPrefix, const string &tablePrefix, 
	string &errStr)
{
	//Changeset
	bool ok = true;
	int64_t maxVal = 0;
	int64_t val = 0;
	stringstream st;
	st << tablePrefix << "changesets";

	if(parentPrefix.size()>0)
	{
		ok = GetNextId(work, parentPrefix, "changeset", errStr, val);
		if(!ok) return false;
		if(val > maxVal)
			maxVal = val;
	}
	ok = GetMaxObjIdLiveOrOld(work, tablePrefix, "node", "changeset", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxObjIdLiveOrOld(work, tablePrefix, "way", "changeset", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxObjIdLiveOrOld(work, tablePrefix, "relation", "changeset", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxFieldInTable(work, st.str(), "id", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;

	stringstream ss;
	ss << "DELETE FROM "<<tablePrefix<<"nextids WHERE id = 'changeset';";
	ok = DbExec(work, ss.str(), errStr);
	if(!ok) return false;
	stringstream ss2;
	ss2 << "INSERT INTO "<<tablePrefix<<"nextids (id, maxid) VALUES ('changeset', "<<(maxVal+1)<<");";
	ok = DbExec(work, ss2.str(), errStr);
	if(!ok) return false;

	//UID
	maxVal = 0;
	if(parentPrefix.size()>0)
	{
		ok = GetNextId(work, parentPrefix, "uid", errStr, val);
		if(!ok) return false;
		if(val > maxVal)
			maxVal = val;
	}
	ok = GetMaxObjIdLiveOrOld(work, tablePrefix, "node", "uid", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxObjIdLiveOrOld(work, tablePrefix, "way", "uid", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxObjIdLiveOrOld(work, tablePrefix, "relation", "uid", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxFieldInTable(work, st.str(), "uid", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;

	stringstream ss3;
	ss3 << "DELETE FROM "<<tablePrefix<<"nextids WHERE id = 'uid';";
	ok = DbExec(work, ss3.str(), errStr);
	if(!ok) return false;
	stringstream ss4;
	ss4 << "INSERT INTO "<<tablePrefix<<"nextids (id, maxid) VALUES ('uid', "<<(maxVal+1)<<");";
	ok = DbExec(work, ss4.str(), errStr);
	if(!ok) return false;

	return true;
}

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

bool UpdateNextIdsOfType(pqxx::connection &c, pqxx::work *work, 
	const string &objType,
	const string &tableActivePrefix, 
	const string &tableStaticPrefix,
	std::string &errStr)
{
	stringstream ss;
	ss << tableStaticPrefix << objType << "s";
	int64_t maxStaticNode, maxActiveNode;
	bool ok = GetMaxFieldInTable(work, ss.str(), "id", errStr, maxStaticNode);
	if(!ok) return false;
	stringstream ss2;
	ss2 << tableActivePrefix << objType << "s";
	ok = GetMaxFieldInTable(work, ss2.str(), "id", errStr, maxActiveNode);
	if(!ok) return false;
	if(maxStaticNode < 1) maxStaticNode = 1;
	if(maxActiveNode == -1) maxActiveNode = maxStaticNode;
	
	SetNextIdValue(c, work, tableStaticPrefix, objType, maxStaticNode+1);
	SetNextIdValue(c, work, tableActivePrefix, objType, maxActiveNode+1);
	return true;
}

