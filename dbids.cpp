#include "dbids.h"
#include "dbcommon.h"
#include "dbprepared.h"
using namespace std;

#if PQXX_VERSION_MAJOR >= 6
#define pqxxfield pqxx::field
#define pqxxrow pqxx::row
#else
#define pqxxfield pqxx::result::field
#define pqxxrow pqxx::result::tuple
#endif

bool GetMaxObjIdLiveOrOld(pqxx::connection &c, pqxx::transaction_base *work, const string &tablePrefix, 
	const std::string &objType, 
	const std::string &field,
	std::string errStr,
	int64_t &val)
{
	val = 0;
	int64_t val1, val2;
	stringstream ss;
	ss << tablePrefix << "live" << objType << "s";
	bool ok = GetMaxFieldInTable(c, work, ss.str(), field, errStr, val1);
	if(!ok) return false;
	stringstream ss2;
	ss2 << tablePrefix << "old" << objType << "s";
	ok = GetMaxFieldInTable(c, work, ss2.str(), field, errStr, val2);
	if(val2 > val1)
		val = val2;
	else
		val = val1;
	return ok;
}

bool GetMaxFieldInTable(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tableName,
	const string &field,
	string &errStr,
	int64_t &val)
{
	val = 0;
	stringstream ss;
	ss << "SELECT MAX("<<c.quote_name(field)<<") FROM "<<c.quote_name(tableName)<<";";
	pqxx::result r = work->exec(ss.str());
	if(r.size()==0)
	{
		errStr = "No rows returned";
		return false;
	}
	const pqxxrow row = r[0];
	const pqxxfield fieldObj = row[0];
	if(fieldObj.is_null())
		return true; //Return zero
	val = fieldObj.as<int64_t>();
	return true;
}

bool ClearNextIdValuesById(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tablePrefix,
	const string &key)
{
	stringstream ss;
	ss << "DELETE FROM "<< c.quote_name(tablePrefix+"nextids")<<" WHERE id="<<c.quote(key)<<";";
	work->exec(ss.str());
	return true;
}

bool SetNextIdValue(pqxx::connection &c,
	pqxx::transaction_base *work, 
	const string &tablePrefix,
	const string &objType,
	int64_t value)
{
	stringstream ss;
	ss << "INSERT INTO "<<c.quote_name(tablePrefix+"nextids") << "(id, maxid) VALUES ($1, $2);";

	prepare_deduplicated(c, tablePrefix+"setnextId", ss.str());

	pqxx::params params;
	params.reserve(2);
	params.append(objType);
	params.append(value);

	work->exec_prepared(tablePrefix+"setnextId", params);
	return true;
}

bool GetNextId(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tablePrefix,
	const string &objType,
	string &errStr,
	int64_t &out)
{
	out = -1;
	stringstream sstr;
	sstr << "SELECT maxid FROM "<< c.quote_name(tablePrefix+"nextids") << " WHERE id="<<c.quote(objType)<<";";

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
	const pqxxrow row = r[0];
	const pqxxfield field = row[0];
	if(field.is_null())
		throw runtime_error("Cannot determine ID to allocate");
	out = row[0].as<int64_t>();
	return true;
}

bool GetAllocatedIdFromDb(pqxx::connection &c,
	pqxx::transaction_base *work, 
	const string &tablePrefix,
	const string &objType,
	bool increment,
	string &errStr,
	int64_t &val)
{
	bool ok = GetNextId(c, work, 
		tablePrefix,
		objType,
		errStr,
		val);
	if(!ok) return false;

	if(increment)
	{
		stringstream ss;
		ss << "UPDATE "<< c.quote_name(tablePrefix+"nextids") <<" SET maxid="<< (val+1) <<" WHERE id ="<<c.quote(objType)<<";";	
		ok = DbExec(work, ss.str(), errStr);
	}
	return ok;
}

bool ResetChangesetUidCounts(pqxx::connection &c, pqxx::transaction_base *work, 
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
		try
		{
			ok = GetNextId(c, work, parentPrefix, "changeset", errStr, val);
			if(!ok) return false;
			if(val > maxVal)
				maxVal = val;
		}
		catch (runtime_error &err)
		{
			//Changeset ID max not defined in parent table
		}
	}
	ok = GetMaxObjIdLiveOrOld(c, work, tablePrefix, "node", "changeset", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxObjIdLiveOrOld(c, work, tablePrefix, "way", "changeset", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxObjIdLiveOrOld(c, work, tablePrefix, "relation", "changeset", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxFieldInTable(c, work, st.str(), "id", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;

	stringstream ss;
	ss << "DELETE FROM "<<c.quote_name(tablePrefix+"nextids") << " WHERE id = 'changeset';";
	ok = DbExec(work, ss.str(), errStr);
	if(!ok) return false;
	stringstream ss2;
	ss2 << "INSERT INTO "<<c.quote_name(tablePrefix+"nextids") << " (id, maxid) VALUES ('changeset', "<<(maxVal+1)<<");";
	ok = DbExec(work, ss2.str(), errStr);
	if(!ok) return false;

	//UID
	maxVal = 0;
	if(parentPrefix.size()>0)
	{
		try
		{
			ok = GetNextId(c, work, parentPrefix, "uid", errStr, val);
			if(!ok) return false;
			if(val > maxVal)
				maxVal = val;
		}
		catch (runtime_error &err)
		{
			//UID max not defined in parent table
		}
	}
	ok = GetMaxObjIdLiveOrOld(c, work, tablePrefix, "node", "uid", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxObjIdLiveOrOld(c, work, tablePrefix, "way", "uid", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxObjIdLiveOrOld(c, work, tablePrefix, "relation", "uid", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;
	ok = GetMaxFieldInTable(c, work, st.str(), "uid", errStr, val);
	if(!ok) return false; if(val > maxVal) maxVal = val;

	stringstream ss3;
	ss3 << "DELETE FROM "<<c.quote_name(tablePrefix+"nextids") <<" WHERE id = 'uid';";
	ok = DbExec(work, ss3.str(), errStr);
	if(!ok) return false;
	stringstream ss4;
	ss4 << "INSERT INTO "<<c.quote_name(tablePrefix+"nextids") << " (id, maxid) VALUES ('uid', "<<(maxVal+1)<<");";
	ok = DbExec(work, ss4.str(), errStr);
	if(!ok) return false;

	return true;
}

bool GetNextObjectIds(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &tablePrefix,
	map<string, int64_t> &nextIdMap,
	string &errStr)
{
	nextIdMap.clear();
	stringstream sstr;
	sstr << "SELECT RTRIM(id), maxid FROM "<< c.quote_name(tablePrefix+"nextids") << ";";

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
		const pqxxrow row = r[rownum];
		string id;
		int64_t maxid = 0;
		for (unsigned int colnum=0; colnum < row.size(); ++colnum)
		{
			const pqxxfield field = row[colnum];
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

bool UpdateNextObjectIds(pqxx::connection &c, pqxx::transaction_base *work, 
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
				ss << "UPDATE "<< c.quote_name(tablePrefix+"nextids") << " SET maxid = "<< it->second <<" WHERE id="<<c.quote(it->first)<<";"; 

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
			ss << "INSERT INTO "<< c.quote_name(tablePrefix+"nextids") << " (id, maxid) VALUES ("<<c.quote(it->first)<<","<<it->second<<");";

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

bool UpdateNextIdsOfType(pqxx::connection &c, pqxx::transaction_base *work, 
	const string &objType,
	const string &tableActivePrefix, 
	const string &tableStaticPrefix,
	std::string &errStr)
{
	stringstream ss;
	ss << tableStaticPrefix << objType << "s";
	int64_t maxStaticNode, maxActiveNode;
	bool ok = GetMaxFieldInTable(c, work, ss.str(), "id", errStr, maxStaticNode);
	if(!ok) return false;
	stringstream ss2;
	ss2 << tableActivePrefix << objType << "s";
	ok = GetMaxFieldInTable(c, work, ss2.str(), "id", errStr, maxActiveNode);
	if(!ok) return false;
	if(maxStaticNode < 1) maxStaticNode = 1;
	if(maxActiveNode == -1) maxActiveNode = maxStaticNode;
	
	SetNextIdValue(c, work, tableStaticPrefix, objType, maxStaticNode+1);
	SetNextIdValue(c, work, tableActivePrefix, objType, maxActiveNode+1);
	return true;
}

