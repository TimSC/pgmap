#ifndef _PG_COMMON_H
#define _PG_COMMON_H

#include <string>
#include <set>
#include <map>
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/osmxml.h"
#include "cppo5m/OsmData.h"
#include "dbusername.h"

class PgWork
{
public:
	PgWork();
	PgWork(pqxx::transaction_base *workIn);
	PgWork(const PgWork &obj);
	virtual ~PgWork();
	PgWork& operator=(const PgWork &obj);

	std::shared_ptr<pqxx::transaction_base> work;
};

class PgCommon
{
protected:
	std::shared_ptr<pqxx::connection> dbconn;
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::string shareMode;
	std::shared_ptr<class PgWork> sharedWork;
	class DbUsernameLookup dbUsernameLookup;

public:
	PgCommon(std::shared_ptr<pqxx::connection> dbconnIn,
		const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		std::shared_ptr<class PgWork> sharedWorkIn,
		const std::string &shareMode);
	virtual ~PgCommon();

	virtual bool IsAdminMode();

	void GetWaysForNodes(const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);	
	void GetRelationsForObjs(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);
	void GetAffectedParents(std::shared_ptr<class OsmData> inputObjects,
		std::shared_ptr<class OsmData> outputObjects);	
	void GetAffectedParents(const class OsmData &inputObjects,
		std::shared_ptr<class OsmData> outputObjects);
	//void GetAffectedChildren(std::shared_ptr<class OsmData> inputObjects,
	//	std::shared_ptr<class OsmData> outputObjects);

	void GetObjectBboxes(const std::string &type, const std::set<int64_t> &objectIds);

};

#endif //_PG_COMMON_H

