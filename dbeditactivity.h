#ifndef _EDIT_ACTIVITY_H
#define _EDIT_ACTIVITY_H

#include <pqxx/pqxx>
#include <string>
#include <vector>
#include "pgmap.h"

class EditActivity
{
public:
	int64_t objId;

	int nodes;
	int ways;
	int relations;
	std::string action;
	std::vector<double> bbox;
	int64_t changeset;
	int64_t timestamp;
	int64_t uid;

	std::vector<std::string> existingType;
	std::vector<std::pair<int64_t, int64_t> > existingIdVer;
	std::vector<std::string> updatedType;
	std::vector<std::pair<int64_t, int64_t> > updatedIdVer;
	std::vector<std::string> affectedparentsType;
	std::vector<std::pair<int64_t, int64_t> > affectedparentsIdVer;
	std::vector<std::string> relatedType;
	std::vector<std::pair<int64_t, int64_t> > relatedIdVer;

	EditActivity();
	virtual ~EditActivity();
};

bool DbInsertEditActivity(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &tablePrefix, 
	const class EditActivity &activity,
	std::string &errStr,
	int verbose);

void DbGetMostActiveUsers(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, 
	int64_t startTimestamp,
	std::vector<int64_t> &uidOut,
	std::vector<std::vector<int64_t> > &objectCountOut);

bool DbGetEditActivityById(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	int64_t editActivityId,
	class EditActivity &out,
	std::string &errStr);

#endif //_EDIT_ACTIVITY_H
