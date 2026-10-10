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
	// Zero/-1 denote legacy activity with unknown transaction boundaries.
	int64_t atomicEditId;
	int64_t blockIndex;

	int nodes;
	int ways;
	int relations;
	std::string action;
	std::vector<double> bbox;
	// JSON reference array and matching SRID-4326 GeometryCollection WKT.
	// Empty strings mean context was not recorded.
	std::string syncBefore;
	std::string bboxBefore;
	std::string syncAfter;
	std::string bboxAfter;
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
	// The relations that have one of the related nodes as a member, other than
	// those already among the edited objects and affected parents. Something
	// following these records needs them when a related node is new to it: the
	// edit did not touch these relations, so nothing else here mentions them.
	// relatedparentsRecorded is false for a row made before this was recorded,
	// which is not the same as there being none.
	std::vector<std::string> relatedparentsType;
	std::vector<std::pair<int64_t, int64_t> > relatedparentsIdVer;
	bool relatedparentsRecorded;

	EditActivity();
	virtual ~EditActivity();

	EditActivity( const EditActivity &obj);
	EditActivity& operator=(const EditActivity &arg);
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

void DbQueryEditActivityByTimestamp(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	int64_t sinceTimestamp,
	int64_t untilTimestamp,
	std::vector<std::shared_ptr<class EditActivity> > &out,
	std::string &errStr,
	int64_t limit = 0); //The rows come in ID order; limit, if positive, is the most to return

// Latest edit activity row ID and atomic edit ID; zero when there is no activity.
void DbGetLatestEditIds(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, int64_t &editActivityId, int64_t &atomicEditId);

void DbQueryEditActivityByIds(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, int64_t firstId, int64_t lastId,
	int64_t atomicEditId, std::vector<std::shared_ptr<class EditActivity> > &out,
	std::string &errStr,
	int64_t limit = 0);

#endif //_EDIT_ACTIVITY_H
