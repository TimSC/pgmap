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
	// Bounding boxes that go with the lists of objects above. Each is the WKT
	// of an SRID-4326 GeometryCollection with one geometry for every object of
	// its list, in the same order. A way or relation whose box is known has a
	// polygon of it; anything else has POLYGON EMPTY, which keeps the
	// positions in step. Boxes are not kept for nodes, whose position is in
	// the object itself, and an object that has been deleted has none. An
	// empty string means the row was made before this was recorded; a list
	// with no objects has an empty collection.
	std::string existingBbox; // of the existing objects, before the edit
	std::string updatedBbox; // of the updated objects, after the edit
	std::string affectedparentsBeforeBbox; // of the affected parents, before the edit
	std::string affectedparentsAfterBbox; // and after it: a parent's box moves with its members
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
