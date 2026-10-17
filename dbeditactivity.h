#ifndef _EDIT_ACTIVITY_H
#define _EDIT_ACTIVITY_H

#include <pqxx/pqxx>
#include <string>
#include <vector>
#include <map>
#include <set>
#include "pgmap.h"
#include "dbusername.h"

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
	// Where the objects of the lists above are. Each is the WKT of an
	// SRID-4326 GeometryCollection with one geometry for every object of its
	// list, in the same order: a point for a node, a polygon of its box for a
	// way or relation, and POLYGON EMPTY, which keeps the positions in step,
	// for an object that has been deleted. An empty string means it was not
	// recorded, either because the row is older than this or because the box
	// of one of the objects was not known; a list with no objects has an
	// empty collection.
	std::string existingBbox; // of the existing objects, before the edit
	std::string updatedBbox; // of the updated objects, after the edit
	std::string affectedparentsBeforeBbox; // of the affected parents, before the edit
	std::string affectedparentsAfterBbox; // and after it: a parent's box moves with its members
	// Where the edit matters beyond those, as the WKT of a polygon, or empty
	// when there is nowhere more. It covers the ways that share a member node
	// with a relation being edited: an extract holding such a way holds the
	// relation too, wherever the relation itself is. Searches by area test
	// it along with the collections above.
	std::string relationReachBbox;
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
	int64_t limit = 0, //The rows come in ID order; limit, if positive, is the most to return
	//bbox, if it has four values (min lon, min lat, max lon, max lat), keeps
	//the rows where the existing objects, the updated objects or the affected
	//parents before or after the edit reach into it, those whose relation
	//reach does, and those where one of the four collections was not recorded,
	//which could concern anywhere. With
	//newest, a limit keeps the highest IDs, not the lowest; still in ID order.
	const std::vector<double> &bbox = std::vector<double>(),
	bool newest = false);

// Latest edit activity row ID and atomic edit ID; zero when there is no activity.
void DbGetLatestEditIds(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, int64_t &editActivityId, int64_t &atomicEditId);

// bbox, or else wkt (a polygon or multipolygon), keeps only the rows that may
// concern that area; see ActivityAreaCondition.
void DbQueryEditActivityByIds(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, int64_t firstId, int64_t lastId,
	int64_t atomicEditId, std::vector<std::shared_ptr<class EditActivity> > &out,
	std::string &errStr,
	int64_t limit = 0,
	const std::vector<double> &bbox = std::vector<double>(),
	bool newest = false,
	const std::string &wkt = "");

// Builds the edit activity record of a block of changes: one action (create,
// modify or delete) applied to a set of objects. It is used both by the web
// API's upload (through PgTransaction) and by the admin tool when it applies
// diffs, so that everything that changes the map is recorded by one set of
// rules. For each block, call BeforeStore while the map is as it was, store
// the block and bring the boxes of its ways and relations and of
// AffectedParents() up to date, then call Build. The map must stay
// exclusively locked from the first block until commit.
//
// Every list of a record holds (type, ID, version) references, nodes first,
// then ways, then relations. The rules for what goes in each list are:
//
// existing: the objects of the block as the map had them before the edit, so
//   with the version the edit replaces.
//   - create: empty.
//   - modify, delete: every object of the block that the map has.
//
// updated: the objects the block stored, with the version the edit gave them.
//   - create: the new objects, under the IDs they were given.
//   - modify: the new version of each object.
//   - delete: the deleted version of each object (not visible).
//
// affectedparents: ways and relations the block does not edit, but which hold
//   something it does. Their version is unchanged, though their extent can
//   change. Found by GetAffectedParents2 before the block is stored:
//   - create: empty. Nothing can hold an object that did not exist.
//   - modify, delete:
//     1. ways having a node of the block,
//     2. relations having as a member a node of the block, a way of the block
//        or a way from step 1,
//     3. relations holding a relation of the block or one already found,
//        repeated upwards for at most 10 levels.
//   - An object in the block is never listed, as it is in updated.
//
// related: unedited objects below the edited ones, which are needed to make
//   sense of them, at their current version.
//   - create: every existing object the block refers to: the nodes of its
//     ways and the node, way and relation members of its relations.
//   - modify: nodes only: those of the block's ways (as now stored) and those
//     of the affected parent ways. The members of a modified relation are not
//     included.
//   - delete: empty.
//   - Anything in the block or in affectedparents is left out.
//
// relatedparents: relations having a related node as a member, leaving out
//   any in the block or in affectedparents. Ways holding a related node, and
//   relations holding a related way or relation, are not followed.
//
// Four collections say where the objects of existing, updated and
// affectedparents (before and after) are: see the fields of EditActivity.
//
// relation_reach_bbox says where an edit to a relation matters beyond the
// relation's own box. None of the lists above holds a way that merely shares
// a member node with an edited relation, and such a way can be all that
// connects the edit to an area.
//
//   Why it is needed. Something keeping a copy of one area of the map (a
//   stored extract) asks only for the records that reach into its area. Its
//   copy follows the rule of a map query:
//     1. the nodes inside the area,
//     2. the ways having one of those nodes, with all of their nodes, inside
//        the area or not,
//     3. the relations having any of those nodes or ways as a member.
//   By rule 3 a relation can belong to the copy through a node that is only
//   there by rule 2, and then nothing about the relation is near the area.
//
//   An example:
//     - node N is inside the area and node F is far outside it,
//     - way W runs from N to F,
//     - relation R has F as its only member. W is not a member of R.
//   The copy has N, so W, so F, so R. Now R is modified or deleted. The
//   record says where R is (existing_bbox and updated_bbox: the point F) and
//   where what holds R is (the affected parents: nothing does). None of that
//   reaches the area, so the record would be left out and the copy would keep
//   the old R. What connects the edit to the area is W, which the edit does
//   not touch and which is in none of the record's lists: W is neither R, nor
//   above R, nor below it. It shares a member of R.
//
//   How it is populated. For every relation of the block, take its node
//   members that already exist. Find every way holding any of those nodes,
//   and take the box of each: the stored one, or one worked out from the
//   way's nodes. Way and relation members are not followed, as a member way's
//   extent is already part of the relation's own box, and a copy does not
//   follow relations that are members of relations. This is done twice:
//   before the block is stored, with the relations as the map had them (their
//   old members), and afterwards, unless they were deleted, with the
//   relations as stored (their new members). relation_reach_bbox is the one
//   rectangle around all the boxes from both. It is left unset when there are
//   none, as for any edit without relations, which means there is nowhere
//   more, not that it is unknown. Being a single rectangle it can take in
//   more than the ways themselves, so a record is sometimes sent without
//   need, never missed.
class EditActivityRecorder
{
public:
	EditActivityRecorder(pqxx::connection &c, pqxx::transaction_base *work,
		const std::string &tableActivePrefix, class PgCommon *common);

	// False on a map whose schema has no atomic edits, where nothing is recorded
	bool Enabled() const {return enabled;}

	void BeforeStore(const std::string &action, const class OsmData &block);
	// The ways and relations holding the block's objects, found by BeforeStore
	const class OsmData &AffectedParents() const {return *parents;}
	// Fills in the record of the block that has now been stored. Its
	// changeset and user are those of the block's first object and its time
	// the latest of the objects'. The atomic edit is left to whoever inserts it.
	void Build(const std::string &action, const class OsmData &block, class EditActivity &activity);
	// Build, then insert as the next block of this recorder's own atomic
	// edit: one recorder used this way is one atomic edit.
	bool AfterStore(const std::string &action, const class OsmData &block, std::string &errStr);

private:
	typedef std::pair<int, int64_t> ObjKey; // Type (0 node, 1 way, 2 relation) and ID
	typedef std::map<ObjKey, std::vector<double> > BoxMap;

	pqxx::connection &c;
	pqxx::transaction_base *work;
	std::string tableActivePrefix;
	class PgCommon *common;
	class DbUsernameLookup usernames;
	bool enabled;
	int64_t atomicEditId, blockIndex;

	std::shared_ptr<class OsmData> existing, parents;
	BoxMap existingBoxes, parentBoxesBefore;
	std::vector<std::vector<double> > reachBoxes;

	void Visible(const std::string &type, const std::set<int64_t> &ids, std::shared_ptr<class OsmData> out);
	void Boxes(const class OsmData &objs, BoxMap &out);
	void ReachBoxes(const class OsmData &objs);
};

#endif //_EDIT_ACTIVITY_H
