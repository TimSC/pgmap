#include <algorithm>
#include "dbeditactivity.h"
#include "dbdecode.h"
#include "dbjson.h"
#include "util.h"
#include "dbprepared.h"
#include "dbquery.h"
#include "pgcommon.h"
using namespace std;

#define pqxxrow pqxx::row

// The geometry columns holding the boxes that go with the lists of objects.
// They were added to schema 14 after it was first in use; a map that is at
// version 14 without them has to be asked for the latest schema again.
// The first four are the collections; the last is a single polygon.
static const size_t ACTIVITY_BBOX_COUNT = 5, ACTIVITY_COLLECTION_COUNT = 4;
static const char *ACTIVITY_BBOX_COLUMNS[ACTIVITY_BBOX_COUNT] = {"existing_bbox", "updated_bbox",
	"affectedparents_before_bbox", "affectedparents_after_bbox", "relation_reach_bbox"};

static std::string *ActivityBboxField(class EditActivity &activity, size_t index)
{
	std::string *fields[ACTIVITY_BBOX_COUNT] = {&activity.existingBbox, &activity.updatedBbox,
		&activity.affectedparentsBeforeBbox, &activity.affectedparentsAfterBbox, &activity.relationReachBbox};
	return fields[index];
}

class EditActivityCols
{
public:

	int idCol;
	int atomicEditIdCol, blockIndexCol;
	int nodesCol;
	int waysCol;
	int relationsCol;
	int actionCol;
	int changesetCol;
	int timestampCol;
	int uidCol;

	int existingCol, updatedCol, affectedParentsCol, relatedCol;
	int relatedParentsCol;
	int bboxCols[ACTIVITY_BBOX_COUNT]; //In the order of ACTIVITY_BBOX_COLUMNS

	EditActivityCols(pqxx::result &r);
};

EditActivityCols::EditActivityCols(pqxx::result &r)
{
	idCol = r.column_number("id");
	atomicEditIdCol = r.column_number("atomic_edit_id");
	blockIndexCol = r.column_number("block_index");
	nodesCol = r.column_number("nodes");
	waysCol = r.column_number("ways");
	relationsCol = r.column_number("relations");
	actionCol = r.column_number("action");
	changesetCol = r.column_number("changeset");
	timestampCol = r.column_number("timestamp");
	uidCol = r.column_number("uid");

	existingCol = r.column_number("existing");
	updatedCol = r.column_number("updated");
	affectedParentsCol = r.column_number("affectedparents");
	relatedCol = r.column_number("related");
	relatedParentsCol = r.column_number("related_parents");
	for(size_t i = 0; i < ACTIVITY_BBOX_COUNT; i++)
		bboxCols[i] = r.column_number(string(ACTIVITY_BBOX_COLUMNS[i]) + "_wkt");
}

//*******************************************************

EditActivity::EditActivity()
{
	objId = 0;
	atomicEditId = 0;
	blockIndex = -1;
	nodes = 0;
	ways = 0;
	relations = 0;
	changeset = 0;
	timestamp = 0;
	uid = 0;
	relatedparentsRecorded = false;
}

EditActivity::~EditActivity()
{

}

EditActivity::EditActivity( const EditActivity &obj)
{
	operator=(obj);
}

EditActivity& EditActivity::operator=(const EditActivity &arg)
{
	objId = arg.objId;
	atomicEditId = arg.atomicEditId;
	blockIndex = arg.blockIndex;

	nodes = arg.nodes;
	ways = arg.ways;
	relations = arg.relations;
	action = arg.action;
	changeset = arg.changeset;
	timestamp = arg.timestamp;
	uid = arg.uid;

	existingType = arg.existingType;
	existingIdVer = arg.existingIdVer;
	updatedType = arg.updatedType;
	updatedIdVer = arg.updatedIdVer;
	affectedparentsType = arg.affectedparentsType;
	affectedparentsIdVer = arg.affectedparentsIdVer;
	relatedType = arg.relatedType;
	relatedIdVer = arg.relatedIdVer;
	existingBbox = arg.existingBbox;
	updatedBbox = arg.updatedBbox;
	affectedparentsBeforeBbox = arg.affectedparentsBeforeBbox;
	affectedparentsAfterBbox = arg.affectedparentsAfterBbox;
	relationReachBbox = arg.relationReachBbox;
	relatedparentsType = arg.relatedparentsType;
	relatedparentsIdVer = arg.relatedparentsIdVer;
	relatedparentsRecorded = arg.relatedparentsRecorded;

	return *this;
}

// ****************************************************************

void DecodeEditActivityRow(const class EditActivityCols &cols,
	const pqxxrow row,
	class EditActivity &out)
{
	out.objId = row[cols.idCol].as<int64_t>();
	out.atomicEditId = row[cols.atomicEditIdCol].is_null() ? 0 : row[cols.atomicEditIdCol].as<int64_t>();
	out.blockIndex = row[cols.blockIndexCol].is_null() ? -1 : row[cols.blockIndexCol].as<int64_t>();

	out.nodes = row[cols.nodesCol].as<int64_t>();
	out.ways = row[cols.waysCol].as<int64_t>();
	out.relations = row[cols.relationsCol].as<int64_t>();
	out.action = row[cols.actionCol].as<string>();
	out.changeset = row[cols.changesetCol].as<int64_t>();
	out.timestamp = row[cols.timestampCol].as<int64_t>();
	out.uid = row[cols.uidCol].as<int64_t>();

	string existingJson = row[cols.existingCol].as<string>();
	string updatedJson = row[cols.updatedCol].as<string>();
	string affectedParentsJson = row[cols.affectedParentsCol].as<string>();
	string relatedJson = row[cols.relatedCol].as<string>();

	DecodeObjTypeIdVers(existingJson,
		out.existingType,
		out.existingIdVer);
	DecodeObjTypeIdVers(updatedJson,
		out.updatedType,
		out.updatedIdVer);
	DecodeObjTypeIdVers(affectedParentsJson,
		out.affectedparentsType,
		out.affectedparentsIdVer);
	DecodeObjTypeIdVers(relatedJson,
		out.relatedType,
		out.relatedIdVer);

	for(size_t i = 0; i < ACTIVITY_BBOX_COUNT; i++)
	{
		std::string *field = ActivityBboxField(out, i);
		field->clear();
		if(!row[cols.bboxCols[i]].is_null())
			*field = row[cols.bboxCols[i]].as<string>();
	}

	out.relatedparentsType.clear();
	out.relatedparentsIdVer.clear();
	out.relatedparentsRecorded = !row[cols.relatedParentsCol].is_null();
	if(out.relatedparentsRecorded)
	{
		string relatedParentsJson = row[cols.relatedParentsCol].as<string>();
		DecodeObjTypeIdVers(relatedParentsJson,
			out.relatedparentsType,
			out.relatedparentsIdVer);
	}
}

// What a query of the table selects: its columns, with the boxes as text
static std::string ActivitySelect(const std::string &table)
{
	string out = "SELECT " + table + ".*";
	for(const char *column : ACTIVITY_BBOX_COLUMNS)
		out += ", ST_AsText(" + table + "." + column + ") AS " + column + "_wkt";
	return out + " FROM " + table;
}

// An error from a table that lacks the columns schema 14 gained after it was
// first in use says what to do about it.
static std::string ActivityError(const std::exception &e)
{
	string message = e.what();
	if(message.find("does not exist") != string::npos)
		message += " (the edit activity table may predate the current schema: "
			"ask the admin tool for the latest schema version again)";
	return message;
}

bool DbGetEditActivityById(pqxx::connection &c,
	pqxx::transaction_base *work,
	const std::string &tablePrefix,
	int64_t editActivityId,
	class EditActivity &out,
	std::string &errStr)
{
	string table = c.quote_name(tablePrefix + "edit_activity");

	stringstream sql;
	sql << ActivitySelect(table) << " WHERE id="<<editActivityId<<";" ;

	try
	{
		pqxx::result r = work->exec(sql.str());
		class EditActivityCols cols(r);

		for (int rownum=0; rownum < r.size(); ++rownum)
		{
			const pqxxrow row = r[rownum];
			DecodeEditActivityRow(cols, row, out);

			return true; //Only expecting one row
		}
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = ActivityError(e);
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = ActivityError(e);
		return false;
	}

	return false;
}

// The condition keeping the rows that may concern an area. A row does if the
// objects it had before the edit, those it has after, or the affected parents
// on either side of it reach into the area. Old and new both count: an object
// can leave an area as well as enter it, and a parent's extent changes with
// its members while its version does not. relation_reach_bbox adds where an
// edited relation matters beyond its own box. A row where one of these was not
// recorded could concern anywhere, and is kept. && compares the area with the
// extent of a whole collection, so a row may be kept without need, never lost.
static std::string ActivityBboxCondition(const std::string &table, const std::vector<double> &bbox)
{
	if(bbox.empty()) return "";
	if(bbox.size() != 4)
		throw invalid_argument("bbox must have four values");
	stringstream area;
	area.precision(17);
	area << "ST_MakeEnvelope(" << bbox[0] << "," << bbox[1] << "," << bbox[2] << "," << bbox[3] << ",4326)";
	stringstream sql;
	sql << " AND (";
	for(size_t i=0; i<ACTIVITY_COLLECTION_COUNT; i++)
	{
		string column = table + "." + ACTIVITY_BBOX_COLUMNS[i];
		sql << (i ? " OR " : "") << column << " IS NULL OR " << column << " && " << area.str();
	}
	// Where a relation of the edit matters through ways sharing its member
	// nodes. Unset means nowhere more, not unknown.
	sql << " OR " << table << ".relation_reach_bbox && " << area.str() << ")";
	return sql.str();
}

// How the rows are ordered and limited. The newest are chosen from the top and
// then put back into ID order by the caller.
static std::string ActivityOrder(int64_t limit, bool newest)
{
	stringstream sql;
	sql << " ORDER BY id" << ((newest && limit > 0) ? " DESC" : "");
	if(limit > 0) sql << " LIMIT " << limit;
	return sql.str();
}

void DbQueryEditActivityByTimestamp(pqxx::connection &c,
	pqxx::transaction_base *work,
	const std::string &tablePrefix,
	int64_t sinceTimestamp,
	int64_t untilTimestamp,
	std::vector<std::shared_ptr<class EditActivity> > &out,
	std::string &errStr,
	int64_t limit,
	const std::vector<double> &bbox,
	bool newest)
{
	string table = c.quote_name(tablePrefix + "edit_activity");

	stringstream sql;
	sql << ActivitySelect(table) << " WHERE timestamp>="<<sinceTimestamp;
	if (untilTimestamp > 0)
		sql << " AND timestamp<"<<untilTimestamp;
	sql << ActivityBboxCondition(table, bbox) << ActivityOrder(limit, newest) << ";";

	try
	{
		pqxx::result r = work->exec(sql.str());
		class EditActivityCols cols(r);
		out.resize(r.size());

		for (int rownum=0; rownum < r.size(); ++rownum)
		{
			const pqxxrow row = r[rownum];
			auto activity = make_shared<class EditActivity>();
			DecodeEditActivityRow(cols, row, *activity);
			out[rownum] = activity;
		}
		if(newest && limit > 0)
			std::reverse(out.begin(), out.end());
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = ActivityError(e);
	}
	catch (const std::exception &e)
	{
		errStr = ActivityError(e);
	}
}

void DbQueryEditActivityByIds(pqxx::connection &c,
	pqxx::transaction_base *work,
	const std::string &tablePrefix,
	int64_t firstId,
	int64_t lastId,
	int64_t atomicEditId,
	std::vector<std::shared_ptr<class EditActivity> > &out,
	std::string &errStr,
	int64_t limit,
	const std::vector<double> &bbox,
	bool newest)
{
	string table = c.quote_name(tablePrefix + "edit_activity");

	stringstream sql;
	sql << ActivitySelect(table) << " WHERE id >= " << firstId;
	if(lastId > 0) sql << " AND id <= " << lastId;
	if(atomicEditId > 0) sql << " AND atomic_edit_id = " << atomicEditId;
	sql << ActivityBboxCondition(table, bbox) << ActivityOrder(limit, newest) << ";";

	try
	{
		pqxx::result r = work->exec(sql.str());
		class EditActivityCols cols(r);
		out.resize(r.size());

		for (int rownum=0; rownum < r.size(); ++rownum)
		{
			const pqxxrow row = r[rownum];
			auto activity = make_shared<class EditActivity>();
			DecodeEditActivityRow(cols, row, *activity);
			out[rownum] = activity;
		}
		if(newest && limit > 0)
			std::reverse(out.begin(), out.end());
	}
	catch (const pqxx::sql_error &e)
	{
		errStr = ActivityError(e);
	}
	catch (const std::exception &e)
	{
		errStr = ActivityError(e);
	}
}

bool DbInsertEditActivity(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix,
	const class EditActivity &activity,
	std::string &errStr,
	int verbose)
{
	string existingEnc, updatedEnc, affectedParentsEnc, relatedEnc;
	EncodeObjTypeIdVers(activity.existingType, activity.existingIdVer, existingEnc);
	EncodeObjTypeIdVers(activity.updatedType, activity.updatedIdVer, updatedEnc);
	EncodeObjTypeIdVers(activity.affectedparentsType, activity.affectedparentsIdVer, affectedParentsEnc);
	EncodeObjTypeIdVers(activity.relatedType, activity.relatedIdVer, relatedEnc);

	// The relations holding the related nodes are NULL when not recorded,
	// and so is a box that is not known: both arrive as empty strings
	string relatedParentsEnc;
	if(activity.relatedparentsRecorded)
		EncodeObjTypeIdVers(activity.relatedparentsType, activity.relatedparentsIdVer, relatedParentsEnc);

	stringstream sql;
	sql << "INSERT INTO "<< c.quote_name(tablePrefix+"edit_activity") << " (changeset, timestamp, uid, action, nodes, ways, relations, "
		"existing, updated, affectedparents, related, atomic_edit_id, block_index, related_parents";
	for(const char *column : ACTIVITY_BBOX_COLUMNS)
		sql << ", " << column;
	sql << ") VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NULLIF($14,'')::jsonb";
	for(size_t i = 0; i < ACTIVITY_BBOX_COUNT; i++)
		sql << ",ST_GeomFromText(NULLIF($" << (15 + i) << ",''),4326)";
	sql << ");";

	try
	{
		if(verbose >= 1)
			cout << sql.str() << endl;

		prepare_deduplicated(c, tablePrefix+"insert_edit_activity3", sql.str());
		work->exec_prepared(tablePrefix+"insert_edit_activity3", activity.changeset, activity.timestamp,
			activity.uid, activity.action, activity.nodes, activity.ways, activity.relations, existingEnc, updatedEnc,
			affectedParentsEnc, relatedEnc, activity.atomicEditId, activity.blockIndex, relatedParentsEnc,
			activity.existingBbox, activity.updatedBbox, activity.affectedparentsBeforeBbox,
			activity.affectedparentsAfterBbox, activity.relationReachBbox);
	}
	catch (const std::exception &e)
	{
		errStr = ActivityError(e);
		return false;
	}
	return true;
}

void DbGetMostActiveUsers(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix,
	int64_t startTimestamp,
	std::vector<int64_t> &uidOut,
	std::vector<std::vector<int64_t> > &objectCountOut)
{
	stringstream sql;
	sql << "SELECT uid, SUM(nodes) as nodes, SUM(ways) as ways, SUM(relations) as relations, SUM(nodes)+SUM(ways)+SUM(relations) as objects";
	sql << " FROM " << c.quote_name(tablePrefix+"edit_activity") << " WHERE timestamp>=$1 GROUP BY uid ORDER BY objects DESC LIMIT 10;";

	prepare_deduplicated(c, tablePrefix+"get_most_active_users", sql.str());
	pqxx::result r = work->exec_prepared(tablePrefix+"get_most_active_users", startTimestamp);

	int uidCol = r.column_number("uid");
	int nodesCol = r.column_number("nodes");
	int waysCol = r.column_number("ways");
	int relationsCol = r.column_number("relations");

	for (pqxx::result::const_iterator c = r.begin(); c != r.end(); ++c)
	{
		int64_t uid = c[uidCol].as<int64_t>();
		int64_t nodes = c[nodesCol].as<int64_t>();
		int64_t ways = c[waysCol].as<int64_t>();
		int64_t relations = c[relationsCol].as<int64_t>();

		uidOut.push_back(uid);
		std::vector<int64_t> oc = {nodes, ways, relations};
		objectCountOut.push_back(oc);
	}
}



void DbGetLatestEditIds(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix, int64_t &editActivityId, int64_t &atomicEditId)
{
	auto latest = work->exec("SELECT COALESCE(max(id),0), COALESCE(max(atomic_edit_id),0) FROM " +
		c.quote_name(tablePrefix + "edit_activity"));
	editActivityId = latest[0][0].as<int64_t>();
	atomicEditId = latest[0][1].as<int64_t>();
}

// ************* Recording activity outside an upload *************

static const char *ACTIVITY_TYPE_NAMES[3] = {"node", "way", "relation"};

EditActivityRecorder::EditActivityRecorder(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tableActivePrefix, class PgCommon *common):
	c(c), work(work), tableActivePrefix(tableActivePrefix), common(common),
	usernames(c, work, "", ""), //Only IDs, versions and positions are wanted
	atomicEditId(0), blockIndex(0),
	existing(new class OsmData()), parents(new class OsmData())
{
	// Activity is grouped into atomic edits from schema 14, which this sequence came with
	string sequence = c.quote_name(tableActivePrefix + "atomic_edit_id_seq");
	enabled = !work->exec("SELECT to_regclass(" + work->quote(sequence) + ")")[0][0].is_null();
}

void EditActivityRecorder::Visible(const std::string &type, const std::set<int64_t> &ids,
	std::shared_ptr<class OsmData> out)
{
	std::set<int64_t>::const_iterator it = ids.begin();
	while(it != ids.end())
		GetVisibleObjectsById(c, work, usernames, tableActivePrefix, type, ids, it, 1000, out);
}

// The box of each way and relation as the map has them now: the stored one,
// or for a way without one, a box worked out from its nodes.
void EditActivityRecorder::Boxes(const class OsmData &objs, BoxMap &out)
{
	std::set<int64_t> wayIds, relationIds;
	for(size_t i=0; i<objs.ways.size(); i++) wayIds.insert(objs.ways[i].objId);
	for(size_t i=0; i<objs.relations.size(); i++) relationIds.insert(objs.relations[i].objId);

	for(int type=1; type<=2; type++)
	{
		const std::set<int64_t> &ids = type == 1 ? wayIds : relationIds;
		if(ids.empty()) continue;
		std::map<int64_t, std::vector<double> > stored;
		common->GetObjectBboxes(ACTIVITY_TYPE_NAMES[type], ids, stored);
		for(auto it=stored.begin(); it!=stored.end(); it++)
			if(it->second.size() == 4)
				out[ObjKey(type, it->first)] = it->second;
	}

	std::set<int64_t> unboxed;
	for(int64_t wayId : wayIds)
		if(out.find(ObjKey(1, wayId)) == out.end())
			unboxed.insert(wayId);
	if(unboxed.empty()) return;

	// The ways are read again, so the nodes are those they have at this moment
	std::shared_ptr<class OsmData> found(new class OsmData());
	Visible("way", unboxed, found);
	std::set<int64_t> nodeIds;
	for(size_t i=0; i<found->ways.size(); i++)
		nodeIds.insert(found->ways[i].refs.begin(), found->ways[i].refs.end());
	Visible("node", nodeIds, found);
	std::map<int64_t, std::pair<double, double> > positions;
	for(size_t i=0; i<found->nodes.size(); i++)
		positions[found->nodes[i].objId] = std::make_pair(found->nodes[i].lon, found->nodes[i].lat);
	for(size_t i=0; i<found->ways.size(); i++)
	{
		std::vector<double> box;
		for(int64_t ref : found->ways[i].refs)
		{
			auto it = positions.find(ref);
			if(it == positions.end()) continue;
			if(box.empty())
				box = {it->second.first, it->second.second, it->second.first, it->second.second};
			else
			{
				box[0] = std::min(box[0], it->second.first);
				box[1] = std::min(box[1], it->second.second);
				box[2] = std::max(box[2], it->second.first);
				box[3] = std::max(box[3], it->second.second);
			}
		}
		if(!box.empty())
			out[ObjKey(1, found->ways[i].objId)] = box;
	}
}

// Adds the boxes of the ways that hold a node member of any of the relations:
// where an edit to a relation matters beyond the relation's own box. The
// header says why this is needed, with an example.
void EditActivityRecorder::ReachBoxes(const class OsmData &objs)
{
	std::set<int64_t> nodeIds;
	for(size_t i=0; i<objs.relations.size(); i++)
		for(const RelationMember &member : objs.relations[i].members)
			if(member.type == ObjectType::Node && member.ref > 0)
				nodeIds.insert(member.ref);
	if(nodeIds.empty()) return;
	std::shared_ptr<class OsmData> ways(new class OsmData());
	common->GetWaysForNodes(nodeIds, ways);
	BoxMap boxes;
	Boxes(*ways, boxes);
	for(auto it=boxes.begin(); it!=boxes.end(); it++)
		reachBoxes.push_back(it->second);
}

static void AppendNodePositions(const std::vector<OsmNode> &nodes,
	std::map<std::pair<int, int64_t>, std::vector<double> > &out)
{
	for(const OsmNode &node : nodes)
		out[std::make_pair(0, node.objId)] = {node.lon, node.lat, node.lon, node.lat};
}

static std::string BoxPolygonWkt(const std::vector<double> &box)
{
	stringstream wkt;
	wkt.precision(17);
	wkt << "POLYGON((" << box[0] << " " << box[1] << "," << box[0] << " " << box[3] << ","
		<< box[2] << " " << box[3] << "," << box[2] << " " << box[1] << "," << box[0] << " " << box[1] << "))";
	return wkt.str();
}

// One geometry for each object of a list, in order: a point for a node, the
// rectangle of its box for a way or relation. An object without one has an
// empty polygon where that is expected, as of deleted objects; otherwise the
// box is not known and nothing is returned, which leaves the column unset.
static std::string BoxCollectionWkt(const std::vector<std::string> &types,
	const std::vector<std::pair<int64_t, int64_t> > &idVers,
	const std::map<std::pair<int, int64_t>, std::vector<double> > &boxes, bool noneExpected)
{
	if(types.empty()) return "GEOMETRYCOLLECTION EMPTY";
	stringstream wkt;
	wkt.precision(17);
	wkt << "GEOMETRYCOLLECTION(";
	for(size_t i=0; i<types.size(); i++)
	{
		int type = types[i] == "node" ? 0 : (types[i] == "way" ? 1 : 2);
		auto it = boxes.find(std::make_pair(type, idVers[i].first));
		if(i > 0) wkt << ",";
		if(it == boxes.end())
		{
			if(!noneExpected) return "";
			wkt << "POLYGON EMPTY";
		}
		else if(type == 0)
			wkt << "POINT(" << it->second[0] << " " << it->second[1] << ")";
		else
			wkt << BoxPolygonWkt(it->second);
	}
	wkt << ")";
	return wkt.str();
}

static void ListTypeIdVers(const class OsmData &objs, std::vector<std::string> &types,
	std::vector<std::pair<int64_t, int64_t> > &idVers)
{
	for(const OsmNode &obj : objs.nodes)
	{
		types.push_back("node");
		idVers.push_back(std::make_pair(obj.objId, obj.metaData.version));
	}
	for(const OsmWay &obj : objs.ways)
	{
		types.push_back("way");
		idVers.push_back(std::make_pair(obj.objId, obj.metaData.version));
	}
	for(const OsmRelation &obj : objs.relations)
	{
		types.push_back("relation");
		idVers.push_back(std::make_pair(obj.objId, obj.metaData.version));
	}
}

void EditActivityRecorder::BeforeStore(const std::string &action, const class OsmData &block)
{
	existing.reset(new class OsmData());
	parents.reset(new class OsmData());
	existingBoxes.clear();
	parentBoxesBefore.clear();
	reachBoxes.clear();
	if(!enabled) return;

	// existing: the block's objects as the map has them before the edit.
	// Nothing was there before a creation.
	if(action != "create")
	{
		std::set<int64_t> ids;
		for(const OsmNode &obj : block.nodes) ids.insert(obj.objId);
		Visible("node", ids, existing);
		ids.clear();
		for(const OsmWay &obj : block.ways) ids.insert(obj.objId);
		Visible("way", ids, existing);
		ids.clear();
		for(const OsmRelation &obj : block.relations) ids.insert(obj.objId);
		Visible("relation", ids, existing);

		Boxes(*existing, existingBoxes);
		AppendNodePositions(existing->nodes, existingBoxes);
		ReachBoxes(*existing);

		// affectedparents: what holds the block's objects without being in
		// it, with the boxes they have before anything moves
		common->GetAffectedParents2(block, parents);
		Boxes(*parents, parentBoxesBefore);
	}
}

void EditActivityRecorder::Build(const std::string &action, const class OsmData &block, class EditActivity &activity)
{
	activity = EditActivity();
	if(block.nodes.empty() && block.ways.empty() && block.relations.empty()) return;
	activity.action = action;
	activity.nodes = block.nodes.size();
	activity.ways = block.ways.size();
	activity.relations = block.relations.size();

	// A diff's objects each say who changed them and in which changeset. A
	// record has one of each: those of the block's first object, and the
	// latest of the times.
	const MetaData *first = !block.nodes.empty() ? &block.nodes[0].metaData :
		(!block.ways.empty() ? &block.ways[0].metaData : &block.relations[0].metaData);
	activity.changeset = first->changeset;
	activity.uid = first->uid;
	activity.timestamp = 0;
	for(const OsmNode &obj : block.nodes) activity.timestamp = std::max(activity.timestamp, (int64_t)obj.metaData.timestamp);
	for(const OsmWay &obj : block.ways) activity.timestamp = std::max(activity.timestamp, (int64_t)obj.metaData.timestamp);
	for(const OsmRelation &obj : block.relations) activity.timestamp = std::max(activity.timestamp, (int64_t)obj.metaData.timestamp);

	ListTypeIdVers(*existing, activity.existingType, activity.existingIdVer);
	ListTypeIdVers(block, activity.updatedType, activity.updatedIdVer);
	ListTypeIdVers(*parents, activity.affectedparentsType, activity.affectedparentsIdVer);

	// related: unedited objects below the edited ones. For a creation, every
	// object the block refers to; for a modification, the nodes of its ways
	// and of the affected parent ways; for a deletion, none. Anything in the
	// block or among the affected parents is left out.
	std::set<int64_t> known[3], wanted[3];
	for(const OsmNode &obj : block.nodes) known[0].insert(obj.objId);
	for(const OsmWay &obj : block.ways) known[1].insert(obj.objId);
	for(const OsmRelation &obj : block.relations) known[2].insert(obj.objId);
	for(const OsmWay &obj : parents->ways) known[1].insert(obj.objId);
	for(const OsmRelation &obj : parents->relations) known[2].insert(obj.objId);
	if(action == "create" || action == "modify")
		for(const OsmWay &obj : block.ways)
			wanted[0].insert(obj.refs.begin(), obj.refs.end());
	if(action == "create")
		for(const OsmRelation &obj : block.relations)
			for(const RelationMember &member : obj.members)
				wanted[(int)member.type].insert(member.ref);
	if(action == "modify")
		for(const OsmWay &obj : parents->ways)
			wanted[0].insert(obj.refs.begin(), obj.refs.end());
	std::shared_ptr<class OsmData> related(new class OsmData());
	std::set<int64_t> relatedNodeIds;
	for(int type=0; type<3; type++)
	{
		std::set<int64_t> ids;
		for(int64_t objId : wanted[type])
			if(known[type].find(objId) == known[type].end())
				ids.insert(objId);
		Visible(ACTIVITY_TYPE_NAMES[type], ids, related);
		if(type == 0) relatedNodeIds = ids;
	}
	ListTypeIdVers(*related, activity.relatedType, activity.relatedIdVer);

	// relatedparents: the relations holding a related node, other than those
	// already in the block or among the affected parents
	activity.relatedparentsRecorded = true;
	if(!relatedNodeIds.empty())
	{
		std::shared_ptr<class OsmData> found(new class OsmData());
		common->GetRelationsForObjs("node", relatedNodeIds, found);
		std::set<int64_t> listed;
		for(const OsmRelation &obj : found->relations)
		{
			if(known[2].find(obj.objId) != known[2].end() || !listed.insert(obj.objId).second)
				continue;
			activity.relatedparentsType.push_back("relation");
			activity.relatedparentsIdVer.push_back(std::make_pair(obj.objId, obj.metaData.version));
		}
	}

	// Where the objects of each list are. A deleted object is nowhere afterwards.
	BoxMap updatedBoxes, parentBoxesAfter;
	if(action != "delete")
	{
		Boxes(block, updatedBoxes);
		AppendNodePositions(block.nodes, updatedBoxes);
		ReachBoxes(block);
	}
	Boxes(*parents, parentBoxesAfter);
	activity.existingBbox = BoxCollectionWkt(activity.existingType, activity.existingIdVer, existingBoxes, false);
	activity.updatedBbox = BoxCollectionWkt(activity.updatedType, activity.updatedIdVer, updatedBoxes, action == "delete");
	activity.affectedparentsBeforeBbox = BoxCollectionWkt(activity.affectedparentsType,
		activity.affectedparentsIdVer, parentBoxesBefore, false);
	activity.affectedparentsAfterBbox = BoxCollectionWkt(activity.affectedparentsType,
		activity.affectedparentsIdVer, parentBoxesAfter, false);
	if(!reachBoxes.empty())
	{
		std::vector<double> reach = reachBoxes[0];
		for(const std::vector<double> &box : reachBoxes)
		{
			reach[0] = std::min(reach[0], box[0]);
			reach[1] = std::min(reach[1], box[1]);
			reach[2] = std::max(reach[2], box[2]);
			reach[3] = std::max(reach[3], box[3]);
		}
		activity.relationReachBbox = BoxPolygonWkt(reach);
	}

}

bool EditActivityRecorder::AfterStore(const std::string &action, const class OsmData &block, std::string &errStr)
{
	if(!enabled) return true;
	if(block.nodes.empty() && block.ways.empty() && block.relations.empty()) return true;
	class EditActivity activity;
	Build(action, block, activity);

	// Allocated once the map is locked, as an upload does; see PgTransaction::InsertEditActivity
	if(atomicEditId == 0)
	{
		string sequence = c.quote_name(tableActivePrefix + "atomic_edit_id_seq");
		atomicEditId = work->exec("SELECT nextval(" + work->quote(sequence) + "::regclass)")[0][0].as<int64_t>();
	}
	activity.atomicEditId = atomicEditId;
	activity.blockIndex = blockIndex;
	bool ok = DbInsertEditActivity(c, work, tableActivePrefix, activity, errStr, 0);
	if(ok) blockIndex ++;
	return ok;
}
