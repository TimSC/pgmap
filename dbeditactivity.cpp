#include <algorithm>
#include "dbeditactivity.h"
#include "dbdecode.h"
#include "dbjson.h"
#include "util.h"
#include "dbprepared.h"
using namespace std;

#if PQXX_VERSION_MAJOR >= 6
#define pqxxrow pqxx::row
#else
#define pqxxrow pqxx::result::tuple
#endif

// The geometry columns holding the boxes that go with the lists of objects,
// all added to schema 14 after it was first in use, so a map may lack them.
static const char *ACTIVITY_BBOX_COLUMNS[4] = {"existing_bbox", "updated_bbox",
	"affectedparents_before_bbox", "affectedparents_after_bbox"};

static std::string *ActivityBboxField(class EditActivity &activity, size_t index)
{
	std::string *fields[4] = {&activity.existingBbox, &activity.updatedBbox,
		&activity.affectedparentsBeforeBbox, &activity.affectedparentsAfterBbox};
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
	int bboxCol;
	int changesetCol;
	int timestampCol;
	int uidCol;

	int existingCol, updatedCol, affectedParentsCol, relatedCol;
	//-1 on a map whose table has not gained the column
	int relatedParentsCol;
	int bboxCols[4]; //In the order of ACTIVITY_BBOX_COLUMNS

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
	bboxCol = r.column_number("bbox");
	changesetCol = r.column_number("changeset");
	timestampCol = r.column_number("timestamp");
	uidCol = r.column_number("uid");

	existingCol = r.column_number("existing");
	updatedCol = r.column_number("updated");
	affectedParentsCol = r.column_number("affectedparents");
	relatedCol = r.column_number("related");
	try
	{
		relatedParentsCol = r.column_number("related_parents");
	}
	catch(const std::exception &)
	{
		relatedParentsCol = -1;
	}
	for(size_t i = 0; i < 4; i++)
	{
		try
		{
			bboxCols[i] = r.column_number(string(ACTIVITY_BBOX_COLUMNS[i]) + "_wkt");
		}
		catch(const std::exception &)
		{
			bboxCols[i] = -1;
		}
	}
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
	bbox = arg.bbox;
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

	for(size_t i = 0; i < 4; i++)
	{
		std::string *field = ActivityBboxField(out, i);
		field->clear();
		if(cols.bboxCols[i] >= 0 && !row[cols.bboxCols[i]].is_null())
			*field = row[cols.bboxCols[i]].as<string>();
	}

	out.relatedparentsType.clear();
	out.relatedparentsIdVer.clear();
	out.relatedparentsRecorded = cols.relatedParentsCol >= 0 && !row[cols.relatedParentsCol].is_null();
	if(out.relatedparentsRecorded)
	{
		string relatedParentsJson = row[cols.relatedParentsCol].as<string>();
		DecodeObjTypeIdVers(relatedParentsJson,
			out.relatedparentsType,
			out.relatedparentsIdVer);
	}
}

static bool ActivityTableHasColumn(pqxx::transaction_base *work, const std::string &tablePrefix, const char *column)
{
	return !work->exec("SELECT 1 FROM information_schema.columns WHERE table_schema = ANY(current_schemas(false))"
		" AND table_name = " + work->quote(tablePrefix + "edit_activity") +
		" AND column_name = " + work->quote(string(column))).empty();
}

///Select-list entries for columns added to schema 14 after it was first in
///use, which a map may not have gained yet. Each ends with a comma.
static std::string OptionalActivityColumns(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix)
{
	string table = c.quote_name(tablePrefix + "edit_activity"), out;
	for(const char *column : ACTIVITY_BBOX_COLUMNS)
		if(ActivityTableHasColumn(work, tablePrefix, column))
			out += "ST_AsText(" + table + "." + column + ") AS " + column + "_wkt, ";
	return out;
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
	sql << "SELECT "<<table<<".*, "<<OptionalActivityColumns(c, work, tablePrefix)<<"ST_XMin("<<table<<".bbox) as xmin, ST_XMax("<<table<<".bbox) as xmax,";
	sql << " ST_YMin("<<table<<".bbox) as ymin, ST_YMax("<<table<<".bbox) as ymax";
	sql << " FROM " << table << " WHERE id="<<editActivityId<<";" ;

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
		errStr = e.what();
		return false;
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
		return false;
	}

	return false;
}

// The condition keeping the rows that may concern an area. A row does if the
// objects it had before the edit, those it has after, or the affected parents
// on either side of it reach into the area. Old and new both count: an object
// can leave an area as well as enter it, and a parent's extent changes with
// its members while its version does not. The row's bbox column adds where an
// edited relation matters beyond its own box. A row where one of these was not
// recorded could concern anywhere, and is kept. && compares the area with the
// extent of a whole collection, so a row may be kept without need, never lost.
static std::string ActivityBboxCondition(pqxx::transaction_base *work, const std::string &tablePrefix,
	const std::string &table, const std::vector<double> &bbox)
{
	if(bbox.empty()) return "";
	if(bbox.size() != 4)
		throw invalid_argument("bbox must have four values");
	// A table that has not gained the columns cannot say, so nothing is left out
	for(const char *column : ACTIVITY_BBOX_COLUMNS)
		if(!ActivityTableHasColumn(work, tablePrefix, column)) return "";

	stringstream area;
	area.precision(17);
	area << "ST_MakeEnvelope(" << bbox[0] << "," << bbox[1] << "," << bbox[2] << "," << bbox[3] << ",4326)";
	stringstream sql;
	sql << " AND (";
	for(size_t i=0; i<4; i++)
	{
		string column = table + "." + ACTIVITY_BBOX_COLUMNS[i];
		sql << (i ? " OR " : "") << column << " IS NULL OR " << column << " && " << area.str();
	}
	// Where a relation of the edit matters through ways sharing its member nodes
	sql << " OR " << table << ".bbox && " << area.str() << ")";
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
	sql << "SELECT "<<table<<".*, "<<OptionalActivityColumns(c, work, tablePrefix)<<"ST_XMin("<<table<<".bbox) as xmin, ST_XMax("<<table<<".bbox) as xmax,";
	sql << " ST_YMin("<<table<<".bbox) as ymin, ST_YMax("<<table<<".bbox) as ymax";
	sql << " FROM " << table << " WHERE timestamp>="<<sinceTimestamp;
	if (untilTimestamp > 0)
		sql << " AND timestamp<"<<untilTimestamp;
	sql << ActivityBboxCondition(work, tablePrefix, table, bbox) << ActivityOrder(limit, newest) << ";";

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
		errStr = e.what();
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
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
	sql << "SELECT "<<table<<".*, "<<OptionalActivityColumns(c, work, tablePrefix)<<"ST_XMin("<<table<<".bbox) as xmin, ST_XMax("<<table<<".bbox) as xmax,";
	sql << " ST_YMin("<<table<<".bbox) as ymin, ST_YMax("<<table<<".bbox) as ymax";
	sql << " FROM " << table << " WHERE id >= " << firstId;
	if(lastId > 0) sql << " AND id <= " << lastId;
	if(atomicEditId > 0) sql << " AND atomic_edit_id = " << atomicEditId;
	sql << ActivityBboxCondition(work, tablePrefix, table, bbox) << ActivityOrder(limit, newest) << ";";

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
		errStr = e.what();
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
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

	stringstream sql;
	sql << "INSERT INTO "<< c.quote_name(tablePrefix+"edit_activity") << " (changeset, timestamp, uid, action, nodes, ways, relations, existing, updated, affectedparents, related, atomic_edit_id, block_index, bbox) VALUES ";
	sql << "($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,";
	if(activity.bbox.size() == 4)
		sql << "ST_MakeEnvelope($14,$15,$16,$17,4326)";
	else
		sql << "null";
	sql << ");";

	try
	{
		if(verbose >= 1)
			cout << sql.str() << endl;

		if(activity.bbox.size() == 4)
		{
			prepare_deduplicated(c, tablePrefix+"insert_edit_activity1", sql.str());
#if PQXX_VERSION_MAJOR >= 7
			work->exec_prepared(tablePrefix+"insert_edit_activity1", activity.changeset, activity.timestamp,
				activity.uid, activity.action, activity.nodes, activity.ways, activity.relations, existingEnc, updatedEnc,
				affectedParentsEnc, relatedEnc, activity.atomicEditId, activity.blockIndex, activity.bbox[0], activity.bbox[1], activity.bbox[2], activity.bbox[3]);
#else
			work->prepared(tablePrefix+"insert_edit_activity1")(activity.changeset)(activity.timestamp)
				(activity.uid)(activity.action)(activity.nodes)(activity.ways)(activity.relations)(existingEnc)(updatedEnc)
				(affectedParentsEnc)(relatedEnc)(activity.atomicEditId)(activity.blockIndex)(activity.bbox[0])(activity.bbox[1])(activity.bbox[2])(activity.bbox[3]).exec();
#endif
		}
		else
		{
			prepare_deduplicated(c, tablePrefix+"insert_edit_activity2", sql.str());
#if PQXX_VERSION_MAJOR >= 7
			work->exec_prepared(tablePrefix+"insert_edit_activity2", activity.changeset, activity.timestamp,
				activity.uid, activity.action, activity.nodes, activity.ways, activity.relations, existingEnc, updatedEnc,
				affectedParentsEnc, relatedEnc, activity.atomicEditId, activity.blockIndex);
#else
			work->prepared(tablePrefix+"insert_edit_activity2")(activity.changeset)(activity.timestamp)
				(activity.uid)(activity.action)(activity.nodes)(activity.ways)(activity.relations)(existingEnc)(updatedEnc)
				(affectedParentsEnc)(relatedEnc)(activity.atomicEditId)(activity.blockIndex).exec();
#endif
		}
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

	// What remains goes in columns added to schema 14 after it was first in use
	class EditActivity recorded(activity);
	bool anyBbox = false;
	for(size_t i = 0; i < 4; i++)
		anyBbox = anyBbox || !ActivityBboxField(recorded, i)->empty();
	if(!activity.relatedparentsRecorded && !anyBbox)
		return true;
	try
	{
		// A map that has not gained one of these columns yet goes on recording
		// edits without it.
		string tableName = tablePrefix + "edit_activity";
		string table = c.quote_name(tableName);
		auto hasColumn = [&](const char *column) { return ActivityTableHasColumn(work, tablePrefix, column); };
		std::vector<string> values;
		if(activity.relatedparentsRecorded && hasColumn("related_parents"))
		{
			string relatedParentsEnc;
			EncodeObjTypeIdVers(activity.relatedparentsType, activity.relatedparentsIdVer, relatedParentsEnc);
			values.push_back("related_parents = " + work->quote(relatedParentsEnc) + "::jsonb");
		}
		for(size_t i = 0; i < 4; i++)
		{
			const std::string &wkt = *ActivityBboxField(recorded, i);
			if(!wkt.empty() && hasColumn(ACTIVITY_BBOX_COLUMNS[i]))
				values.push_back(string(ACTIVITY_BBOX_COLUMNS[i]) + " = ST_GeomFromText(" + work->quote(wkt) + ", 4326)");
		}
		if(!values.empty())
		{
			string sql = "UPDATE " + table + " SET ";
			for(size_t i = 0; i < values.size(); i++)
				sql += (i ? ", " : "") + values[i];
			// The row just inserted, in this session: the map is locked for writing
			work->exec(sql + " WHERE id = currval(pg_get_serial_sequence(" + work->quote(table) + ", 'id'))");
		}
	}
	catch (const std::exception &e)
	{
		errStr = e.what();
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
#if PQXX_VERSION_MAJOR >= 7
	pqxx::result r = work->exec_prepared(tablePrefix+"get_most_active_users", startTimestamp);
#else
	pqxx::result r = work->prepared(tablePrefix+"get_most_active_users")(startTimestamp).exec();
#endif

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
