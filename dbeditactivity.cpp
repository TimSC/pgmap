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

class EditActivityCols
{
public:

	int idCol;
	int nodesCol;
	int waysCol;
	int relationsCol;
	int actionCol;
	int bboxCol;
	int changesetCol;
	int timestampCol;
	int uidCol;
	
	int existingCol, updatedCol, affectedParentsCol, relatedCol;

	EditActivityCols(pqxx::result &r);
};

EditActivityCols::EditActivityCols(pqxx::result &r)
{
	idCol = r.column_number("id");
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
}

//*******************************************************

EditActivity::EditActivity()
{
	objId = 0;
	nodes = 0;
	ways = 0;
	relations = 0;
	changeset = 0;
	timestamp = 0;
	uid = 0;
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

	return *this;
}

// ****************************************************************

void DecodeEditActivityRow(const class EditActivityCols &cols,
	const pqxxrow row,
	class EditActivity &out)
{
	out.objId = row[cols.idCol].as<int64_t>();

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
	sql << "SELECT "<<table<<".*, ST_XMin("<<table<<".bbox) as xmin, ST_XMax("<<table<<".bbox) as xmax,";
	sql << " ST_YMin("<<table<<".bbox) as ymin, ST_YMax("<<table<<".bbox) as ymax";
	sql << " FROM " << table << " WHERE id="<<editActivityId<<";" ;

	try
	{
		pqxx::result r = work->exec(sql.str());
		class EditActivityCols cols(r);

		for (unsigned int rownum=0; rownum < r.size(); ++rownum)
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

void DbQueryEditActivityByTimestamp(pqxx::connection &c, 
	pqxx::transaction_base *work, 
	const std::string &tablePrefix,
	int64_t sinceTimestamp,
	int64_t untilTimestamp,
	std::vector<std::shared_ptr<class EditActivity> > &out,
	std::string &errStr)
{
	string table = c.quote_name(tablePrefix + "edit_activity");

	stringstream sql;
	sql << "SELECT "<<table<<".*, ST_XMin("<<table<<".bbox) as xmin, ST_XMax("<<table<<".bbox) as xmax,";
	sql << " ST_YMin("<<table<<".bbox) as ymin, ST_YMax("<<table<<".bbox) as ymax";
	sql << " FROM " << table << " WHERE timestamp>="<<sinceTimestamp;
	if (untilTimestamp > 0)
		sql << " AND timestamp<"<<untilTimestamp<<endl;
	sql <<";" ;

	try
	{
		pqxx::result r = work->exec(sql.str());
		class EditActivityCols cols(r);
		out.resize(r.size());

		for (unsigned int rownum=0; rownum < r.size(); ++rownum)
		{
			const pqxxrow row = r[rownum];
			auto activity = make_shared<class EditActivity>();
			DecodeEditActivityRow(cols, row, *activity);
			out[rownum] = activity;
		}
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
	sql << "INSERT INTO "<< c.quote_name(tablePrefix+"edit_activity") << " (changeset, timestamp, uid, action, nodes, ways, relations, existing, updated, affectedparents, related, bbox) VALUES ";
	sql << "($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,";
	if(activity.bbox.size() == 4)
		sql << "ST_MakeEnvelope($12,$13,$14,$15,4326)";
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
				affectedParentsEnc, relatedEnc, activity.bbox[0], activity.bbox[1], activity.bbox[2], activity.bbox[3]);
#else
			work->prepared(tablePrefix+"insert_edit_activity1")(activity.changeset)(activity.timestamp)
				(activity.uid)(activity.action)(activity.nodes)(activity.ways)(activity.relations)(existingEnc)(updatedEnc)
				(affectedParentsEnc)(relatedEnc)(activity.bbox[0])(activity.bbox[1])(activity.bbox[2])(activity.bbox[3]).exec();
#endif
		}
		else
		{
			prepare_deduplicated(c, tablePrefix+"insert_edit_activity2", sql.str());
#if PQXX_VERSION_MAJOR >= 7
			work->exec_prepared(tablePrefix+"insert_edit_activity2", activity.changeset, activity.timestamp,
				activity.uid, activity.action, activity.nodes, activity.ways, activity.relations, existingEnc, updatedEnc,
				affectedParentsEnc, relatedEnc);
#else
			work->prepared(tablePrefix+"insert_edit_activity2")(activity.changeset)(activity.timestamp)
				(activity.uid)(activity.action)(activity.nodes)(activity.ways)(activity.relations)(existingEnc)(updatedEnc)
				(affectedParentsEnc)(relatedEnc).exec();
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
		int64_t ways = c[nodesCol].as<int64_t>();
		int64_t relations = c[relationsCol].as<int64_t>();

		uidOut.push_back(uid);
		std::vector<int64_t> oc = {nodes, ways, relations};
		objectCountOut.push_back(oc);
	}
}


