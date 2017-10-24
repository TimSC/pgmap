#include "dbchangeset.h"
using namespace std;

void DecodeRowsToChangesets(pqxx::result &r, std::vector<class PgChangeset> &changesets)
{
	for (unsigned int rownum=0; rownum < r.size(); ++rownum)
	{
		const pqxx::result::tuple row = r[rownum];
		string id;
		int64_t maxid = 0;
		for (unsigned int colnum=0; colnum < row.size(); ++colnum)
		{
			const pqxx::result::field field = row[colnum];
			cout << field.name() << endl;
			//id = pqxx::to_string(field);
		}
	}
}

bool GetChangesetFromDb(pqxx::work *work, 
	const std::string &tablePrefix,
	int64_t objId,
	class PgChangeset &changesetOut,
	std::string &errStr)
{
	string changesetTable = tablePrefix + "changesets";

	stringstream sql;
	sql << "SELECT * FROM " << changesetTable << " WHERE id = " << objId << ";";

	pqxx::result r = work->exec(sql.str());
	if(r.size() < 1)
	{
		errStr = "Changeset not found";
		return false;
	}

	std::vector<class PgChangeset> changesets;
	DecodeRowsToChangesets(r, changesets);
	changesetOut = changesets[0];
	return true;
}

