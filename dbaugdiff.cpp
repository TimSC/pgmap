
#include "dbaugdiff.h"

void DbQueryChangesSinceTimestamp()
{
	

}

bool DbQueryAugDiff(pqxx::connection &c, pqxx::transaction_base *work, 
	const std::string &staticTablePrefix, 
	const std::string &activeTablePrefix, 
	class DbUsernameLookup &dbUsernameLookup, 
	int64_t startTimestamp,
	bool bboxSet,
	const std::vector<double> &bbox,
	std::streambuf &fiOut,
	std::string &errStr)
{
	fiOut.sputn("test", 4);

	return true;
}
