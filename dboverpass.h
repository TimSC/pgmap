#ifndef _DB_OVERPASS_H
#define _DB_OVERPASS_H

#include <pqxx/pqxx>
#include "cppo5m/o5m.h"
#include "cppo5m/model.h"
#include "dbusername.h"

//Returns complete objects
void DbXapiQueryVisible(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox, 
	std::shared_ptr<IDataStreamHandler> enc);

//Returns only objects of specified type
void DbXapiQueryObjVisible(pqxx::connection &c, pqxx::transaction_base *work, 
	class DbUsernameLookup &usernames, 
	const std::string &tablePrefix, 
	const std::string &objType,
	const std::string &tagKey,
	const std::string &tagValue,
	const std::vector<double> &bbox, 
	std::shared_ptr<IDataStreamHandler> enc);

///One condition on the tags of an object, as written in an Overpass query:
///[key], [!key], [key=value], [key!=value], [key~regex] or [key!~regex].
///The two negative value conditions also match objects without the key.
class OverpassTagFilter
{
public:
	enum Op { Exists = 0, NotExists = 1, Equals = 2, NotEquals = 3, Matches = 4, NotMatches = 5 };
	std::string key;
	std::string value; //The value or regular expression; unused by Exists and NotExists
	int op = Exists;
	bool ignoreCase = false; //For Matches and NotMatches

	OverpassTagFilter() {}
	OverpassTagFilter(int opIn, const std::string &keyIn, const std::string &valueIn = std::string(),
		bool ignoreCaseIn = false) : key(keyIn), value(valueIn), op(opIn), ignoreCase(ignoreCaseIn) {}
};

///Further conditions of an Overpass query, beyond tags, bbox and IDs. Left as
///constructed, it adds none.
class OverpassQueryOptions
{
public:
	//Last edited by one of these users, matched by ID or by the name stored
	//with the object. Both empty means any user.
	std::vector<int64_t> uids;
	std::vector<std::string> usernames;
	int64_t newerThan = -1; //Last edited after this time; -1 means any time
	int64_t changedSince = -1; //Last edited at or after this time; -1 means any time
	int64_t changedUntil = -1; //Last edited at or before this time; -1 means any time
	//Within aroundRadius metres of one of these positions, given as lon, lat,
	//lon, lat and so on. Only nodes can be found this way.
	std::vector<double> aroundPoints;
	double aroundRadius = 0.0;
};

///Finds the visible objects of one type that meet every condition given.
///bbox is empty or left, bottom, right, top; a way or relation is selected if
///its own stored bounding box overlaps it, so use a bbox for those only on a
///map whose useBboxInQuery setting says the boxes are stored. ids, if not empty, limits the search to
///those objects. limit is the most objects to return, or zero for no limit.
///Throws std::invalid_argument for a regular expression the database rejects,
///and std::runtime_error starting "Query timed out" if the statement timeout
///ends the query; either leaves the transaction unusable.
void DbOverpassQueryObjVisible(pqxx::connection &c, pqxx::transaction_base *work,
	class DbUsernameLookup &usernames,
	const std::string &tablePrefix,
	const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	std::shared_ptr<IDataStreamHandler> enc,
	const OverpassQueryOptions &options = OverpassQueryOptions());

///As DbOverpassQueryObjVisible, but appends only the IDs of the objects found.
void DbOverpassQueryIdsVisible(pqxx::connection &c, pqxx::transaction_base *work,
	const std::string &tablePrefix,
	const std::string &objType,
	const std::vector<OverpassTagFilter> &filters,
	const std::vector<double> &bbox,
	const std::vector<int64_t> &ids,
	size_t limit,
	std::vector<int64_t> &idsOut,
	const OverpassQueryOptions &options = OverpassQueryOptions());

#endif //_DB_OVERPASS_H
