#ifndef _PGMAP_H
#define _PGMAP_H

#include <string>
#include <set>
#include <map>
#include <pqxx/pqxx> //apt install libpqxx-dev
#include "cppo5m/o5m.h"
#include "cppo5m/osmxml.h"
#include "cppo5m/model.h"
#include "dbusername.h"
#include "pgcommon.h"
#include "dbeditactivity.h"
#include "dbextract.h"
#include "dboverpass.h"

class PgMapError
{
public:
	PgMapError();
	PgMapError(const std::string &connection);
	PgMapError(const PgMapError &obj);
	virtual ~PgMapError();

	std::string errStr;
};

class PgChangeset
{
public:
	PgChangeset();
	PgChangeset(const PgChangeset &obj);
	virtual ~PgChangeset();
	PgChangeset& operator=(const PgChangeset &obj);
	
	int64_t objId, uid, open_timestamp, close_timestamp;
	std::string username;
	TagMap tags;
	bool is_open, bbox_set;
	double x1, y1, x2, y2;
	// Objects created, modified and deleted in the changeset. These are not
	// stored with it: they stay zero until GetChangesetChangeCounts fills them.
	int64_t created_count, modified_count, deleted_count;
};

// Selects and orders the changesets returned by PgTransaction::GetChangesets.
class PgChangesetQuery
{
public:
	int64_t user_uid = 0; //0 means don't filter
	int64_t openedBeforeTimestamp = -1; //Opened before this; -1 means don't filter
	int64_t openedFromTimestamp = -1; //Opened at or after this; -1 means don't filter
	int64_t closedAfterTimestamp = -1; //Still open, or closed at or after this; -1 means don't filter
	bool is_open_only = false;
	bool is_closed_only = false;
	bool oldestFirst = false; //Order by opening time, newest first unless set
	size_t limit = 100; //0 means no limit
	std::vector<int64_t> ids; //Empty means don't filter
	std::vector<double> bbox; //Empty, or left, bottom, right, top to find changesets overlapping it
};

class PgMapQuery
{
private:
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::shared_ptr<pqxx::connection> dbconn;
	bool mapQueryActive;
	int mapQueryPhase;
	std::shared_ptr<IDataStreamHandler> mapQueryEnc;
	std::vector<double> mapQueryBbox;
	std::string mapQueryWkt;
	std::vector<double> mapQueryWktBounds; // The rectangle around mapQueryWkt
	std::shared_ptr<class PgWork> sharedWork;
	std::shared_ptr<pqxx::icursorstream> cursor;
	std::string extraNodesSql, relationsSql;
	class DbUsernameLookup &dbUsernameLookup;
	bool useBboxInQuery;

	int StartCommon(const std::vector<double> &bbox, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc);
	void SelectObjects();

public:
	PgMapQuery(const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		std::shared_ptr<pqxx::connection> &db,
		std::shared_ptr<class PgWork> sharedWorkIn,
		class DbUsernameLookup &dbUsernameLookupIn);
	virtual ~PgMapQuery();
	PgMapQuery& operator=(const PgMapQuery&);

	int Start(const std::vector<double> &bbox, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc);
	int Start(const std::string &wkt, int64_t timestamp, std::shared_ptr<IDataStreamHandler> &enc);
	int Continue();
	void Reset();
};

class EditActivityRecorder;

class PgTransaction : public PgCommon
{
private:
	int64_t atomicEditId = 0;
	int64_t activityBlockIndex = 0;
	int64_t SaveExtractArea(const std::string &areaSql, const std::vector<double> &bbox,
		const std::string &wkt, const std::string &name);
	std::shared_ptr<class EditActivityRecorder> activityRecorder;
	void LockExtractTables(const std::string &accessMode);


public:
	PgTransaction(std::shared_ptr<pqxx::connection> dbconnIn,
		const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		std::shared_ptr<class PgWork> sharedWorkIn,
		const std::string &shareMode);
	virtual ~PgTransaction();

	std::shared_ptr<class PgMapQuery> GetQueryMgr();

	void GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);
	void GetFullObjectById(const std::string &type, int64_t objectId, 
		std::shared_ptr<IDataStreamHandler> out);
	void GetObjectsByIdVer(const std::string &type, const std::set<std::pair<int64_t, int64_t> > &objectIdVers, 
		std::shared_ptr<IDataStreamHandler> out);
	void GetObjectsHistoryById(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);

	bool StoreObjects(class OsmData &data, 
		std::map<int64_t, int64_t> &createdNodeIds, 
		std::map<int64_t, int64_t> &createdWaysIds,
		std::map<int64_t, int64_t> &createdRelationsIds,
		bool saveToStaticTables,
		class PgMapError &errStr);
	int UpdateObjectBboxesById(
		const std::string &objType,
		const std::set<int64_t> &objectIds, int verbose, 
		bool saveToStaticTables,
		class PgMapError &errStr);
	// Recording a block of an upload as edit activity. Call the first before
	// the block is stored, which also gives the ways and relations holding
	// its objects, and the second once it is stored and the boxes of its ways
	// and relations and of those parents are up to date. The rules for what
	// is recorded are with EditActivityRecorder.
	void EditActivityBeforeStore(const std::string &action, std::shared_ptr<class OsmData> block,
		std::shared_ptr<class OsmData> affectedParentsOut);
	bool EditActivityAfterStore(const std::string &action, std::shared_ptr<class OsmData> block,
		int64_t changeset, int64_t uid, int64_t timestamp, class PgMapError &errStr);
	bool InsertEditActivity(const class EditActivity &activity,
		class PgMapError &errStr);

	bool ResetActiveTables(class PgMapError &errStr);
	void GetReplicateDiff(int64_t timestampStart, int64_t timestampEnd,
		class OsmChange &out);
	void Dump(bool order, bool nodes, bool ways, bool relations, 
		std::shared_ptr<IDataStreamHandler> enc);
	// Write the whole map to a file, as the dump tool does. The file name
	// selects the format: .osm, .o5m, .pbf or .json, optionally followed by
	// .gz. With updateInfo, what is needed to keep the file up to date goes
	// in its header, in the formats that have somewhere to hold it (all but
	// .pbf): the latest edit activity ID and atomic edit ID of this snapshot,
	// and publicUrl, this server's address, as update_url if it is given. The
	// file appears under its name only once it is complete. Throws
	// std::invalid_argument for an unusable name.
	void DumpToFile(const std::string &filename, bool updateInfo, const std::string &publicUrl = "");

	int64_t GetAllocatedId(const std::string &type);
	int64_t PeekNextAllocatedId(const std::string &type);

	int GetChangeset(int64_t objId,
		class PgChangeset &changesetOut,
		class PgMapError &errStr);
	int GetChangesetOsmChange(int64_t changesetId,
		std::shared_ptr<class IOsmChangeHandler> output,
		class PgMapError &errStr);
	bool GetChangesets(std::vector<class PgChangeset> &changesetsOut,
		const class PgChangesetQuery &query,
		class PgMapError &errStr);
	// Fill in the created, modified and deleted counts from the edit activity
	// recorded for each changeset. Changesets with no recorded activity, such
	// as those imported with the map, are left at zero.
	void GetChangesetChangeCounts(std::vector<class PgChangeset> &changesets);
	void GetChangesetChangeCounts(class PgChangeset &changeset);
	// The number of changesets a user has opened.
	int64_t GetChangesetCount(int64_t user_uid);
	bool GetChangesets(std::vector<class PgChangeset> &changesetsOut,
		int64_t user_uid, //0 means don't filter
		int64_t openedBeforeTimestamp, //-1 means don't filter
		int64_t closedAfterTimestamp, //-1 means don't filter
		bool is_open_only,
		bool is_closed_only,
		class PgMapError &errStr);
	int64_t CreateChangeset(const class PgChangeset &changeset,
		class PgMapError &errStr);
	bool UpdateChangeset(const class PgChangeset &changeset,
		class PgMapError &errStr);
	bool ExpandChangesetBbox(int64_t cid,
		const std::vector<double> &bbox,
		class PgMapError &errStr);
	bool CloseChangeset(int64_t changesetId,
		int64_t closedTimestamp,
		class PgMapError &errStr);
	bool CloseChangesetsOlderThan(int64_t whereBeforeTimestamp,
		int64_t closedTimestamp,
		class PgMapError &errStr);

	// Stream a rectangular snapshot to current-state extract tables and return its ID.
	// The caller commits metadata, checkpoints and contents together.
	int64_t SaveExtract(const std::vector<double> &bbox, const std::string &name);
	// The same for a boundary given as WKT: a valid polygon or multipolygon
	// not crossing the antimeridian (see DbCheckExtractBoundary).
	int64_t SaveExtractWkt(const std::string &wkt, const std::string &name);
	// A boundary given as WKT grown outwards by a distance in metres, as WKT,
	// to give to SaveExtractWkt or as an imported extract's boundary in place
	// of the one given. Checked before and after (see DbBufferExtractBoundary).
	std::string BufferExtractBoundary(const std::string &wkt, double bufferMetres);

	// Bring a stored extract up to date with the map and return its ID.
	// Select by positive ID, or by a unique name when ID is zero.
	int64_t UpdateExtract(int64_t extractId, const std::string &name);

	// Stream every stored object without rerunning the rectangle query.
	// Select by positive ID, or by a unique name when ID is zero.
	int64_t ExportExtract(int64_t extractId, const std::string &name,
		std::shared_ptr<IDataStreamHandler> output);
	std::shared_ptr<PgExtractExport> StartExportExtract(int64_t extractId,
		const std::string &name, std::shared_ptr<IDataStreamHandler> output);

	// Store a stream of map data as a new extract: send the returned handler a
	// map file's contents ending with Finish, then commit. An empty bbox takes the
	// rectangle from the stream's bounds; negative edit IDs take the checkpoint
	// from the stream's attributes, leaving none if it has no such attributes.
	std::shared_ptr<PgExtractImport> StartImportExtract(const std::string &name,
		const std::vector<double> &bbox = std::vector<double>(),
		int64_t editActivityId = -1, int64_t atomicEditId = -1);

	// Write a stored extract to a file, the format chosen by the file's name
	// (.osm, .o5m, .pbf or .json, optionally followed by .gz). With
	// updateInfo, the header gets GetExtractHeaderAttribs, where the format has
	// room. The file appears only once complete. Select by positive ID, or by a
	// unique name. Returns the extract's ID.
	int64_t ExportExtractToFile(int64_t extractId, const std::string &name,
		const std::string &filename, bool updateInfo, const std::string &publicUrl = "");
	// What a file of a stored extract needs to be kept up to date, as header
	// attributes: the edits it is current to (edit_activity_id,
	// atomic_edit_id), its area (bbox, as west,south,east,north, or boundary,
	// the WKT of one that is not a rectangle) and the server whose edits to
	// follow (update_url: the extract's own update URL, or else publicUrl, this
	// server's address, if given). Empty if the extract is not found or the
	// name is ambiguous.
	std::map<std::string, std::string> GetExtractHeaderAttribs(int64_t extractId, const std::string &name,
		const std::string &publicUrl = "");
	// Store a map file as a new extract, as StartImportExtract does, its
	// boundary the bbox, the WKT polygon, or else what the file says. Returns
	// {ID, nodes, ways, relations}. The caller commits.
	std::vector<int64_t> ImportExtractFromFile(const std::string &filename, const std::string &name,
		const std::vector<double> &bbox, const std::string &wkt,
		int64_t editActivityId = -1, int64_t atomicEditId = -1);

	// Remove a stored extract and its contents; returns its ID.
	int64_t DeleteExtract(int64_t extractId, const std::string &name);

	// Change what a stored extract holds from objects supplied by the caller,
	// without reading this map: see PgExtractPatch. Commit when done.
	std::shared_ptr<PgExtractPatch> StartPatchExtract(int64_t extractId);

	// Set whether an extract is to be updated automatically, and the API to
	// update it from: blank for this map's own, else an http or https URL.
	// Returns the extract's ID. Select by positive ID, or by a unique name
	// when ID is zero.
	int64_t SetExtractAutoUpdate(int64_t extractId, const std::string &name,
		bool autoUpdate, const std::string &updateUrl);

	// Describe every stored extract in ID order, without object counts.
	void ListExtracts(std::vector<ExtractInfo> &out);
	// Describe one extract, including object counts. Returns false if not found.
	bool GetExtract(int64_t extractId, ExtractInfo &out);

	// Compare a stored extract with a fresh map query of its bbox.
	void CompareExtract(int64_t extractId, const std::string &name,
		class ExtractComparison &out);
	// Compare every stored extract, in ID order, using this one snapshot.
	void CompareAllExtracts(std::vector<ExtractComparison> &out);

	bool GetEditActivityById(int64_t editActivityId,
		class EditActivity &activity,
		class PgMapError &errStr);
	// Latest edit activity row ID and atomic edit ID in this transaction's
	// snapshot, in that order; zero when there is no activity. Anything read in
	// the same transaction reflects exactly the edits up to these IDs.
	std::pair<int64_t, int64_t> GetLatestEditIds();
	// The same IDs as attributes for the root element of an OSM XML document.
	std::map<std::string, std::string> GetLatestEditIdAttribs();
	// Inclusive row range; lastId/atomicEditId zero means no corresponding filter.
	// Both give rows in ID order. limit, if positive, is the most to return.
	void QueryEditActivityByIds(int64_t firstId, int64_t lastId, int64_t atomicEditId,
		std::vector<std::shared_ptr<class EditActivity> > &editActivity,
		class PgMapError &errStr, int64_t limit = 0,
		const std::vector<double> &bbox = std::vector<double>(), bool newest = false);
	void QueryEditActivityByTimestamp(int64_t sinceTimestamp,
		int64_t untilTimestamp,
		std::vector<std::shared_ptr<class EditActivity> > &editActivity,
		class PgMapError &errStr, int64_t limit = 0,
		const std::vector<double> &bbox = std::vector<double>(), bool newest = false);

	std::string GetMetaValue(const std::string &key, 
		class PgMapError &errStr);
	bool SetMetaValue(const std::string &key, 
		const std::string &value, 
		class PgMapError &errStr);
	// Every metadata key and value: of the tables being edited, where the
	// settings that change how the map behaves are kept, or with staticTables
	// of the static tables beneath them.
	void GetMetaValues(std::map<std::string, std::string> &valuesOut, bool staticTables = false);
	// Remove a metadata key of the tables being edited, so that its default
	// applies again. Returns false if it was not set.
	bool DeleteMetaValue(const std::string &key);
	// How many current ways there are, followed by how many of them have no
	// stored bounding box, which a search by area in useBboxInQuery mode misses.
	std::vector<int64_t> CountWaysWithoutBbox();
	// How many ways the map has, and how many of them have no stored box,
	// quickly: exact for a small map, and for a large one estimated from
	// PostgreSQL's statistics, gathering them first if a table has none.
	// Returns {ways, without a box, 1 if exact or 0 if estimated}.
	std::vector<int64_t> EstimateWaysWithoutBbox();
	bool UpdateUsername(int uid, const std::string &username,
		class PgMapError &errStr);

	void XapiQuery(const std::string &objType,
		const std::string &tagKey,
		const std::string &tagValue,
		const std::vector<double> &bbox, 
		std::shared_ptr<IDataStreamHandler> enc);
	// Finds visible objects of one type by tag conditions, bbox and IDs, without
	// completing ways or relations. See DbOverpassQueryObjVisible.
	void OverpassQuery(const std::string &objType,
		const std::vector<OverpassTagFilter> &filters,
		const std::vector<double> &bbox,
		const std::vector<int64_t> &ids,
		size_t limit,
		std::shared_ptr<IDataStreamHandler> enc,
		const OverpassQueryOptions &options = OverpassQueryOptions());
	// The same search, giving only the IDs of what it finds.
	void OverpassQueryIds(const std::string &objType,
		const std::vector<OverpassTagFilter> &filters,
		const std::vector<double> &bbox,
		const std::vector<int64_t> &ids,
		size_t limit,
		std::vector<int64_t> &idsOut,
		const OverpassQueryOptions &options = OverpassQueryOptions());
	// Whether the map's useBboxInQuery setting is on: every way and relation
	// then has its bounding box stored, and searches by area may rely on it.
	bool UseBboxInQuery();
	// Ends any later statement in this transaction that runs for longer than
	// this; zero removes the limit. A query ended this way fails.
	void SetStatementTimeout(int64_t milliseconds);
	void GetMostActiveUsers(int64_t startTimestamp,
		std::vector<int64_t> &uidOut,
		std::vector<std::vector<int64_t> > &objectCountOut);

	void Commit();
	void Abort();
};

class PgAdmin : public PgCommon
{
private:
	std::string tableModPrefix;
	std::string tableTestPrefix;

public:
	PgAdmin(std::shared_ptr<pqxx::connection> dbconnIn,
		const std::string &tableStaticPrefixIn, 
		const std::string &tableModPrefixIn,
		const std::string &tableTestPrefixIn,
		std::shared_ptr<class PgWork> sharedWorkIn,
		const std::string &shareModeIn);
	virtual ~PgAdmin();

	virtual bool IsAdminMode();

	bool CreateMapTables(int verbose, int targetVer, bool latest, class PgMapError &errStr);
	bool DropMapTables(int verbose, class PgMapError &errStr);
	// Reads a map file straight into the static map tables, which should be
	// empty. The file name selects the format: .osm, .o5m, .pbf or .json,
	// optionally followed by .gz.
	bool ImportMapFile(int verbose, const std::string &filename, class PgMapError &errStr);
	bool CreateMapIndices(int verbose, class PgMapError &errStr);
	// The version of the indices "Create indicies" builds, and what to say
	// about the table sets whose indices are behind it: a line for each, or
	// nothing if all are up to date.
	int GetLatestIndexVersion();
	// Brings the indices of every table set to an index version, upgrading
	// or downgrading: 0 takes away all that "Create indicies" builds
	bool SetIndexVersion(int verbose, int targetVersion, class PgMapError &errStr);
	// The recorded index version of each table set, static, modify and test
	std::vector<int> GetIndexVersions();
	// The schema version of each table set, static, modify and test; 0 for
	// one that has none
	std::vector<int> GetSchemaVersions();
	std::string GetIndexVersionNotes();
	bool ApplyDiffs(const std::string &diffPath, int verbose, class PgMapError &errStr);
	bool RefreshMapIds(int verbose, class PgMapError &errStr);
	bool ImportChangesetMetadata(const std::string &fina, int verbose, class PgMapError &errStr);
	bool RefreshMaxChangesetUid(int verbose, class PgMapError &errStr);
	bool GenerateUsernameTable(int verbose, class PgMapError &errStr);

	// Works out the stored box of every way and relation. With
	// onlyMissingWays, ways that already have a box are passed over, which
	// takes up a run that was stopped. Got from GetAdmin() with no
	// transaction, the ways are committed a batch at a time.
	bool UpdateBboxes(int verbose, bool updateStatic, bool updateActive, class PgMapError &errStr,
		bool onlyMissingWays = false);
	bool CreateBboxIndices(int verbose, class PgMapError &errStr);
	bool DropBboxIndices(int verbose, class PgMapError &errStr);

	bool CheckNodesExistForWays(class PgMapError &errStr);
	bool CheckObjectIdTables(class PgMapError &errStr);

	void Commit();
	void Abort();
};

class PgMap
{
private:
	std::shared_ptr<pqxx::connection> dbconn;
	std::string tableStaticPrefix;
	std::string tableActivePrefix;
	std::string tableModPrefix;
	std::string tableTestPrefix;
	std::string connectionString;
	std::shared_ptr<class PgWork> sharedWork;

public:
	PgMap(const std::string &connection, const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		const std::string &tableModPrefixIn,
		const std::string &tableTestPrefixIn);
	virtual ~PgMap();
	PgMap& operator=(const PgMap&) {return *this;};

	bool Ready();

	//pqxx only supports one active transaction per connection
	std::shared_ptr<class PgTransaction> GetTransaction(const std::string &shareMode);
	std::shared_ptr<class PgAdmin> GetAdmin();
	std::shared_ptr<class PgAdmin> GetAdmin(const std::string &shareMode);
};

#endif //_PGMAP_H
