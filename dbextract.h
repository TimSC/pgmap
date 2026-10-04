#ifndef PGMAP_DBEXTRACT_H
#define PGMAP_DBEXTRACT_H
#include <pqxx/pqxx>
#include <string>
#include <cstdint>
#include <memory>
#include <vector>

// Resumable export. The caller keeps its transaction alive until completion.
class PgExtractExport
{
    friend class PgTransaction;
    std::shared_ptr<pqxx::connection> connection;
    std::shared_ptr<class PgWork> work;
    std::shared_ptr<class IDataStreamHandler> output;
    std::string prefix;
    int64_t extractId, lastId = 0;
    int phase = 0;
    bool storedBboxMode = false, mapBboxMode = false, pendingActivity = false;
    PgExtractExport(std::shared_ptr<pqxx::connection>, std::shared_ptr<class PgWork>,
        const std::string &, int64_t, const std::string &,
        std::shared_ptr<class IDataStreamHandler>);
public:
    int64_t GetId() const { return extractId; }
    // Query mode stored with the extract, and the map's current query mode.
    bool GetStoredBboxMode() const { return storedBboxMode; }
    bool GetMapBboxMode() const { return mapBboxMode; }
    // True if the map has edit activity later than the extract's checkpoint.
    bool HasPendingActivity() const { return pendingActivity; }
    // Encode up to 1000 objects; return 1 when the XML document is complete.
    int Continue();
};

// Caller holds main-map locks followed by EXCLUSIVE locks on all extract tables.
// Recalculates the extract from the latest visible source snapshot, whatever
// kinds of object were edited, and advances its checkpoint. Returns the extract
// ID. Throws on ungrouped activity; caller must abort the transaction on failure.
int64_t DbUpdateExtract(pqxx::connection &connection, pqxx::transaction_base &work,
	const std::string &staticPrefix, const std::string &prefix,
	int64_t extractId, const std::string &name);

// One object whose presence or version differs. A version of zero means the
// object is absent from that side.
class ExtractDifference
{
public:
	std::string type;
	int64_t objId = 0;
	int64_t extractVersion = 0, queryVersion = 0;
};

class ExtractComparison
{
public:
	int64_t extractId = 0;
	std::vector<double> bbox;
	// Objects of each type: node, way, relation.
	std::vector<int64_t> extractCounts, queryCounts;
	int64_t missingFromExtract = 0, notInQuery = 0, versionMismatches = 0;
	// Every differing object, ordered by type then ID.
	std::vector<ExtractDifference> differences;
	// Reasons the two sides are expected to differ.
	bool pendingActivity = false, queryModeDiffers = false;

	int64_t NumDifferences() const { return missingFromExtract + notInQuery + versionMismatches; }
	bool Matches() const { return NumDifferences() == 0; }
};

// Caller holds EXCLUSIVE locks on all extract tables. Removes the extract's
// metadata, objects and membership rows; the map is untouched. Select by
// positive ID, or by a unique name when ID is zero. Returns the extract ID.
int64_t DbDeleteExtract(pqxx::connection &connection, pqxx::transaction_base &work,
	const std::string &prefix, int64_t extractId, const std::string &name);

// Metadata describing one stored extract.
class ExtractInfo
{
public:
	int64_t extractId = 0;
	std::string name;
	std::vector<double> bbox;
	bool useBboxInQuery = false;
	// Seconds since the epoch when the extract was saved or last updated.
	int64_t performedAt = 0;
	// Synchronization checkpoint; -1 when not established.
	int64_t editActivityId = -1, atomicEditId = -1;
	// True if the map has edit activity later than the checkpoint.
	bool pendingActivity = false;
	// Object counts; -1 when they were not requested.
	int64_t nodes = -1, ways = -1, relations = -1;
};

// Describe stored extracts in ascending ID order: all of them, or only the one
// with a positive extractId. Counting objects reads every row of each extract,
// so it is optional. Caller holds the extract locks.
void DbListExtracts(pqxx::connection &connection, pqxx::transaction_base &work,
	const std::string &prefix, int64_t extractId, bool withCounts,
	std::vector<ExtractInfo> &out);

// IDs of all stored extracts in ascending order. Caller holds the extract locks.
std::vector<int64_t> DbListExtractIds(pqxx::connection &connection,
	pqxx::transaction_base &work, const std::string &prefix);

// Compare a stored extract with a fresh map query of its bbox, both read from
// the transaction's snapshot. Object types, IDs and versions must agree;
// ordering is ignored. Does not modify the extract.
void DbCompareExtract(class PgTransaction &transaction, int64_t extractId,
	const std::string &name, ExtractComparison &out);
#endif
