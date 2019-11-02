/* pgmap.i */
%module pgmap
%module unicode_strings
%begin %{
#define SWIG_PYTHON_2_UNICODE
%}
%include "stdint.i"
%include "std_string.i"
%include "std_vector.i"
%include "std_map.i"
%include "std_set.i"
%include "std_shared_ptr.i"
%include "streambuf.i"
%include exception.i
using std::string;

%{
/* Put header files here */
#include "pgmap.h"
#include "cppo5m/OsmData.h"
#include "cppo5m/utils.h"
%}

%exception {
	try {
		$action
    } catch(const std::runtime_error& e) {
		std::stringstream ss;
		ss << "Standard runtime exception: " << e.what();
        SWIG_exception(SWIG_RuntimeError, ss.str().c_str());
	} catch(const std::exception& e) {
		std::stringstream ss;
		ss << "Standard exception: " << e.what();
		SWIG_exception(SWIG_UnknownError, ss.str().c_str());
	} catch(...) {
		SWIG_exception(SWIG_UnknownError, "Unknown exception");
	}
}

namespace std {
	%template(vectori) vector<int>;
	%template(vectori64) vector<int64_t>;
	%template(vectord) vector<double>;
	%template(vectorbool) vector<bool>;
	%template(vectorstring) vector<string>;
	%template(vectordd) vector<vector<double> >;

	%template(mapstringstring) std::map<std::string, std::string>;
	%template(mapi64i64) std::map<int64_t, int64_t>;

	%template(seti64) std::set<int64_t>;
	%template(setpairi64i64) std::set<std::pair<int64_t, int64_t> >;

	%template(pairi64i64) std::pair<int64_t, int64_t>;
};
typedef std::map<std::string, std::string> TagMap;

class MetaData
{
public:
	uint64_t version;
	int64_t timestamp, changeset;
	uint64_t uid;
	std::string username;
	bool visible, current;

	MetaData();
	virtual ~MetaData();
	MetaData( const MetaData &obj);
	MetaData& operator=(const MetaData &arg);
};

%shared_ptr(IDataStreamHandler)

class IDataStreamHandler
{
public:
	virtual bool Sync() {return false;};
	virtual bool Reset() {return false;};
	virtual bool Finish() {return false;};

	virtual bool StoreIsDiff(bool diff) {return false;};
	virtual bool StoreBounds(double x1, double y1, double x2, double y2) {return false;};
	virtual bool StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon) {return false;};
	virtual bool StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs) {return false;};
	virtual bool StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles) {return false;};

};

%shared_ptr(IOsmChangeBlock)

class IOsmChangeBlock
{
public:
	virtual ~IOsmChangeBlock() {};

	virtual void StoreOsmData(const std::string &action, const class OsmData &osmData, bool ifunused) {};
};

%shared_ptr(O5mEncodeBase)

class O5mEncodeBase : public IDataStreamHandler
{
public:
	O5mEncodeBase();
	virtual ~O5mEncodeBase();
};

%shared_ptr(PyO5mEncode)

class PyO5mEncode : public O5mEncodeBase
{
public:
	PyO5mEncode(PyObject* obj);
	virtual ~PyO5mEncode();
};

%shared_ptr(OsmXmlEncodeBase)

class OsmXmlEncodeBase : public IDataStreamHandler
{
public:
	OsmXmlEncodeBase();
	virtual ~OsmXmlEncodeBase();
};

%shared_ptr(PyOsmXmlEncode)

class PyOsmXmlEncode : public OsmXmlEncodeBase
{
public:
	PyOsmXmlEncode(PyObject* obj, const TagMap &customAttribs);
	virtual ~PyOsmXmlEncode();

	void SetOutput(PyObject* obj);
};

class OsmObject
{
public:
	int64_t objId;
	class MetaData metaData;
	TagMap tags; 

	OsmObject();
	virtual ~OsmObject();
	OsmObject( const OsmObject &obj);
};

class OsmNode : public OsmObject
{
public:
	double lat;
	double lon;

	OsmNode();
	virtual ~OsmNode();
	OsmNode( const OsmNode &obj);
};

class OsmWay : public OsmObject
{
public:
	std::vector<int64_t> refs;

	OsmWay();
	virtual ~OsmWay();
	OsmWay( const OsmWay &obj);
};

class OsmRelation : public OsmObject
{
public:
	std::vector<std::string> refTypeStrs;
	std::vector<int64_t> refIds;
	std::vector<std::string> refRoles;

	OsmRelation();
	virtual ~OsmRelation();
	OsmRelation( const OsmRelation &obj);
};

namespace std {
	%template(vectorosmnode) vector<class OsmNode>;
	%template(vectorosmway) vector<class OsmWay>;
	%template(vectorosmrelation) vector<class OsmRelation>;
};

%shared_ptr(OsmData)

class OsmData : public IDataStreamHandler
{
public:
	std::vector<class OsmNode> nodes;
	std::vector<class OsmWay> ways;
	std::vector<class OsmRelation> relations;
	std::vector<std::vector<double> > bounds;
	bool isDiff;

	OsmData();
	virtual ~OsmData();

	void StreamTo(class IDataStreamHandler &out, bool finishStream = true);
	void Clear();
};

namespace std {
	%template(vectorosmdata) vector<class OsmData>;
};

%shared_ptr(OsmChange)

class OsmChange : public IOsmChangeBlock
{
public:
	std::vector<class OsmData> blocks;
	std::vector<std::string> actions;
	std::vector<bool> ifunused;

	OsmChange();
	OsmChange( const OsmChange &obj);
	OsmChange& operator=(const OsmChange &arg);
	virtual ~OsmChange();

	virtual void StoreOsmData(const std::string &action, const class OsmData &osmData, bool ifunused);
};

class OsmXmlDecodeString
{
public:
	std::string errString;
	bool parseCompletedOk;
	class IDataStreamHandler *output;

	OsmXmlDecodeString();
	virtual ~OsmXmlDecodeString();

	bool DecodeSubString(const char *xml, size_t len, bool done);
};

class OsmChangeXmlDecodeString
{
public:
	std::string errString;
	bool parseCompletedOk;
	class IOsmChangeBlock *output;

	OsmChangeXmlDecodeString();
	virtual ~OsmChangeXmlDecodeString();

	bool DecodeSubString(const char *xml, size_t len, bool done);
};

class PgMapError
{
public:
	PgMapError();
	PgMapError(const std::string &connection);
	PgMapError(const PgMapError &obj);
	virtual ~PgMapError();

	std::string errStr;
};

%shared_ptr(PgChangeset)

class PgChangeset
{
public:
	PgChangeset();
	PgChangeset(const PgChangeset &obj);
	virtual ~PgChangeset();
	
	int64_t objId, uid, open_timestamp, close_timestamp;
	std::string username;
	TagMap tags;
	bool is_open, bbox_set;
	double x1, y1, x2, y2;
};

namespace std {
	%template(vectorchangeset) vector<class PgChangeset>;
};

%shared_ptr(PgWork)

class PgWork
{
public:
	PgWork();
	PgWork(pqxx::transaction_base *workIn);
	PgWork(const PgWork &obj);
	virtual ~PgWork();
	PgWork& operator=(const PgWork &obj);

	std::shared_ptr<pqxx::transaction_base> work;
};

%shared_ptr(PgMapQuery)

class PgMapQuery
{
public:
	PgMapQuery(const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		std::shared_ptr<pqxx::connection> &db,
		std::shared_ptr<class PgWork> sharedWorkIn,
		class DbUsernameLookup &dbUsernameLookupIn);
	virtual ~PgMapQuery();

	int Start(const std::vector<double> &bbox, std::shared_ptr<IDataStreamHandler> &enc);
	int Start(const std::string &wkt, std::shared_ptr<IDataStreamHandler> &enc);
	int Continue();
	void Reset();
};

%shared_ptr(PgTransaction)

class PgTransaction
{
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
	void GetWaysForNodes(const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);	
	void GetRelationsForObjs(const std::string &type, const std::set<int64_t> &objectIds, 
		std::shared_ptr<IDataStreamHandler> out);
	void GetAffectedObjects(std::shared_ptr<class OsmData> inputObjects,
		std::shared_ptr<class OsmData> outputObjects);

	bool ResetActiveTables(class PgMapError &errStr);
	void GetReplicateDiff(int64_t timestampStart, int64_t timestampEnd,
		class OsmChange &out);
	void Dump(bool onlyLiveData, bool nodes, bool ways, bool relations, 
		std::shared_ptr<IDataStreamHandler> enc);

	int64_t GetAllocatedId(const std::string &type);
	int64_t PeekNextAllocatedId(const std::string &type);

	int GetChangeset(int64_t objId,
		class PgChangeset &changesetOut,
		class PgMapError &errStr);
	int GetChangesetOsmChange(int64_t changesetId,
		std::shared_ptr<class IOsmChangeBlock> output,
		class PgMapError &errStr);
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
	bool CloseChangeset(int64_t changesetId,
		int64_t closedTimestamp,
		class PgMapError &errStr);
	bool CloseChangesetsOlderThan(int64_t whereBeforeTimestamp,
		int64_t closedTimestamp,
		class PgMapError &errStr);

	std::string GetMetaValue(const std::string &key, 
		class PgMapError &errStr);
	bool SetMetaValue(const std::string &key, 
		const std::string &value, 
		class PgMapError &errStr);
	bool UpdateUsername(int uid, const std::string &username,
		class PgMapError &errStr);

	bool GetHistoricMapQuery(const std::vector<double> &bbox, 
		int64_t existsAtTimestamp,
		std::shared_ptr<IDataStreamHandler> &enc);

	void Commit();
	void Abort();
};

%shared_ptr(PgMpap)

class PgMap
{
public:
	PgMap(const std::string &connection, const std::string &tableStaticPrefixIn, 
		const std::string &tableActivePrefixIn,
		const std::string &tableModPrefixIn,
		const std::string &tableTestPrefixIn);
	virtual ~PgMap();
	PgMap& operator=(const PgMap&) {return *this;};

	bool Ready();

	std::shared_ptr<class PgTransaction> GetTransaction(const std::string &shareMode);
};

// Convenience functions: load and save from std::streambuf

void LoadFromO5m(std::streambuf &fi, class IDataStreamHandler *output);
void LoadFromOsmXml(std::streambuf &fi, class IDataStreamHandler *output);
void LoadFromPbf(std::streambuf &fi, class IDataStreamHandler *output);
void LoadFromDecoder(std::streambuf &fi, class OsmDecoder *osmDecoder, class IDataStreamHandler *output);

void LoadFromOsmChangeXml(std::streambuf &fi, class IOsmChangeBlock *output);

void SaveToO5m(const class OsmData &osmData, std::streambuf &fi);
void SaveToOsmXml(const class OsmData &osmData, std::streambuf &fi);
void SaveToOsmChangeXml(const class OsmChange &osmChange, bool separateActions, std::streambuf &fi);

std::shared_ptr<class OsmDecoder> DecoderOsmFactory(std::streambuf &handleIn, const std::string &filename);

// Convenience functions: load from std::string

void LoadFromO5m(const std::string &fi, class IDataStreamHandler *output);
void LoadFromOsmXml(const std::string &fi, class IDataStreamHandler *output);

void LoadFromOsmChangeXml(const std::string &fi, class IOsmChangeBlock *output);

