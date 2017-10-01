/* pgmap.i */
%module pgmap
%include "stdint.i"
%include "std_string.i"
%include "std_vector.i"
%include "std_map.i"
%include "std_set.i"
%include "std_shared_ptr.i"
%include "exception.i"
using std::string;

%{
/* Put header files here */
#include "pgmap.h"
#include "cppo5m/OsmData.h"
%}

namespace std {
	%template(vectori) vector<int>;
	%template(vectori64) vector<int64_t>;
	%template(vectord) vector<double>;
	%template(vectorstring) vector<string>;
	%template(vectordd) vector<vector<double> >;

	%template(mapstringstring) std::map<std::string, std::string>;
	%template(mapi64i64) std::map<int64_t, int64_t>;

	%template(seti64) std::set<int64_t>;
};
typedef std::map<std::string, std::string> TagMap;

class MetaData
{
public:
	uint64_t version;
	int64_t timestamp, changeset;
	uint64_t uid;
	std::string username;
	bool visible;

	MetaData();
	virtual ~MetaData();
	MetaData( const MetaData &obj);
	MetaData& operator=(const MetaData &arg);
};

%shared_ptr(IDataStreamHandler)

class IDataStreamHandler
{
public:
	virtual void Sync() {};
	virtual void Reset() {};
	virtual void Finish() {};

	virtual void StoreIsDiff(bool) {};
	virtual void StoreBounds(double x1, double y1, double x2, double y2) {};
	virtual void StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon) {};
	virtual void StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs) {};
	virtual void StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles) {};

};

%shared_ptr(IOsmChangeBlock)

class IOsmChangeBlock
{
public:
	virtual ~IOsmChangeBlock() {};

	virtual void StoreOsmData(const std::string &action, const class OsmData &osmData) {};
};

%shared_ptr(PyO5mEncode)

class PyO5mEncode : public IDataStreamHandler
{
public:
	PyO5mEncode(PyObject* obj);
	virtual ~PyO5mEncode();
};

%shared_ptr(PyOsmXmlEncode)

class PyOsmXmlEncode : public IDataStreamHandler
{
public:
	PyOsmXmlEncode(PyObject* obj);
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

	OsmChange();
	OsmChange( const OsmChange &obj);
	OsmChange& operator=(const OsmChange &arg);
	virtual ~OsmChange();

	virtual void StoreOsmData(const std::string &action, const class OsmData &osmData);
};

class OsmXmlDecodeString
{
public:
	std::shared_ptr<class IDataStreamHandler> output;
	std::string errString;
	bool parseCompletedOk;

	OsmXmlDecodeString();
	virtual ~OsmXmlDecodeString();

	bool DecodeSubString(const char *xml, size_t len, bool done);
};

class OsmChangeXmlDecodeString
{
public:
	std::string errString;
	bool parseCompletedOk;
	std::shared_ptr<class IOsmChangeBlock> output;

	OsmChangeXmlDecodeString();
	virtual ~OsmChangeXmlDecodeString();

	bool DecodeSubString(const char *xml, size_t len, bool done);
};

class PgMapError
{
public:
	PgMapError();
	PgMapError(const string &connection);
	PgMapError(const PgMapError &obj);
	virtual ~PgMapError();

	std::string errStr;
};

%shared_ptr(PgMapQuery)

class PgMapQuery
{
public:
	PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn,
		shared_ptr<pqxx::connection> &db);
	virtual ~PgMapQuery();

	int Start(const std::vector<double> &bbox, std::shared_ptr<IDataStreamHandler> &enc);
	int Continue();
	void Reset();
};

class PgMap
{
public:
	PgMap(const string &connection, const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn);
	virtual ~PgMap();

	bool Ready();

	void Dump(bool onlyLiveData, std::shared_ptr<IDataStreamHandler> &enc);

	std::shared_ptr<class PgMapQuery> GetQueryMgr();

	void GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, std::shared_ptr<IDataStreamHandler> &out);
	bool StoreObjects(class OsmData &data, 
		std::map<int64_t, int64_t> &createdNodeIds, 
		std::map<int64_t, int64_t> &createdWayIds,
		std::map<int64_t, int64_t> &createdRelationIds,
		class PgMapError &errStr);
	bool ResetActiveTables(class PgMapError &errStr);
};

void LoadFromO5m(std::streambuf &fi, std::shared_ptr<class IDataStreamHandler> output);
void LoadFromOsmXml(std::streambuf &fi, std::shared_ptr<class IDataStreamHandler> output);
void LoadFromOsmChangeXml(std::streambuf &fi, std::shared_ptr<class IOsmChangeBlock> output);
void SaveToO5m(const class OsmData &osmData, std::streambuf &fi);
void SaveToOsmXml(const class OsmData &osmData, std::streambuf &fi);

