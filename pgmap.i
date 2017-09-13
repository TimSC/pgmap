/* pgmap.i */
%module pgmap
%include "stdint.i"
%include "std_string.i"
%include "std_vector.i"
%include "std_map.i"
%include "std_set.i"
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

class PyO5mEncode : public IDataStreamHandler
{
public:
	PyO5mEncode(PyObject* obj);
	virtual ~PyO5mEncode();
};

class PyOsmXmlEncode : public IDataStreamHandler
{
public:
	PyOsmXmlEncode(PyObject* obj);
	virtual ~PyOsmXmlEncode();	

	void SetOutput(PyObject* obj);
};

class OsmNode
{
public:
	int64_t objId;
	class MetaData metaData;
	TagMap tags; 
	double lat;
	double lon;

	OsmNode();
	virtual ~OsmNode();
	OsmNode( const OsmNode &obj);
	OsmNode& operator=(const OsmNode &arg);
};

class OsmWay
{
public:
	int64_t objId;
	class MetaData metaData;
	TagMap tags; 
	std::vector<int64_t> refs;

	OsmWay();
	virtual ~OsmWay();
	OsmWay( const OsmWay &obj);
	OsmWay& operator=(const OsmWay &arg);
};

class OsmRelation
{
public:
	int64_t objId;
	class MetaData metaData;
	TagMap tags; 
	std::vector<std::string> refTypeStrs;
	std::vector<int64_t> refIds;
	std::vector<std::string> refRoles;

	OsmRelation();
	virtual ~OsmRelation();
	OsmRelation( const OsmRelation &obj);
	OsmRelation& operator=(const OsmRelation &arg);
};

namespace std {
	%template(vectorosmnode) vector<class OsmNode>;
	%template(vectorosmway) vector<class OsmWay>;
	%template(vectorosmrelation) vector<class OsmRelation>;
};

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

class OsmXmlDecodeString
{
public:
	class IDataStreamHandler *output;
	std::string errString;
	bool parseCompletedOk;

	OsmXmlDecodeString();
	virtual ~OsmXmlDecodeString();

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

class PgMapQuery
{
public:
	PgMapQuery(const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn);
	virtual ~PgMapQuery();

	int Start(const std::vector<double> &bbox, IDataStreamHandler &enc);
	int Continue();
	void Reset();
};

class PgMap
{
public:
	class PgMapQuery pgMapQuery;

	PgMap(const string &connection, const string &tableStaticPrefixIn, 
		const string &tableActivePrefixIn);
	virtual ~PgMap();

	bool Ready();

	void Dump(bool onlyLiveData, IDataStreamHandler &enc);

	void GetObjectsById(const std::string &type, const std::set<int64_t> &objectIds, class IDataStreamHandler &out);
	bool StoreObjects(class OsmData &data, 
		std::map<int64_t, int64_t> &createdNodeIds, 
		std::map<int64_t, int64_t> &createdWayIds,
		std::map<int64_t, int64_t> &createdRelationIds,
		class PgMapError &errStr);
};

