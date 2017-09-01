/* pgmap.i */
%module pgmap
%include "std_string.i"
%include "std_vector.i"
using std::string;
namespace std {
   %template(vectori) vector<int>;
   %template(vectord) vector<double>;
};

%{
/* Put header files here */
#include "pgmap.h"
%}

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

class PgMap
{
public:
	PgMap(const std::string &connection, const string &tablePrefixIn);
	virtual ~PgMap();

	bool Ready();
	void MapQuery(const std::vector<double> &bbox, IDataStreamHandler &enc);
	void Dump(bool onlyLiveData, IDataStreamHandler &enc);
};

class O5mEncode : public IDataStreamHandler
{
public:
	O5mEncode(std::streambuf &handle);
	O5mEncode(PyObject* obj);
	virtual ~O5mEncode();
};

