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
	virtual ~O5mEncode();
};

%inline %{
//Based on http://swig.10945.n7.nabble.com/Using-std-istream-from-Python-tp3733p3735.html
class CPyOutbuf : public std::streambuf
{
public:
	CPyOutbuf(PyObject* obj) {
		m_PyObj = obj;
		Py_INCREF(m_PyObj);
		m_Write = PyObject_GetAttrString(m_PyObj, "write");
	}
	~CPyOutbuf() {
		Py_XDECREF(m_Write);
		Py_XDECREF(m_PyObj);
	}
protected:
	int overflow (int c = EOF)
	{
		if(m_Write != nullptr)
		{
			PyObject* ret = PyObject_CallFunction(m_Write, (char *)"c", c);
			Py_XDECREF(ret);
		}
		return c;
	}
	std::streamsize xsputn(const char* s, std::streamsize count) {
		if(m_Write != nullptr)
		{
			#if PY_MAJOR_VERSION < 3
			PyObject* ret = PyObject_CallFunction(m_Write, (char *)"s#", s, int(count));
			#else
			PyObject* ret = PyObject_CallFunction(m_Write, (char *)"y#", s, int(count));
			#endif 
			Py_XDECREF(ret);
		}
		return count;
	}
	PyObject* m_PyObj;
	PyObject* m_Write;
};

%} 

