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
#include "cppo5m/cppo5m.h"
#include "cppo5m/pysink.h"

// Python classes for the cppo5m exception types, created when the module loads
static PyObject *pyOsmDecodeError = nullptr;
static PyObject *pyOsmLimitError = nullptr;
%}

%init %{
	// OsmDecodeError is a RuntimeError, so code catching RuntimeError still works.
	// OsmLimitError carries (message, limit name, maximum, actual) in its args.
	pyOsmDecodeError = PyErr_NewException("pgmap.OsmDecodeError", PyExc_RuntimeError, nullptr);
	Py_INCREF(pyOsmDecodeError);
	PyModule_AddObject(m, "OsmDecodeError", pyOsmDecodeError);
	pyOsmLimitError = PyErr_NewException("pgmap.OsmLimitError", pyOsmDecodeError, nullptr);
	Py_INCREF(pyOsmLimitError);
	PyModule_AddObject(m, "OsmLimitError", pyOsmLimitError);
%}

%pythoncode %{
OsmDecodeError = _pgmap.OsmDecodeError
OsmLimitError = _pgmap.OsmLimitError
%}

%exception {
	try {
		$action
	} catch(const OsmLimitError& e) {
		PyObject *args = Py_BuildValue("(ssnn)", e.what(), e.limit.c_str(),
			(Py_ssize_t)e.maximum, (Py_ssize_t)e.actual);
		PyErr_SetObject(pyOsmLimitError, args);
		Py_XDECREF(args);
		SWIG_fail;
	} catch(const OsmDecodeError& e) {
		PyErr_SetString(pyOsmDecodeError, e.what());
		SWIG_fail;
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
	%template(vectorvectori64) vector<vector<int64_t> >;
	%template(vectorpairi64i64) vector<std::pair<int64_t, int64_t> >;
	%template(vectord) vector<double>;
	%template(vectorbool) vector<bool>;
	%template(vectorstring) vector<string>;
	%template(vectordd) vector<vector<double> >;

	%template(mapstringstring) std::map<std::string, std::string>;
	%template(mapstringi64) std::map<std::string, int64_t>;
	%template(mapi64i64) std::map<int64_t, int64_t>;
	%template(mapi64vectord) std::map<int64_t, vector<double> >;

	%template(seti64) std::set<int64_t>;
	%template(setpairi64i64) std::set<std::pair<int64_t, int64_t> >;

	%template(pairi64i64) std::pair<int64_t, int64_t>;
};

// ****** cppo5m ******

%shared_ptr(IDataStreamHandler)
%shared_ptr(IOsmChangeHandler)
%shared_ptr(ByteSink)
%shared_ptr(StreamSink)
%shared_ptr(StringSink)
%shared_ptr(PySink)
%shared_ptr(OsmEncoder)
%shared_ptr(O5mEncode)
%shared_ptr(PyO5mEncode)
%shared_ptr(OsmXmlEncode)
%shared_ptr(PyOsmXmlEncode)
%shared_ptr(OsmChangeXmlEncode)
%shared_ptr(OsmJsonEncode)
%shared_ptr(PyOsmJsonEncode)
%shared_ptr(FindBbox)
%shared_ptr(DeduplicateOsm)
%shared_ptr(SortOsm)
%shared_ptr(OsmData)
%shared_ptr(OsmChange)

// Python copies objects with, for example, pgmap.OsmNode(node)
%copyctor MetaData;
%copyctor Bounds;
%copyctor OsmNode;
%copyctor OsmWay;
%copyctor RelationMember;
%copyctor OsmRelation;
%copyctor OsmData;
%copyctor OsmChangeBlock;
%copyctor OsmChange;

// The C++ exception types are raised as the Python classes defined below
%ignore OsmDecodeError;
%ignore OsmLimitError;
// Stream decoders read from C++ stream buffers; Python uses the push parsers
// and the LoadFrom functions instead
%ignore OsmDecoder;
%ignore O5mDecode;
%ignore OsmXmlDecode;
%ignore OsmChangeXmlDecode;
%ignore OsmXmlObjectReader;
%ignore MakeDecoder;
%ignore MakeEncoder;
// Python passes bytes through FeedBytes, or text through Feed(str, final)
%ignore XmlPushParser::Feed(const char *, size_t, bool);

%include "cppo5m/handler.h"
%include "cppo5m/model.h"

%template(vectorbounds) std::vector<Bounds>;
%template(vectorosmnode) std::vector<OsmNode>;
%template(vectorosmway) std::vector<OsmWay>;
%template(vectorrelationmember) std::vector<RelationMember>;
%template(vectorosmrelation) std::vector<OsmRelation>;
%template(vectorosmchangeblock) std::vector<OsmChangeBlock>;

%include "cppo5m/sink.h"
%include "cppo5m/pysink.h"
%include "cppo5m/encoder.h"
%include "cppo5m/decoder.h"
%include "cppo5m/o5m.h"
%include "cppo5m/osmxml.h"
%include "cppo5m/osmchangexml.h"
%include "cppo5m/osmjson.h"
%include "cppo5m/filters.h"

%extend RelationMember {
	///The member type as "node", "way" or "relation"
	std::string TypeName() const { return ObjectTypeName($self->type); }
}

%extend OsmRelation {
	///Appends a member, giving its type as "node", "way" or "relation"
	void AddMember(const std::string &type, int64_t ref, const std::string &role = std::string())
	{
		$self->members.push_back(RelationMember(ObjectTypeFromName(type), ref, role));
	}
}

%extend XmlPushParser {
	///Parses the next piece of a document held in a Python bytes object. The
	///pieces may split the text anywhere, including inside a character.
	void FeedBytes(PyObject *data, bool final)
	{
		char *buffer = nullptr;
		Py_ssize_t length = 0;
		if(PyBytes_AsStringAndSize(data, &buffer, &length) < 0)
			throw std::invalid_argument("FeedBytes expects a bytes object");
		$self->Feed(buffer, (size_t)length, final);
	}
}

%inline %{
///Writes OSM XML to a Python file object.
class PyOsmXmlEncode : public OsmXmlEncode
{
public:
	PyOsmXmlEncode(PyObject *obj, const TagMap &customAttribs = TagMap()) :
		OsmXmlEncode(std::make_shared<PySink>(obj), customAttribs) {}

	///Sends later output to another file object, such as a fresh buffer
	void SetOutput(PyObject *obj)
	{
		std::static_pointer_cast<PySink>(this->GetSink())->SetOutput(obj);
	}
};

///Writes OSM JSON to a Python file object.
class PyOsmJsonEncode : public OsmJsonEncode
{
public:
	PyOsmJsonEncode(PyObject *obj, const TagMap &customAttribs = TagMap()) :
		OsmJsonEncode(std::make_shared<PySink>(obj), customAttribs) {}

	///Sends later output to another file object, such as a fresh buffer
	void SetOutput(PyObject *obj)
	{
		std::static_pointer_cast<PySink>(this->GetSink())->SetOutput(obj);
	}
};

///Writes o5m to a Python file object.
class PyO5mEncode : public O5mEncode
{
public:
	PyO5mEncode(PyObject *obj) : O5mEncode(std::make_shared<PySink>(obj)) {}

	void SetOutput(PyObject *obj)
	{
		std::static_pointer_cast<PySink>(this->GetSink())->SetOutput(obj);
	}
};
%}

%shared_ptr(PgChangeset)

namespace std {
	%template(vectorchangeset) vector<PgChangeset>;
	%template(vectorsharedptreditactivity) vector<shared_ptr<EditActivity> >;
};

%shared_ptr(EditActivity)
%shared_ptr(PgWork)
%shared_ptr(PgCommon)

%include "pgcommon.h"
%shared_ptr(PgExtractExport)
%ignore DbUpdateExtract;
%ignore DbCompareExtract;
%ignore DbListExtractIds;
%ignore DbDeleteExtract;
%ignore DbListExtracts;
%copyctor ExtractInfo;
%copyctor ExtractDifference;
%copyctor ExtractComparison;
%include "dbextract.h"
namespace std {
	%template(vectorextractdifference) vector<ExtractDifference>;
	%template(vectorextractcomparison) vector<ExtractComparison>;
	%template(vectorextractinfo) vector<ExtractInfo>;
};

%shared_ptr(PgMapQuery)
%shared_ptr(PgTransaction)
%shared_ptr(PgAdmin)
%shared_ptr(PgMap)

%include "pgmap.h"
%include "cppo5m/io.h"
%include "dbeditactivity.h"

/*
%shared_ptr(PbfDecode)
%shared_ptr(PbfEncodeBase)
%shared_ptr(PbfEncode)
%shared_ptr(PyPbfEncode)

%include "cppo5m/pbf.h"
*/
