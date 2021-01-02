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
	%template(vectorvectori64) vector<vector<int64_t> >;
	%template(vectord) vector<double>;
	%template(vectorbool) vector<bool>;
	%template(vectorstring) vector<string>;
	%template(vectordd) vector<vector<double> >;

	%template(mapstringstring) std::map<std::string, std::string>;
	%template(mapi64i64) std::map<int64_t, int64_t>;
	%template(mapi64vectord) std::map<int64_t, vector<double> >;

	%template(seti64) std::set<int64_t>;
	%template(setpairi64i64) std::set<std::pair<int64_t, int64_t> >;

	%template(pairi64i64) std::pair<int64_t, int64_t>;
};

%shared_ptr(MetaData)
%shared_ptr(IDataStreamHandler)
%shared_ptr(IOsmChangeBlock)
%shared_ptr(OsmDecoder)
%shared_ptr(FindBbox)

%shared_ptr(O5mEncodeBase)
%shared_ptr(PyO5mEncode)
%shared_ptr(O5mDecode)
%shared_ptr(O5mEncode)

%shared_ptr(OsmXmlEncodeBase)
%shared_ptr(PyOsmXmlEncode)
%shared_ptr(OsmXmlDecodeString)
%shared_ptr(OsmXmlDecode)
%shared_ptr(OsmXmlEncode)
%shared_ptr(OsmChangeXmlEncode)

%shared_ptr(DeduplicateOsm)

namespace std {
	%template(vectorosmnode) vector<class OsmNode>;
	%template(vectorosmway) vector<class OsmWay>;
	%template(vectorosmrelation) vector<class OsmRelation>;
};

%shared_ptr(OsmData)

namespace std {
	%template(vectorosmdata) vector<class OsmData>;
};

%shared_ptr(OsmChange)

%include "cppo5m/OsmData.h"
%include "cppo5m/osmxml.h"
%include "cppo5m/o5m.h"

%shared_ptr(PgChangeset)

namespace std {
	%template(vectorchangeset) vector<class PgChangeset>;
};

%shared_ptr(PgWork)
%shared_ptr(PgCommon)

%include "pgcommon.h"

%shared_ptr(PgMapQuery)
%shared_ptr(PgTransaction)
%shared_ptr(PgAdmin)
%shared_ptr(PgMap)

%include "pgmap.h"
%include "cppo5m/utils.h"

/*
%shared_ptr(PbfDecode)
%shared_ptr(PbfEncodeBase)
%shared_ptr(PbfEncode)
%shared_ptr(PyPbfEncode)

%include "cppo5m/pbf.h"
*/

