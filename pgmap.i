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

class PgMap
{
public:
	PgMap(const std::string &connection, const string &tablePrefixIn);
	virtual ~PgMap();

	bool Ready();
	void MapQuery(const vector<double> &bbox, IDataStreamHandler &enc);
	void Dump(bool onlyLiveData, IDataStreamHandler &enc);
};


