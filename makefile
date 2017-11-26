

all: dump extract admin osm2csv swigpy2

common = util.cpp dbquery.cpp dbids.cpp dbadmin.cpp dbcommon.cpp dbreplicate.cpp \
	dbdecode.cpp dbstore.cpp dbdump.cpp dbfilters.cpp dbchangeset.cpp dbjson.cpp pgmap.cpp cppo5m/o5m.cpp \
	cppo5m/varint.cpp cppo5m/OsmData.cpp cppo5m/osmxml.cpp cppo5m/iso8601lib/iso8601.c cppGzip/EncodeGzip.cpp

libs = -lboost_filesystem -lboost_system -lpqxx -lexpat -lz

dump: dump.cpp $(common)
	g++ $^ -std=c++11 -Wall $(libs) -o $@

extract: extract.cpp $(common)
	g++ $^ -std=c++11 -Wall $(libs) -o $@

admin: admin.cpp $(common)
	g++ $^ -std=c++11 -Wall $(libs) -o $@

osm2csv: osm2csv.cpp dbjson.cpp util.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppo5m/osmxml.cpp cppo5m/iso8601lib/iso8601.c cppGzip/DecodeGzip.cpp cppGzip/EncodeGzip.cpp 
	g++ $^ -std=c++11 -Wall $(libs) -o $@

swigpy2: pgmap.i $(common)
	swig -python -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC -std=c++11 -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python2-config --includes --libs} $(libs) -o _pgmap.so

swigpy3: pgmap.i $(common)
	swig -python -py3 -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC -std=c++11 -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python3-config --includes --libs} $(libs) -g -o _pgmap.so

