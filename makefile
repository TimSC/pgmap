

all: dump extract admin osm2csv swigpy2

common = util.cpp dbquery.cpp dbids.cpp dbadmin.cpp dbcommon.cpp dbreplicate.cpp \
	dbdecode.cpp dbstore.cpp dbdump.cpp dbfilters.cpp dbchangeset.cpp dbjson.cpp pgmap.cpp cppo5m/o5m.cpp \
	cppo5m/varint.cpp cppo5m/OsmData.cpp cppo5m/osmxml.cpp cppo5m/cppiso8601/iso8601.cpp cppGzip/EncodeGzip.cpp

dump: dump.cpp $(common)
	g++ $^ -std=c++11 -Wall -lpqxx -lexpat -lz -o $@

extract: extract.cpp $(common)
	g++ $^ -std=c++11 -Wall -lpqxx -lexpat -lz -o $@

admin: admin.cpp $(common)
	g++ $^ -std=c++11 -Wall -lpqxx -lexpat -lz -o $@

osm2csv: osm2csv.cpp dbjson.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppo5m/osmxml.cpp cppo5m/cppiso8601/iso8601.cpp cppGzip/DecodeGzip.cpp cppGzip/EncodeGzip.cpp 
	g++ $^ -std=c++11 -Wall -lexpat -lz -o $@

swigpy2: pgmap.i $(common)
	swig -python -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC -std=c++11 -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python2-config --includes --libs} -lpqxx -lexpat -lz -o _pgmap.so

swigpy3: pgmap.i $(common)
	swig -python -py3 -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC -std=c++11 -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python3-config --includes --libs} -lpqxx -lexpat -lz -g -o _pgmap.so

