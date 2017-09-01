
all: dump extract swigpy2

dump: dump.cpp util.cpp db.cpp pgmap.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppGzip/EncodeGzip.cpp
	g++ $^ -std=c++11 -Wall -lpqxx -lz -o $@

extract: extract.cpp util.cpp db.cpp pgmap.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppGzip/EncodeGzip.cpp
	g++ $^ -std=c++11 -Wall -lpqxx -lz -o $@

swigpy2: pgmap.i util.cpp db.cpp pgmap.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppGzip/EncodeGzip.cpp
	swig -python -c++ pgmap.i
	g++ -shared -fPIC -std=c++11 pgmap_wrap.cxx util.cpp db.cpp pgmap.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppGzip/EncodeGzip.cpp ${shell python2-config --includes --libs} -lpqxx -lz -o _pgmap.so

swigpy3: pgmap.i util.cpp db.cpp pgmap.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppGzip/EncodeGzip.cpp
	swig -python -py3 -c++ pgmap.i
	g++ -shared -fPIC -std=c++11 pgmap_wrap.cxx util.cpp db.cpp pgmap.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppGzip/EncodeGzip.cpp ${shell python3-config --includes --libs} -lpqxx -lz -g -o _pgmap.so

