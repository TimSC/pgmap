

all: dump extract swigpy2

common = util.cpp db.cpp pgmap.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppo5m/OsmData.cpp cppo5m/osmxml.cpp cppGzip/EncodeGzip.cpp

dump: dump.cpp $(common)
	g++ $^ -std=c++11 -Wall -lpqxx -lz -o $@

extract: extract.cpp $(common)
	g++ $^ -std=c++11 -Wall -lpqxx -lz -o $@

swigpy2: pgmap.i $(common)
	swig -python -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC -std=c++11 -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python2-config --includes --libs} -lpqxx -lz -o _pgmap.so

swigpy3: pgmap.i $(common)
	swig -python -py3 -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC -std=c++11 -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python3-config --includes --libs} -lpqxx -lz -g -o _pgmap.so

