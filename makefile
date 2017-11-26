

all: dump extract admin osm2csv swigpy2

%.co: %.c
	gcc -Wall -fPIC -c -o $@ $<
%.o: %.cpp
	g++ -Wall -fPIC -c -std=c++11 -o $@ $<

common = util.o dbquery.o dbids.o dbadmin.o dbcommon.o dbreplicate.o \
	dbdecode.o dbstore.o dbdump.o dbfilters.o dbchangeset.o dbjson.o pgmap.o cppo5m/o5m.o \
	cppo5m/varint.o cppo5m/OsmData.o cppo5m/osmxml.o cppo5m/iso8601lib/iso8601.co cppGzip/EncodeGzip.o

libs = -lboost_filesystem -lboost_system -lpqxx -lexpat -lz

dump: dump.o $(common)
	g++ $^ -std=c++11 -Wall $(libs) -o $@

extract: extract.o $(common)
	g++ $^ -std=c++11 -Wall $(libs) -o $@

admin: admin.o $(common)
	g++ $^ -std=c++11 -Wall $(libs) -o $@

osm2csv: osm2csv.o dbjson.o util.o cppo5m/o5m.o cppo5m/varint.o cppo5m/OsmData.o cppo5m/osmxml.o cppo5m/iso8601lib/iso8601.co cppGzip/DecodeGzip.o cppGzip/EncodeGzip.o 
	g++ $^ -std=c++11 -Wall $(libs) -o $@

swigpy2: pgmap.i $(common)
	swig -python -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC -std=c++11 -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python2-config --includes --libs} $(libs) -o _pgmap.so

swigpy3: pgmap.i $(common)
	swig -python -py3 -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC -std=c++11 -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python3-config --includes --libs} $(libs) -g -o _pgmap.so

