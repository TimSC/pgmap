cppflags= -std=c++11 -Wall

all: dump extract admin applydiffs osm2csv checkdata

%.co: %.c %.h
	gcc -Wall -fPIC -c -o $@ $<

%.o: %.cpp %.h
	g++ $(cppflags) -fPIC -c -o $@ $<

common = util.o dbquery.o dbids.o dbadmin.o dbcommon.o dbreplicate.o \
	dbdecode.o dbstore.o dbdump.o dbfilters.o dbchangeset.o dbjson.o dbmeta.o dbusername.o \
	dboverpass.o dbeditactivity.o pgcommon.o pgmap.o \
	cppo5m/o5m.o cppo5m/varint.o cppo5m/OsmData.o cppo5m/osmxml.o \
	cppo5m/utils.o cppo5m/pbf.o cppo5m/pbf/fileformat.pb.cc cppo5m/pbf/osmformat.pb.cc\
	cppo5m/iso8601lib/iso8601.co cppGzip/EncodeGzip.o cppGzip/DecodeGzip.o

osmdata = cppo5m/o5m.o cppo5m/varint.o cppo5m/OsmData.o cppo5m/osmxml.o \
	cppo5m/utils.o cppo5m/pbf.o cppo5m/pbf/fileformat.pb.cc cppo5m/pbf/osmformat.pb.cc\
	cppo5m/iso8601lib/iso8601.co cppGzip/DecodeGzip.o cppGzip/EncodeGzip.o

libs = -lboost_filesystem -lboost_program_options -lboost_system -lprotobuf -lboost_iostreams -lpqxx -lexpat -lz

dump: dump.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

extract: extract.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

downloadtiles: downloadtiles.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

admin: admin.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

applydiffs: applydiffs.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

osm2csv: osm2csv.cpp dbjson.o util.o $(osmdata) 
	g++ $^ $(cppflags) $(libs) -o $@

checkdata: checkdata.cpp dbjson.o util.o $(osmdata) $(common) 
	g++ $^ $(cppflags) $(libs) -o $@

swigpy2: pgmap.i $(common)
	swig -python -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC $(cppflags) -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python2-config --includes --libs} $(libs) -o _pgmap.so

swigpy3: pgmap.i $(common)
	swig -python -py3 -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC $(cppflags) -DPYTHON_AWARE pgmap_wrap.cxx $(common) ${shell python3-config --includes --libs} $(libs) -o _pgmap.so

quickinit: quickinit.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

clean:
	rm *.o admin dump extract applydiffs osm2csv checkdata quickinit

