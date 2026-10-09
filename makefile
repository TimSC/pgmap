cppflags= -std=c++17 -Wall -DPY_SSIZE_T_CLEAN

all: dump extract export_extract import_extract update_extract compare_extract admin applydiffs osm2csv checkdata

%.co: %.c %.h
	gcc -Wall -fPIC -c -o $@ $<

# cppo5m's classes are compiled into these objects, so a change to its headers
# must rebuild them: mixing old objects with a new library crashes at run time.
%.o: %.cpp %.h $(wildcard cppo5m/*.h)
	g++ $(cppflags) -fPIC -c -o $@ $<

# cppo5m is built by its own makefile as a static library
cppo5m = cppo5m/libcppo5m.a

cppo5m/libcppo5m.a: FORCE
	$(MAKE) -C cppo5m libcppo5m.a

FORCE:

common = util.o osmfile.o dbquery.o dbids.o dbadmin.o dbcommon.o dbreplicate.o \
	dbdecode.o dbextract.o dbstore.o dbdump.o dbfilters.o dbchangeset.o dbjson.o dbmeta.o dbusername.o \
	dboverpass.o dbeditactivity.o dbprepared.o pgcommon.o pgmap.o \
	$(cppo5m) cppGzip/EncodeGzip.o cppGzip/DecodeGzip.o

osmdata = $(cppo5m) cppGzip/DecodeGzip.o cppGzip/EncodeGzip.o

libs = -lboost_filesystem -lboost_program_options -lboost_system -lprotobuf -lpqxx -lexpat -lz

dump: dump.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

extract: extract.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

export_extract: export_extract.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

import_extract: import_extract.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

update_extract: update_extract.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

compare_extract: compare_extract.cpp $(common)
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
	g++ -shared -fPIC $(cppflags) -DPYTHON_AWARE -DPY_SSIZE_T_CLEAN pgmap_wrap.cxx cppo5m/pysink.cpp $(common) ${shell python2-config --includes --libs} $(libs) -o _pgmap.so

swigpy3: pgmap.i $(common)
	swig -python -py3 -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i
	g++ -shared -fPIC $(cppflags) -DPYTHON_AWARE -DPY_SSIZE_T_CLEAN pgmap_wrap.cxx cppo5m/pysink.cpp $(common) ${shell python3-config --includes --libs} $(libs) -o _pgmap.so

quickinit: quickinit.cpp $(common)
	g++ $^ $(cppflags) $(libs) -o $@

clean:
	rm *.o admin dump extract export_extract import_extract update_extract compare_extract applydiffs osm2csv checkdata quickinit

