
all: dump extract

dump: dump.cpp util.cpp common.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppGzip/EncodeGzip.cpp
	g++ $^ -std=c++11 -Wall -lpqxx -lz -o $@

extract: extract.cpp util.cpp common.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppGzip/EncodeGzip.cpp
	g++ $^ -std=c++11 -Wall -lpqxx -lz -o $@

