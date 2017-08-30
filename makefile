all:
	g++ dump.cpp util.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppGzip/EncodeGzip.cpp -Wall -lpqxx -lz -o dump
