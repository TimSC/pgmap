
dump: dump.cpp util.cpp common.cpp cppo5m/o5m.cpp cppo5m/varint.cpp cppGzip/EncodeGzip.cpp
	g++ $^ -std=c++11 -Wall -lpqxx -lz -o $@

