#ifndef _DB_JSON_H
#define _DB_JSON_H

#include "cppo5m/o5m.h"
#include <string>

void EncodeTags(const TagMap &tagmap, std::string &out);
void EncodeInt64Vec(const std::vector<int64_t> &vals, std::string &out);
void EncodeStringVec(const std::vector<std::string> &vals, std::string &out);
void EncodeRelationMems(const std::vector<std::string> &objTypes, const std::vector<int64_t> &objIds, std::string &out);
void EncodeObjTypeIdVers(const std::vector<std::string> &types, 
	const std::vector<std::pair<int64_t, int64_t> > &idVers, 
	std::string &out);
void DecodeObjTypeIdVers(std::string &json,
	std::vector<std::string> &typesOut, 
	std::vector<std::pair<int64_t, int64_t> > &idVersOut);

#endif //_DB_JSON_H
