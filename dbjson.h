#ifndef _DB_JSON_H
#define _DB_JSON_H

#include "cppo5m/o5m.h"
#include <string>

void EncodeTags(const TagMap &tagmap, std::string &out);
void EncodeInt64Vec(const std::vector<int64_t> &vals, std::string &out);
void EncodeStringVec(const std::vector<std::string> &vals, std::string &out);
void EncodeRelationMems(const std::vector<std::string> &objTypes, const std::vector<int64_t> &objIds, std::string &out);

#endif //_DB_JSON_H
