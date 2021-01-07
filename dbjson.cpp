#include <rapidjson/writer.h> //rapidjson-dev
#include <rapidjson/stringbuffer.h>
#include <rapidjson/document.h>
#include "dbjson.h"
using namespace std;

void EncodeTags(const TagMap &tagmap, string &out)
{
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartObject();
	for(auto it=tagmap.begin(); it!=tagmap.end(); it++)
	{
		writer.Key(it->first.c_str());
		writer.String(it->second.c_str());
	}
	writer.EndObject();
	out = buffer.GetString();
}

void EncodeInt64Vec(const std::vector<int64_t> &vals, string &out)
{
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartArray();
	for(size_t i=0; i<vals.size(); i++)
	{
		writer.Int64(vals[i]);
	}
	writer.EndArray();
	out = buffer.GetString();
}

void EncodeStringVec(const std::vector<std::string> &vals, string &out)
{
	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartArray();
	for(size_t i=0; i<vals.size(); i++)
	{
		writer.String(vals[i].c_str());
	}
	writer.EndArray();
	out = buffer.GetString();
}

void EncodeRelationMems(const std::vector<std::string> &objTypes, const std::vector<int64_t> &objIds, std::string &out)
{
	if(objTypes.size() != objIds.size())
		throw runtime_error("Input vector lengths must match.");

	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartArray();
	for(size_t i=0; i<objTypes.size(); i++)
	{
		writer.StartArray();
		writer.String(objTypes[i].c_str());
		writer.Int64(objIds[i]);
		writer.EndArray();
	}
	writer.EndArray();
	out = buffer.GetString();
}

void EncodeObjTypeIdVers(const std::vector<std::string> &types, 
	const std::vector<std::pair<int64_t, int64_t> > &idVers, 
	std::string &out)
{
	if(types.size() != idVers.size())
		throw runtime_error("Input vector lengths must match.");

	rapidjson::StringBuffer buffer;
	rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
	writer.StartArray();
	for(size_t i=0; i<types.size(); i++)
	{
		writer.StartArray();
		writer.String(types[i].c_str());
		writer.Int64(idVers[i].first);
		writer.Int64(idVers[i].second);
		writer.EndArray();
	}
	writer.EndArray();
	out = buffer.GetString();
}

void DecodeObjTypeIdVers(std::string &json,
	std::vector<std::string> &typesOut, 
	std::vector<std::pair<int64_t, int64_t> > &idVersOut)
{
	rapidjson::Document doc;
	doc.Parse(json.c_str());

	for (rapidjson::Value::ConstValueIterator itr = doc.Begin(); itr != doc.End(); ++itr)
	{
		const rapidjson::Value& a = *itr;
		if (!a.IsArray()) continue;
		if (a.Size() < 3) continue;

		typesOut.push_back(a[0].GetString());
		std::pair<int64_t, int64_t> idVer(a[1].GetInt64(), a[2].GetInt64());
		idVersOut.push_back(idVer);
	}
}
