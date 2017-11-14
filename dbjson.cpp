#include <rapidjson/writer.h> //rapidjson-dev
#include <rapidjson/stringbuffer.h>
#include "dbjson.h"

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
