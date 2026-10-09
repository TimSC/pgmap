#include "osmfile.h"
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <vector>
#include <unistd.h>
using namespace std;

static bool EndsWith(const std::string &text, const std::string &ending)
{
	return text.size() > ending.size() &&
		text.compare(text.size() - ending.size(), ending.size(), ending) == 0;
}

const char *OsmFileWriter::SupportedNames()
{
	return ".osm, .o5m or .pbf, optionally followed by .gz";
}

OsmFormat OsmFileWriter::FormatOf(const std::string &filename)
{
	std::string name = filename;
	if(EndsWith(name, ".gz"))
		name.resize(name.size() - 3);

	if(EndsWith(name, ".osm"))
		return OsmFormat::OsmXml;
	if(EndsWith(name, ".o5m"))
		return OsmFormat::O5m;
	if(EndsWith(name, ".pbf"))
		return OsmFormat::Pbf;
	throw invalid_argument(std::string("Output file name must end in ") + SupportedNames());
}

OsmFileWriter::OsmFileWriter(const std::string &destinationIn, const TagMap &xmlAttribs) :
	destination(destinationIn)
{
	OsmFormat format = FormatOf(destination);

	std::string pattern = destination + ".tmp.XXXXXX";
	std::vector<char> path(pattern.begin(), pattern.end());
	path.push_back('\0');
	int descriptor = mkstemp(path.data());
	if(descriptor < 0)
		throw runtime_error("Cannot create output file " + destination);
	close(descriptor);
	temporary = path.data();

	if(!file.open(temporary, std::ios::out | std::ios::binary))
	{
		remove(temporary.c_str());
		throw runtime_error("Cannot open output file " + destination);
	}

	std::streambuf *stream = &file;
	if(EndsWith(destination, ".gz"))
	{
		gzip.reset(new EncodeGzip(file));
		stream = gzip.get();
	}
	encoder = MakeEncoder(format, *stream, xmlAttribs);
}

OsmFileWriter::~OsmFileWriter()
{
	//Not closed, so the write did not complete: discard it
	encoder.reset();
	gzip.reset();
	if(file.is_open())
		file.close();
	if(!temporary.empty())
		remove(temporary.c_str());
}

std::shared_ptr<IDataStreamHandler> OsmFileWriter::Encoder()
{
	return encoder;
}

void OsmFileWriter::Close()
{
	if(temporary.empty())
		return;

	//The gzip stream writes its remaining data when destroyed
	encoder.reset();
	gzip.reset();
	if(file.close() == nullptr)
		throw runtime_error("Failed to finish writing " + destination);
	if(rename(temporary.c_str(), destination.c_str()) != 0)
		throw runtime_error("Failed to move output into place at " + destination);
	temporary.clear();
}
