#ifndef _OSMFILE_H
#define _OSMFILE_H

#include <fstream>
#include <memory>
#include <string>
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/io.h"

///Writes map data to a file whose name selects the format: .osm, .o5m, .pbf or
///.json, each optionally followed by .gz for gzip compression.
///
///The data goes to a temporary file beside the destination, which replaces the
///destination only when Close succeeds. A failed or abandoned write therefore
///leaves any existing file untouched.
class OsmFileWriter
{
private:
	std::string destination, temporary;
	std::filebuf file;
	std::unique_ptr<EncodeGzip> gzip;
	std::shared_ptr<OsmEncoder> encoder;

public:
	///Describes the accepted file names, for help and error messages.
	static const char *SupportedNames();
	///Returns the format a file name selects. Throws std::invalid_argument if
	///the name does not end in a supported extension.
	static OsmFormat FormatOf(const std::string &filename);

	///True if the format has somewhere to put header attributes: the root
	///element in XML, the top level object in JSON.
	static bool HasHeaderAttribs(OsmFormat format);

	///headerAttribs become attributes of the root element of XML output, or
	///string members of the top level object of JSON output, and are ignored
	///for other formats. Throws if the file cannot be created.
	OsmFileWriter(const std::string &destination, const TagMap &headerAttribs = TagMap());
	~OsmFileWriter();
	OsmFileWriter(const OsmFileWriter &) = delete;
	OsmFileWriter &operator=(const OsmFileWriter &) = delete;

	///Send the map data here. It must be sent Finish before Close is called.
	std::shared_ptr<IDataStreamHandler> Encoder();

	///Completes the file and moves it into place. Throws if it cannot be written.
	void Close();
};

#endif //_OSMFILE_H
