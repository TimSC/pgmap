#ifndef _OSMFILE_H
#define _OSMFILE_H

#include <fstream>
#include <memory>
#include <string>
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/io.h"

///Writes map data to a file whose name selects the format: .osm, .o5m or .pbf,
///each optionally followed by .gz for gzip compression.
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

	///xmlAttribs become attributes of the root element of XML output and are
	///ignored for other formats. Throws if the file cannot be created.
	OsmFileWriter(const std::string &destination, const TagMap &xmlAttribs = TagMap());
	~OsmFileWriter();
	OsmFileWriter(const OsmFileWriter &) = delete;
	OsmFileWriter &operator=(const OsmFileWriter &) = delete;

	///Send the map data here. It must be sent Finish before Close is called.
	std::shared_ptr<IDataStreamHandler> Encoder();

	///Completes the file and moves it into place. Throws if it cannot be written.
	void Close();
};

#endif //_OSMFILE_H
