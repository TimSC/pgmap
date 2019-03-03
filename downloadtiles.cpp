#include <fstream>
#include <iostream>
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/OsmData.h"
#include "cppo5m/osmxml.h"
#include "pgmap.h"
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
namespace po = boost::program_options;
namespace fs = boost::filesystem;

class OutputFileAndEncoder
{
public:
	shared_ptr<IDataStreamHandler> enc;
	EncodeGzip *gzipEnc;
	std::filebuf outfi;

	OutputFileAndEncoder()
	{
		gzipEnc = nullptr;
	}

	virtual ~OutputFileAndEncoder()
	{
		if(gzipEnc != nullptr)
			delete gzipEnc;
		gzipEnc = nullptr;
		outfi.close();
	}
};

shared_ptr<OutputFileAndEncoder> CreateOutEncoder(const::string &extension, fs::path pth, int zoom, int tilex, int tiley)
{
	shared_ptr<OutputFileAndEncoder> out(new OutputFileAndEncoder());
	string outFina = to_string(tiley)+extension;
	fs::path outPth = pth / outFina;

	vector<string> outFinaSp = split(outFina, '.');
	if(outFinaSp.size() < 3)
	{
		cerr << "Output file name does not have a recognized extension" << endl;
		exit(-2);
	}

	if(outFinaSp[outFinaSp.size()-1] == "gz" && outFinaSp[outFinaSp.size()-2] == "o5m")
	{
		out->outfi.open(outPth.string(), std::ios::out);
		out->gzipEnc = new class EncodeGzip(out->outfi);

		out->enc.reset(new O5mEncode(*out->gzipEnc));
	}
	else if(outFinaSp[outFinaSp.size()-1] == "gz" && outFinaSp[outFinaSp.size()-2] == "osm")
	{
		out->outfi.open(outPth.string(), std::ios::out);
		out->gzipEnc = new class EncodeGzip(out->outfi);

		TagMap empty;
		out->enc.reset(new OsmXmlEncode(*out->gzipEnc, empty));
	}
	else
	{
		cerr << "Output file name does not have a recognized extension" << endl;
		exit(-2);
	}
	return out;
}

int main(int argc, char **argv)
{	
	// Declare the supported options.
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "produce help message")
		("bbox", po::value<string>(), "bbox shape (e.g -1.078,50.788,-1.074,50.790)")
		("preset", po::value<string>(), "preset area name")
		("zoom", po::value<int>()->default_value(12), "zoom (e.g. 12)")
		("extension", po::value<string>()->default_value(".osm.gz"), "output file extension")
	;

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
		cout << desc << "\n";
		return 1;
	}

	int zoom = vm["zoom"].as<int>();
	int minx = 0, maxx = 0, miny = 0, maxy = 0;
	bool tile_range_specified = false;

	vector<double> bbox;
	if (vm.count("bbox"))
	{
		vector<string> bboxvals = split(vm["bbox"].as<string>(), ',');
		for(size_t i=0; i < bboxvals.size(); i++)
		{
			bbox.push_back(atof(bboxvals[i].c_str()));
		}
		if(bbox.size() != 4)
		{
			cerr << "Bbox must have 4 numbers" << endl;
			exit(-1);
		}

		minx = floor(long2tilex(bbox[0], zoom));
		maxx = floor(long2tilex(bbox[2], zoom));
		miny = floor(lat2tiley(bbox[1], zoom));
		maxy = floor(lat2tiley(bbox[3], zoom));
		tile_range_specified = true;
	}

	if (vm.count("preset"))
	{
		string preset = vm["preset"].as<string>();
		if (preset == "planet")
		{
			minx = 0;
			maxx = pow(2, zoom)-1;
			miny = 0;
			maxy = maxx;
			tile_range_specified = true;			
		}
	}

	if(not tile_range_specified)
	{
		cerr << "Bbox or preset must be specified" << endl;
		exit(-2);
	}
	cout << "Tile bbox " << minx << "," << miny << "," << maxx << "," << maxy << endl;

	cout << "Reading settings from config.cfg" << endl;
	std::map<string, string> config;
	ReadSettingsFile("config.cfg", config);
	
	string cstr = GeneratePgConnectionString(config);
	
	class PgMap pgMap(cstr, config["dbtableprefix"], config["dbtablemodifyprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);

	if (pgMap.Ready()) {
		cout << "Opened database successfully" << endl;
	} else {
		cout << "Can't open database" << endl;
		return 1;
	}

	std::shared_ptr<class PgTransaction> transaction = pgMap.GetTransaction("ACCESS SHARE");

	fs::path pth(to_string(zoom));
	if(not fs::exists(pth))
		create_directory(pth);

	for(int x=minx; x <= maxx; x++)
	{
		fs::path pth2 = pth / to_string(x);
		if(not fs::exists(pth2))
			create_directory(pth2);

		for(int y=miny; y <= maxy; y++)
		{
			cout << "Tile " << x << "," << y << endl;

			shared_ptr<OutputFileAndEncoder> outputFileAndEncoder = CreateOutEncoder(vm["extension"].as<string>(), pth2, zoom, x, y);

			vector<double> tileBbox = {tilex2long(x, zoom), tiley2lat(y, zoom), tilex2long(x+1, zoom), tiley2lat(y+1, zoom)};

			std::shared_ptr<class PgMapQuery> mapQuery = transaction->GetQueryMgr();
			int ret = 0;
			ret = mapQuery->Start(tileBbox, outputFileAndEncoder->enc);
			while(ret == 0)
			{
				ret = mapQuery->Continue();
			}
		}
	}

	return 0;
}

