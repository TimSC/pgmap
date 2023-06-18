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

class OutputFileAndEncoder : public IDataStreamHandler
{
public:
	EncodeGzip *gzipEnc;
	std::filebuf outfi;

	OutputFileAndEncoder() : IDataStreamHandler()
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

	virtual bool StoreIsDiff(bool) {
		cout << "d" << endl;
		return false;
	}

	virtual bool StoreBounds(double x1, double y1, double x2, double y2) {
		cout << "b" << endl;
		return false;
	}

	virtual bool StoreBbox(const std::vector<double> &bbox) {
		if (bbox.size() == 4)
			cout << "bbox " << bbox[0] << "," << bbox[1] << "," << bbox[2] << "," << bbox[3] << endl;
		else
			cout << "null bbox" << endl;
		return false;
	}

	virtual bool StoreNode(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, double lat, double lon) {
		//cout << "n" << endl;
		return false;
	}

	virtual bool StoreWay(int64_t objId, const class MetaData &metaData, 
		const TagMap &tags, const std::vector<int64_t> &refs) {
		cout << "w" << endl;
		return false;
	}

	virtual bool StoreRelation(int64_t objId, const class MetaData &metaData, const TagMap &tags, 
		const std::vector<std::string> &refTypeStrs, const std::vector<int64_t> &refIds, 
		const std::vector<std::string> &refRoles) {
		cout << "r" << endl;
		return false;
	}
};

shared_ptr<OutputFileAndEncoder> CreateOutEncoder(fs::path pth, int zoom, int tilex, int tiley)
{
	shared_ptr<OutputFileAndEncoder> out(new OutputFileAndEncoder());
	string outFina = to_string(tiley)+".dbx";
	fs::path outPth = pth / outFina;

	out->outfi.open(outPth.string(), std::ios::out);
	out->gzipEnc = new class EncodeGzip(out->outfi);

	return out;
}

int main(int argc, char **argv)
{	
	// Declare the supported options.
	po::options_description desc("Allowed options");
	desc.add_options()
		("help", "produce help message")
		("bbox", po::value<string>(), "bbox shape (e.g -1.078,50.788,-1.074,50.790)")
		("tiles", po::value<string>(), "tile range")
		("preset", po::value<string>(), "preset area name")
		("zoom", po::value<int>()->default_value(12), "zoom (e.g. 12)")
		("out", po::value<string>()->default_value("."), "path to output")
	;
	bool skipExisting = true;

	po::variables_map vm;
	po::store(po::parse_command_line(argc, argv, desc), vm);
	po::notify(vm);

	if (vm.count("help")) {
		cout << desc << "\n";
		return 1;
	}

	string outPath = vm["out"].as<string>();
	if(not fs::exists(outPath))
	{
		cout << "Output path does not exist\n";
		return -1;
	}

	int zoom = vm["zoom"].as<int>();
	int minx = 0, maxx = 0, miny = 0, maxy = 0;
	bool tile_range_specified = false;

	if (vm.count("bbox"))
	{
		vector<double> bbox;
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
		miny = floor(lat2tiley(bbox[3], zoom));
		maxy = floor(lat2tiley(bbox[1], zoom));
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

	if (vm.count("tiles"))
	{
		vector<double> tileBbox;
		vector<string> tileRange = split(vm["tiles"].as<string>(), ',');
		for(size_t i=0; i < tileRange.size(); i++)
		{
			tileBbox.push_back(atoi(tileRange[i].c_str()));
		}
		if(tileBbox.size() != 4)
		{
			cerr << "Tile range must have 4 numbers" << endl;
			exit(-1);
		}

		minx = tileBbox[0];
		maxx = tileBbox[2];
		miny = tileBbox[1];
		maxy = tileBbox[3];
		tile_range_specified = true;
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

	fs::path pth = fs::path(outPath) / to_string(zoom);
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
			string outFina = to_string(y)+".dbm";
			fs::path outPth = pth2 / outFina;
			if(skipExisting and fs::exists(outPth))
				continue;

			shared_ptr<IDataStreamHandler> outputFileAndEncoder = CreateOutEncoder(pth2, zoom, x, y);

			vector<double> tileBbox = {tilex2long(x, zoom), tiley2lat(y, zoom), tilex2long(x+1, zoom), tiley2lat(y+1, zoom)};

			std::shared_ptr<class PgMapQuery> mapQuery = transaction->GetQueryMgr();
			int ret = 0;
			int64_t timestamp = 0;
			ret = mapQuery->Start(tileBbox, timestamp, outputFileAndEncoder);
			while(ret == 0)
			{
				ret = mapQuery->Continue();
			}
		}
	}

	return 0;
}

