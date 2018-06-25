#include <fstream>
#include <sstream>
#include "util.h"
using namespace std;

int ReadFileContents(const char *filename, int binaryMode, std::string &contentOut)
{
	contentOut = "";
	std::ios_base::openmode mode = (std::ios_base::openmode)0;
	if(binaryMode)
		mode ^= std::ios::binary;
	std::ifstream file(filename, mode);
	if(!file) return 0;

	file.seekg(0,std::ios::end);
	std::streampos length = file.tellg();
	file.seekg(0,std::ios::beg);

	contentOut.resize(length);
	file.read(&contentOut[0],length);
	return 1;
}

//http://stackoverflow.com/a/236803/4288232
void split2(const string &s, char delim, vector<string> &elems) {
    stringstream ss;
    ss.str(s);
    string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

vector<string> split(const string &s, char delim) {
    vector<string> elems;
    split2(s, delim, elems);
    return elems;
}

// Inspired by PHP's implode and Python's join
string mergestr(const std::vector<string> &parts, const std::string &glue)
{
	stringstream ss;
	for(size_t i=0; i<parts.size(); i++)
	{
		if(i != 0)
			ss << glue;
		ss << parts[i];
	}
	return ss.str();
}

bool ReadSettingsFile(const std::string &settingsPath, std::map<std::string, std::string> &configOut)
{
	string configContent;
	ReadFileContents(settingsPath.c_str(), false, configContent);
	std::vector<std::string> lines = split(configContent, '\n');
	for(size_t i=0; i < lines.size(); i++)
	{
		const std::string &line = lines[i];
		std::vector<std::string> parts = split(line, ':');
		if (parts.size() < 2) continue;
		std::vector<std::string> partsSlice(++parts.begin(), parts.end());
		string value = mergestr(partsSlice, ":");
		configOut[parts[0]] = value;
	}
	return true;
}

void WriteSettingsFile(const std::string &settingsPath, const std::map<std::string, std::string> &config)
{
	std::ofstream outfile;
	outfile.open (settingsPath);
	for(auto it=config.begin(); it!=config.end(); it++)
	{
		outfile << it->first << ":" << it->second << endl;
	}

	outfile.close();
}

//https://stackoverflow.com/a/15372760/4288232
void StrReplaceAll( string &s, const string &search, const string &replace ) {
    for( size_t pos = 0; ; pos += replace.length() ) {
        // Locate the substring to replace
        pos = s.find( search, pos );
        if( pos == string::npos ) break;
        // Replace by erasing and inserting
        s.erase( pos, search.length() );
        s.insert( pos, replace );
    }
}

// Connection strings should have escaped quotes
// https://www.postgresql.org/docs/9.6/static/libpq-connect.html
std::string EscapeQuotes(std::string str)
{
	StrReplaceAll(str, "\"", "\\\"");
	StrReplaceAll(str, "'", "\\'");
	return str;
}

std::string GeneratePgConnectionString(std::map<std::string, std::string> config)
{
	std::stringstream ss;
	ss << "dbname=";
	ss << "'" << EscapeQuotes(config["dbname"]) << "'";
	ss << " user=";
	ss << "'" << EscapeQuotes(config["dbuser"]) << "'";
	ss << " password=";
	ss << "'" << EscapeQuotes(config["dbpass"]) << "'";
	ss << " hostaddr=";
	ss << "'" << EscapeQuotes(config["dbhost"]) << "'";
	ss << " port=";
	ss << "5432";
	return ss.str();
}

void LoadOsmFromFile(const std::string &filename, shared_ptr<class IDataStreamHandler> csvStore)
{
	vector<string> filenameSplit = split(filename, '.');
	size_t filePart = filenameSplit.size()-1;
	
	//Open file
	std::filebuf *fbRaw = new std::filebuf();
	fbRaw->open(filename, std::ios::in | std::ios::binary);
	if(!fbRaw->is_open())
	{
		cout << "Error opening input file " << filename << endl;
		exit(0);
	}

	shared_ptr<std::streambuf> fb(fbRaw);
	if(fb->in_avail() == 0)
	{
		cout << "Error reading from input file " << filename << endl;
		exit(0);
	}
	cout << "Reading from input " << filename << endl;

	shared_ptr<std::streambuf> fb2;	
	if(filenameSplit[filePart] == "gz")
	{
		//Perform gzip decoding
		fb2.reset(new class DecodeGzip(*fb.get()));
		if(fb2->in_avail() == 0)
		{
			cout << "Error reading from input file" << endl;
			exit(0);
		}
		filePart --;
	}
	else
	{
		fb2.swap(fb);
	}

	if(filenameSplit[filePart] == "o5m")
		LoadFromO5m(*fb2.get(), csvStore);
	else if (filenameSplit[filePart] == "osm")
		LoadFromOsmXml(*fb2.get(), csvStore);
	else
		throw runtime_error("File extension not supported");

	csvStore->Finish();
}

