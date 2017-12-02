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

	std::vector<char> buffer(length);
	file.read(&buffer[0],length);

	contentOut.assign(&buffer[0], length);
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

