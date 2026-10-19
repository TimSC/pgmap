#include <fstream>
#include <chrono>
#include <cstdio>
#include <algorithm>
#include <sstream>
#include <iostream>
#include "pgmap.h"
#include "dbadmin.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/osmxml.h"
#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

//Based on https://gist.github.com/vivithemage/9517678#gistcomment-1658468

std::vector<std::string> get_file_list(const std::string& path)
{
	std::vector<std::string> m_file_list;
    if (!path.empty())
    {
        fs::path apk_path(path);
        fs::recursive_directory_iterator end;

        for (fs::recursive_directory_iterator i(apk_path); i != end; ++i)
        {
            const fs::path cp = (*i);
            m_file_list.push_back(cp.string());
        }
    }
    return m_file_list;
}

// Reading answers. Each reads one word; at the end of input (Ctrl-D) the
// tool stops, where before it went round the menu for ever.

static bool ReadWord(std::string &out)
{
	if(cin >> out) return true;
	cout << endl << "No more input." << endl;
	exit(0);
}

// A whole number, or false with a message for anything else: a stray letter
// used to be read as 0, which for a schema version drops every table
static bool ReadNumber(const std::string &prompt, int &out)
{
	cout << prompt << endl;
	std::string text;
	ReadWord(text);
	size_t start = (!text.empty() && text[0] == '-') ? 1 : 0;
	if(text.size() <= start || text.size() > 9 || text.find_first_not_of("0123456789", start) != std::string::npos)
	{
		cout << "\"" << text << "\" is not a number. Nothing was changed." << endl;
		return false;
	}
	out = atoi(text.c_str());
	return true;
}

// A yes or no question, answered 1 or 0 (or y or n)
static bool ReadYesNo(const std::string &prompt, bool &out)
{
	cout << prompt << " (1/0)" << endl;
	std::string text;
	ReadWord(text);
	if(text == "1" || text == "y" || text == "yes") { out = true; return true; }
	if(text == "0" || text == "n" || text == "no") { out = false; return true; }
	cout << "\"" << text << "\" is not 1 or 0. Nothing was changed." << endl;
	return false;
}

// For a step that destroys data: says what it will do, and goes ahead only
// if the given word is typed back
static bool Confirm(const std::string &consequence, const std::string &word)
{
	cout << consequence << endl << "Type " << word << " to go ahead, or anything else to stop:" << endl;
	std::string text;
	ReadWord(text);
	if(text == word) return true;
	cout << "Nothing was changed." << endl;
	return false;
}

// How long something took, as a person reads it: 4.2s, 12m 05s, 3h 07m 10s
int main(int argc, char **argv)
{	
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

	bool running = true;
	string inputStr;
	class PgMapError errStr;
	while(running)
	{
		// Say so if the indices are behind what this version of the tool builds
		try
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			string notes = admin->GetIndexVersionNotes();
			if(!notes.empty())
				cout << endl << notes;
		}
		catch(const std::exception &) {} // As before the tables exist
		cout << endl;


		cout << "1. Drop tables" << endl;
		cout << "2. Create tables" << endl;
		cout << "3. Import map data (from the file named by dump_path)" << endl;
		cout << "4. Create indicies" << endl;
		cout << "5. Apply diffs (optional)" << endl;
		cout << "6. Refresh max IDs" << endl;
		cout << "7. Import changeset metadata (optional)" << endl;
		cout << "8. Refresh max changeset IDs and UIDs" << endl;
		cout << "9. Build username table (optional)" << endl << endl;
		cout << "a. Reset active tables (this will delete all edits after the import)" << endl;
		cout << "b. Reset test tables" << endl;
		cout << "c. Check nodes exist for ways" << endl;
		cout << "d. Check object ID tables" << endl;
		cout << "e. Update way/relation bboxes" << endl;
		cout << "g. Upgrade/downgrade db schema" << endl;
		cout << "h. Upgrade/downgrade index version" << endl;
		cout << "i. Create/drop bbox indices" << endl;
		cout << "j. Add/drop way and relation shapes (for map renderers)" << endl;

		cout << endl << "q. Quit" << endl;

		ReadWord(inputStr);
		// For saying how long the chosen task took
		auto started = std::chrono::steady_clock::now();
		int verbose = 1;

		if(inputStr == "1")
		{
			if(!Confirm("This drops every map table of the " + config["dbtableprefix"] + ", " +
				config["dbtablemodifyprefix"] + " and " + config["dbtabletestprefix"] + " table sets: the imported "
				"map, every edit since, the edit activity and the stored extracts. It cannot be undone.", "drop"))
				continue;

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->DropMapTables(verbose, errStr);

			if(ok)
				cout << "All done, in " << Took(started) << "." << endl;
			else
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
			continue;
		}

		if(inputStr == "2")
		{

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CreateMapTables(verbose, 0, true, errStr);

			if(ok)
				cout << "All done, in " << Took(started) << "." << endl;
			else
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
			continue;
		}

		if(inputStr == "3")
		{
			cout << "Importing " << config["dump_path"] << endl;
			// In one transaction: the import then happens entirely or not at
			// all, and the database can skip its write-ahead log for tables
			// that start empty (see DbImportMapFile)
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("");
			bool ok = admin->ImportMapFile(verbose, config["dump_path"], errStr);

			if(ok)
			{
				admin->Commit();
				cout << "All done, in " << Took(started) << "." << endl;
			}
			else
			{
				admin->Abort();
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
				cout << "Nothing was imported." << endl;
			}
			continue;
		}

		if(inputStr == "4")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CreateMapIndices(verbose, errStr);

			if(ok)
				cout << "All done, in " << Took(started) << "." << endl;
			else
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
			continue;
		}

		if(inputStr == "5")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("EXCLUSIVE");
			bool ok = admin->ApplyDiffs(config["diffs_path"], verbose, errStr);

			if(ok)
			{
				admin->Commit();
				cout << "All done, in " << Took(started) << "." << endl;
				cout << "Now update next object IDs, or bad things might happen!" << endl;
			}
			else
			{
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
				admin->Abort();
			}
			continue;
		}

		if(inputStr == "6")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("EXCLUSIVE");
			bool ok = admin->RefreshMapIds(verbose, errStr);

			if(ok)
			{
				admin->Commit();
				cout << "All done, in " << Took(started) << "." << endl;
			}
			else
			{
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
				admin->Abort();
			}
			continue;
		}

		if(inputStr == "7")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("EXCLUSIVE");

			cout << "Reading files in " << config["changesets_import_path"] << endl;
			
			fs::path p(config["changesets_import_path"]);
			bool ok = true;
			if(is_directory(p))
			{
				std::vector<std::string> finaList = get_file_list(config["changesets_import_path"]);
				sort(finaList.begin(), finaList.end());

				for(size_t i=0; i<finaList.size(); i++)
				{
					fs::path p2(finaList[i]);
					if(is_directory(p2)) continue;
					ok = admin->ImportChangesetMetadata(finaList[i], verbose, errStr);
					if(!ok)
					{
						cout << "Could not import " << finaList[i] << endl;
						break;
					}
				}
			}
			else
			{
				ok = admin->ImportChangesetMetadata(config["changesets_import_path"], verbose, errStr);
			}

			if(ok)
			{
				admin->Commit();
				cout << "All done, in " << Took(started) << "." << endl;
			}
			else
			{
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
				admin->Abort();
			}
			continue;

		}

		if(inputStr == "8")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("EXCLUSIVE");
			bool ok = admin->RefreshMaxChangesetUid(verbose, errStr);

			if(ok)
			{
				admin->Commit();
				cout << "All done, in " << Took(started) << "." << endl;
			}
			else
			{
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
				admin->Abort();
			}
			continue;
		}

		if(inputStr == "9")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("EXCLUSIVE");
			bool ok = admin->GenerateUsernameTable(verbose, errStr);

			if(ok)
			{
				admin->Commit();
				cout << "All done, in " << Took(started) << "." << endl;
			}
			else
			{
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
				admin->Abort();
			}
			continue;
		}

		if(inputStr == "a")
		{
			if(!Confirm("This deletes every edit made since the map was imported, from the " +
				config["dbtablemodifyprefix"] + " tables, with the changesets and edit activity. It cannot be undone.", "reset"))
				continue;
			std::shared_ptr<class PgTransaction> pgTransaction = pgMap.GetTransaction("EXCLUSIVE");
			bool ok = pgTransaction->ResetActiveTables(errStr);

			if(ok)
			{
				cout << "All done, in " << Took(started) << "." << endl;
				pgTransaction->Commit();
			}
			else
			{
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
				pgTransaction->Abort();
			}
			continue;
		}

		if(inputStr == "b")
		{
			if(!Confirm("This deletes everything in the " + config["dbtabletestprefix"] + " test tables.", "reset"))
				continue;
			class PgMap pgMap2(cstr, config["dbtableprefix"], config["dbtabletestprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);
			std::shared_ptr<class PgTransaction> pgTransaction = pgMap2.GetTransaction("EXCLUSIVE");
			bool ok = pgTransaction->ResetActiveTables(errStr);

			if(ok)
			{
				cout << "All done, in " << Took(started) << "." << endl;
				pgTransaction->Commit();
			}
			else
			{
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
				pgTransaction->Abort();
			}
			continue;
		}

		if(inputStr == "c")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("ACCESS SHARE");
			bool ok = admin->CheckNodesExistForWays(errStr);

			cout << (ok ? "All done, in " : "Failed after ") << Took(started) << "." << endl;
			admin->Commit();

			continue;
		}

		if(inputStr == "d")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("ACCESS SHARE");
			bool ok = admin->CheckObjectIdTables(errStr);

			cout << (ok ? "All done, in " : "Failed after ") << Took(started) << "." << endl;
			admin->Commit();

			continue;
		}

		if(inputStr == "e")
		{
			bool updateStatic = false, updateActive = false, onlyMissing = false;
			if(!ReadYesNo("Update static tables?", updateStatic)) continue;
			if(!ReadYesNo("Update active tables?", updateActive)) continue;
			if(!ReadYesNo("Only ways that have no box yet, as when taking up a run that was stopped?", onlyMissing)) continue;

			// Without a transaction around the whole of it, so that the ways
			// are committed a batch at a time and a stopped run keeps what it did
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = false;
			try
			{
				ok = admin->UpdateBboxes(verbose, updateStatic, updateActive, errStr, onlyMissing);
			}
			catch(const std::exception &e)
			{
				errStr.errStr = e.what();
			}

			if(ok)
				cout << "All done, in " << Took(started) << "." << endl;
			else
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;

			continue;
		}

		if(inputStr == "g")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			std::vector<int> current = admin->GetSchemaVersions();
			cout << "Schema versions now (static, modify, test): " << current[0] << ", " << current[1] << ", "
				<< current[2] << endl;
			int target = 0;
			stringstream prompt;
			prompt << "Schema version? (the latest is " << MAP_SCHEMA_VERSION << ")";
			if(!ReadNumber(prompt.str(), target)) continue;
			if(target < 0 || target > MAP_SCHEMA_VERSION || (target > 0 && target < 11))
			{
				cout << "There is no schema version " << target << ": give one from 11 to " << MAP_SCHEMA_VERSION <<
					", or 0. Nothing was changed." << endl;
				continue;
			}
			// Going down a version takes away what it added, data included
			int highest = *std::max_element(current.begin(), current.end());
			if(target < highest)
			{
				string consequence = "Downgrading takes away what the later versions added, and what is stored in it: "
					"from version 14 down, the edit activity and stored extracts.";
				if(target == 0)
					consequence = "Version 0 is no tables at all: this drops every map table, the imported map and "
						"every edit included. It cannot be undone.";
				if(!Confirm(consequence, "downgrade"))
					continue;
			}

			bool ok = admin->CreateMapTables(verbose, target, false, errStr);
			admin->Commit();

			if(ok)
				cout << "All done, in " << Took(started) << "." << endl;
			else
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
			continue;
		}

		if(inputStr == "h")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			std::vector<int> versions = admin->GetIndexVersions();
			cout << "Index versions now (static, modify, test): " << versions[0] << ", " << versions[1] << ", "
				<< versions[2] << endl;
			int target = 0;
			stringstream prompt;
			prompt << "Index version? (the latest is " << MAP_INDEX_VERSION << "; 0 drops every index and key "
				"that \"Create indicies\" builds)";
			if(!ReadNumber(prompt.str(), target)) continue;
			if(target == 0 && !Confirm("Without its indices the map is very slow to query and to edit until they "
				"are built again.", "drop"))
				continue;
			// Without a transaction around the whole of it: each index is kept
			// as it is built, and a run that was stopped is taken up by running
			// it again
			bool ok = false;
			try
			{
				ok = admin->SetIndexVersion(verbose, target, errStr);
			}
			catch(const std::exception &e)
			{
				errStr.errStr = e.what();
			}

			if(ok)
				cout << "All done, in " << Took(started) << "." << endl;
			else
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
			continue;
		}

		if(inputStr == "i")
		{
			cout << "Create or delete (c/d)?" << endl;
			std::string action;
			ReadWord(action);
			if(action != "c" && action != "d")
			{
				cout << "\"" << action << "\" is not c or d. Nothing was changed." << endl;
				continue;
			}
			bool ok = true;

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			if(action == "c")
				ok = admin->CreateBboxIndices(verbose, errStr);
			else if(action == "d") 
				ok = admin->DropBboxIndices(verbose, errStr);
			admin->Commit();

			if(ok)
				cout << "All done, in " << Took(started) << "." << endl;
			else
				cout << errStr.errStr << endl << "Stopped after " << Took(started) << "." << endl;
			continue;
		}

		if(inputStr == "j")
		{
			cout << "Ways and relations can keep their shapes, as lines and areas, for map renderers. "
				"Edits keep them up to date, and \"Update way/relation bboxes\" (e) fills them in." << endl;
			cout << "Add or drop (a/d)?" << endl;
			std::string action;
			ReadWord(action);
			if(action != "a" && action != "d")
			{
				cout << "\"" << action << "\" is not a or d. Nothing was changed." << endl;
				continue;
			}
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->SetObjectShapes(action == "a", verbose, errStr);
			if(ok)
			{
				admin->Commit();
				cout << "All done, in " << Took(started) << "." << endl;
				if(action == "a")
					cout << "Run \"Update way/relation bboxes\" (e) to work out the shapes of what is there." << endl;
			}
			else
			{
				admin->Abort();
				cout << errStr.errStr << endl << "Nothing was changed, after " << Took(started) << "." << endl;
			}
			continue;
		}

		if(inputStr == "q")
		{
			running = false;
			continue;
		}

	}

	return 0;
}

