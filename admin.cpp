#include <fstream>
#include <iostream>
#include "pgmap.h"
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

		cout << "1. Drop tables" << endl;
		cout << "2. Create tables" << endl;
		cout << "3. Copy map data" << endl;
		cout << "4. Create indicies" << endl;
		cout << "5. Apply diffs" << endl;
		cout << "6. Refresh max IDs" << endl;
		cout << "7. Import changetset metadata" << endl;
		cout << "8. Refresh max changeset IDs and UIDs" << endl;
		cout << "9. Build username table" << endl << endl;
		cout << "a. Reset active tables (this will delete all edits after the import)" << endl;
		cout << "b. Reset test tables" << endl;
		cout << "c. Check nodes exist for ways" << endl;
		cout << "d. Check object ID tables" << endl;
		cout << "e. Update way/relation bboxes" << endl;
		cout << "g. Upgrade/downgrade db schema" << endl;
		cout << "h. Create/drop bbox indices" << endl;

		cout << endl << "q. Quit" << endl;

		cin >> inputStr;
		int verbose = 1;

		if(inputStr == "1")
		{

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->DropMapTables(verbose, errStr);
			//bool ok = admin->CopyMapData("/home/tim/dev/osm2pgcopy/portsmouth-", errStr);
			//bool ok = admin->CreateMapIndices(errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "2")
		{

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CreateMapTables(verbose, 0, true, errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "3")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CopyMapData(verbose, config["csv_absolute_path"], errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "4")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CreateMapIndices(verbose, errStr);

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "5")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("EXCLUSIVE");
			bool ok = admin->ApplyDiffs(config["diffs_path"], verbose, errStr);

			if(ok)
			{
				admin->Commit();
				cout << "All done!" << endl;
				cout << "Now update next object IDs, or bad things might happen!" << endl;
			}
			else
			{
				cout << errStr.errStr << endl;
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
				cout << "All done!" << endl;
			}
			else
			{
				cout << errStr.errStr << endl;
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
						cout << errStr.errStr << endl;
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
				cout << "All done!" << endl;
			}
			else
			{
				cout << errStr.errStr << endl;
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
				cout << "All done!" << endl;
			}
			else
			{
				cout << errStr.errStr << endl;
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
				cout << "All done!" << endl;
			}
			else
			{
				cout << errStr.errStr << endl;
				admin->Abort();
			}
			continue;
		}

		if(inputStr == "a")
		{
			std::shared_ptr<class PgTransaction> pgTransaction = pgMap.GetTransaction("EXCLUSIVE");
			bool ok = pgTransaction->ResetActiveTables(errStr);

			if(ok)
			{
				cout << "All done!" << endl;
				pgTransaction->Commit();
			}
			else
			{
				cout << errStr.errStr << endl;
				pgTransaction->Abort();
			}
			continue;
		}

		if(inputStr == "b")
		{
			class PgMap pgMap2(cstr, config["dbtableprefix"], config["dbtabletestprefix"], config["dbtablemodifyprefix"], config["dbtabletestprefix"]);
			std::shared_ptr<class PgTransaction> pgTransaction = pgMap2.GetTransaction("EXCLUSIVE");
			bool ok = pgTransaction->ResetActiveTables(errStr);

			if(ok)
			{
				cout << "All done!" << endl;
				pgTransaction->Commit();
			}
			else
			{
				cout << errStr.errStr << endl;
				pgTransaction->Abort();
			}
			continue;
		}

		if(inputStr == "c")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("ACCESS SHARE");
			bool ok = admin->CheckNodesExistForWays(errStr);

			cout << "All done!" << ok << endl;
			admin->Commit();

			continue;
		}

		if(inputStr == "d")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("ACCESS SHARE");
			bool ok = admin->CheckObjectIdTables(errStr);

			cout << "All done!" << ok << endl;
			admin->Commit();

			continue;
		}

		if(inputStr == "e")
		{
			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin("ACCESS SHARE");
			bool ok = admin->UpdateBboxes(verbose, errStr);

			cout << "All done!" << ok << endl;
			admin->Commit();

			continue;
		}


		if(inputStr == "g")
		{
			cout << "Schema version?" << endl;
			std::string schemaVer;
			cin >> schemaVer;

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			bool ok = admin->CreateMapTables(verbose, atoi(schemaVer.c_str()), false, errStr);
			admin->Commit();

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
			continue;
		}

		if(inputStr == "h")
		{
			cout << "Create or delete (c/d)?" << endl;
			std::string action;
			cin >> action;
			bool ok = true;

			std::shared_ptr<class PgAdmin> admin = pgMap.GetAdmin();
			if(action == "c")
				ok = admin->CreateBboxIndices(verbose, errStr);
			else if(action == "d") 
				ok = admin->DropBboxIndices(verbose, errStr);
			admin->Commit();

			if(ok)
				cout << "All done!" << endl;
			else
				cout << errStr.errStr << endl;
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

