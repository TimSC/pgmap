#include <fstream>
#include <iostream>
#include "pgmap.h"
#include "util.h"
#include "cppGzip/EncodeGzip.h"
#include "cppo5m/osmxml.h"

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
		cout << "8. Refresh max changeset IDs and UIDs" << endl << endl;
		cout << "a. Reset active tables (this will delete all edits after the import)" << endl;
		cout << "b. Reset test tables" << endl;
		cout << "c. Check nodes exist for ways" << endl << endl;
		cout << "q. Quit" << endl;

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
			bool ok = admin->CreateMapTables(verbose, errStr);

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
			bool ok = admin->ImportChangesetMetadata(verbose, errStr);

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

			cout << "All done!" << endl;
			admin->Commit();

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

