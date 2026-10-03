pgmap
=====

C++ SWIG module for accessing microcosm's postgis OSM map GIS schema. It also provides OSM xml and o5m encoding/decoding, because it is far faster than processing this in C++ than within native python. SWIG can produce a python module containing the functionality, as well as for several other languages. 

Installation
------------

### Installing with Python 3

To use this library using Python 3:

	sudo apt install libpqxx-dev rapidjson-dev libexpat1-dev python3-pip python3-dev swig libboost-filesystem-dev libboost-program-options-dev libprotobuf-dev zlib1g-dev libboost-iostreams-dev protobuf-compiler

If you have not already, create a virtual environment:

	sudo pip3 install virtualenv

	cd <src>

	virtualenv --python=/usr/bin/python3 pgmapenv3

	source pgmapenv3/bin/activate	

	sudo pip install --upgrade pip

Go to the <src>/pycrocosm/pgmap folder or clone a new copy:

	git clone https://github.com/TimSC/pgmap.git --recursive

	cd pgmap

You may or may need to rebuild to protobuf files:

	cd cppo5m

    protoc -I=proto proto/osmformat.proto --cpp_out=pbf

    protoc -I=proto proto/fileformat.proto --cpp_out=pbf

    cd ..

If you have not already, install the python module and build the tools:

	pip install .

	make

	cp config.cfg.template config.cfg

The SWIGWORDSIZE64 option in setup.py assumes you are using a 64 bit platform. See https://github.com/swig/swig/issues/568

Install and configure postgis
-----------------------------

On Linux Mint 18.* and Ubuntu Xenial:

    sudo apt install postgis postgresql postgresql-9.5-postgis-2.2

On Linux Mint 19.* and Ubuntu Bionic:

    sudo apt install postgis postgresql postgresql-10-postgis*

And on Ubuntu Focal:
	
	sudo apt install postgis postgresql postgresql-12-postgis-3

Postgresql can run faster if shared_buffers is increased in /etc/postgresql/9.5/main/postgresql.conf to about a quarter of total memory. (Having a large amount of memory wouldn't hurt either.) This is known to improve dump performance since the query has to interate over an index.

Create the database and user. Generate your own secret password (the colon character should not be used). The user postgres exists on many linux systems and is the default admin account for postgres.
    
	sudo su postgres

	psql

	CREATE DATABASE db_map;

	CREATE USER pycrocosm WITH PASSWORD 'myPassword';

	GRANT ALL PRIVILEGES ON DATABASE db_map to pycrocosm;

Disconnect using ctrl-D. As the user postgres, enable PostGIS on the database.

    psql --dbname=db_map

    ALTER USER pycrocosm WITH SUPERUSER;

	GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pycrocosm;

	CREATE EXTENSION postgis;

	CREATE EXTENSION postgis_topology;
	
Disconnect using ctrl-D (repeatedly) to get back to your normal user. Check you can connect using psql; it often works out of the box. 

    psql -h 127.0.0.1 -d db_map -U pycrocosm --password

If necessary, enable log in by password by changing pg_hba.conf as administrator (as described in https://stackoverflow.com/a/4328789/4288232 ). When connecting, use 127.0.0.1 rather than localhost, if the database is on the same machine (postgresql treats them differently). The exact path to pg_hga.conf depends on the version of postgresql you installed.

	locate pg_hba.conf

	sudo nano /etc/postgresql/9.5/main/pg_hba.conf

Update the pgmap config.cfg files with your new password. Use 127.0.0.1 rather than localhost, if the database is on the same machine.

	cp config.cfg.template config.cfg

	nano config.cfg

Add your database config info to config.cfg then check we can connect:

	./admin

Download a regional or planet map dump, for example this one: https://archive.org/details/uk-eire-fosm-2017-jan.o5m

The file format should be indicated by one of the supported extensions: .osm.gz .o5m.gz .osm .o5m

Set the dump_path variable in pgmap's config.cfg to the actual path of the data to import. Set the csv_absolute_path variable in config.cfg to a folder for temporary files (it is possible to use the pgmap source folder). To convert the data to csv format: 

    ./osm2csv

(CSV format files are used because they can be imported much faster than using conventional SQL.) Use the admin tool to create the tables, copy the csv files into the database, then create the indices.

    ./admin

You should do at least "Create tables", "Copy data" (skip if you want an empty database), "Create indicies", "Refresh max IDs", "Refresh max changeset IDs and UIDs" in order. Create indicies can take DAYS for a planet dump. Hopefully no errors occur. If you finish these steps, congratulations, you have successfully imported your map data! It might be prudent to remove superuser access for your database user, since it is no longer needed:

    sudo su postgres

    psql --dbname=db_map

    ALTER USER pycrocosm WITH NOSUPERUSER;

If you are attempting to configure pycrocosm, you can return to that README at this stage.

Database Design
---------------

Django has an integrated unit test system. This keeps seperate live ("mod") and test tables to avoid corrupting the live database. Although pgmap manages the map orientated tables rather than using Django, it keeps the Django concept of test and operational data unit tests run. The set of tables accessed by pgmap is called the "active" table set, which is either the "test" or "mod" table set, and determined when the main pgmap is initalized.

Another design choice, based on most of the map data is unchanging, is to maintain separate static map tables which don't change even when edits occur. This allows a lengthy map import to be done once and left unchanged. Unfortunately, this makes map query logic more complex, since both the static and active tables need to be checked for objects.

It is quite possibly to import data into the active table and leave the static tables empty.

All reads and writes occur with Postgresql transactions. This ensures database reads see a consistent version of the database, as well as making sure writes are atomic (they are entirely committed or entirely aborted).

Guide to source
---------------

* pgmap.h Public interface to library
* db*.h Low level SQL code to access database

Work in progress
----------------

* Fix expanding bbox on upload.
* Probably should add doxygen documentation (or more comments generally)



Atomic edit activity (schema version 14)
---------------------------------------

The XML endpoints `/replication/edit_activity/<id>` and
`/replication/edit_activities` expose `atomic_edit_id` and `block_index`
attributes for grouped rows. Their `sync_before` and `sync_after` elements
contain actual historical OSM objects resolved from type/ID/version references,
in reference order. Corresponding `bbox_before` and `bbox_after` elements hold
WKT with `format="wkt"` and `srid="4326"`. Unrecorded fields have `null="true"`;
known empty sync context is an empty element. Missing referenced history is an
error rather than a silently shortened list. Rebuild the bindings to expose the
additional C++ reader fields before running these endpoints.

The admin tool's "Drop tables" operation drops the known map tables, visible
views, and atomic-edit sequence directly using IF EXISTS. It does not run the
downgrade chain, so it also works with incomplete or mismatched schemas. It
removes static, mod, and test data, including dependent objects via CASCADE.
Use the separate schema upgrade/downgrade operation to retain map data.

Schema version 14 adds `atomic_edit_id BIGINT` and `block_index BIGINT` to
`edit_activity`, with a unique index on the pair. All activity records inserted
through one `PgTransaction` share an atomic edit ID; block positions start at
zero. Separate uploads in the same changeset receive separate IDs. The C++ and
SWIG `EditActivity` fields are `atomicEditId` and `blockIndex`.

Version 14 also adds `sync_before` and `sync_after` JSONB arrays, paired with
`bbox_before` and `bbox_after` geometry collections (SRID 4326). Each reference
contains an object type, ID, and version. Array entry i corresponds to geometry
component i+1; constraints enforce matching counts and require each pair to be
either fully NULL or fully populated. NULL means context has not been recorded;
an empty array paired with an empty collection means known empty context.
For node edits, the activity writer populates `sync_before` with old node
references and `bbox_before` with matching point geometries. Node creation
records an empty array and empty collection. Skipped if-unused deletions are
excluded. After-state and way/relation context are not populated yet.

These additions amend migration 13-to-14. Databases already marked version 14
will not rerun that migration and need the additional ALTER TABLE commands
applied separately; do not downgrade populated databases merely to add fields.

IDs are allocated lazily from a per-table-set sequence after acquiring the
exclusive map locks. Those locks remain held until commit or abort, ensuring
later writers cannot publish lower IDs after a consumer has advanced. Sequence
gaps after rollback are expected. Activity rows and object changes use the same
transaction; uploads abort if activity insertion fails.

Existing activity rows retain NULL in both new columns because historical
transaction boundaries cannot reliably be reconstructed from timestamps. The
C++ reader represents these as `atomicEditId = 0` and `blockIndex = -1`; they
must not be treated as one grouped edit.

Rebuild the pgmap library and use the admin tool's existing table creation/
upgrade operation with the latest schema before running the updated server.
The upgrade applies to static, mod, and test table sets. Downgrading to version
13 removes the grouping and sync columns and sequence and loses that information.
This change does not add a replication API or populate extract sync data.

Edit activity ID queries
-----------------------

The existing XML API supports:

* `/replication/edit_activity/123`: a single row (404 if missing).
* `/replication/edit_activities?id=123`: a single row as a collection.
* `/replication/edit_activities?first_id=100&last_id=150`: an inclusive row range.
* `/replication/edit_activities?first_id=100`: rows from 100 onwards.
* `/replication/edit_activities?atomic_edit_id=42`: all rows of atomic edit 42.

ID queries return rows ordered by row ID; an unmatched collection query returns
an empty collection. IDs must be positive integers. A row range can additionally
be restricted by atomic edit ID, but that may return only part of the group.
Row ranges can split atomic edits; use atomic-edit queries when a complete group
is required. Timestamp queries remain supported but cannot be combined with ID
filters. Rebuild pgmap bindings for the new QueryEditActivityByIds method.
