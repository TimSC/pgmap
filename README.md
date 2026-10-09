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

Extract storage (schema version 14)
----------------------------------

Migration 13-to-14 creates the following tables under each configured table-set
prefix. There is one shared set of tables per prefix, supporting multiple
extracts by extract ID; each extract has one current state without an overlay.

* `extracts`: generated ID, optional name, rectangular bbox (Polygon, SRID 4326),
  query-mode flag `use_bbox_in_query`, `performed_at` timestamp, last applied
  `edit_activity_id`, and last applied `atomic_edit_id`. A NULL
  checkpoint denotes an extract whose initial snapshot is not established.
* `extract_livenodes`, `extract_liveways`, `extract_liverelations`: columns match
  the main live objects, with `extract_id` added. Primary keys are
  `(extract_id, id)`; tags/members/roles use JSONB, node coordinates use Point
  geometry, and ways/relations have bbox geometry. Spatial indexes are included.
* `extract_way_mems` and `extract_relation_mems_n/w/r`: current membership rows
  with extract ID, owner ID/version, index, and member ID. Reverse lookup indexes
  use `(extract_id, member)`. Deleting an owner removes its membership rows.

Deleting extract metadata cascades to that extract's objects and membership.
Relation members are not constrained to exist locally because map-query results
can contain incomplete relations. There are no extract history, overlay ID,
changeset, or edit activity tables. Object versions and original metadata are
retained, but IDs continue to come from the source map.

All extract queries must scope object and membership lookups by extract ID.
The extract tool can save initial rectangular snapshots, which `update_extract`
brings up to date with later edits. Stored extracts can be exported with
`export_extract`. Downgrade to 13 and direct map-table dropping remove these
tables. Already-version-14 databases will not automatically run the amended
migration; adding these tables requires a separate application of the DDL.

Recording the edit IDs in an extract or dump
--------------------------------------------

The `extract` and `dump` tools can record which edits their output includes:

    ./extract --bbox=-1.078,50.788,-1.074,50.790 --out=portsmouth.osm.gz --edit-ids
    ./dump --out=dump.osm.gz --edit-ids

`--edit-ids` adds `edit_activity_id` and `atomic_edit_id` attributes to the root
`<osm>` element, holding the latest edit activity row ID and atomic edit ID (zero
if there is no activity). They are read in the same transaction snapshot as the
data, so the file contains exactly the edits up to those IDs. The option needs
`.osm.gz` output, because o5m has no root element to hold attributes; with
`.o5m.gz` the tools stop with an error. Without the option the output is
unchanged. `dump` writes `dump.o5m.gz` unless `--out` names another file.
`PgTransaction::GetLatestEditIds` returns the same IDs through the bindings.

Saving a rectangular extract to the database
--------------------------------------------

Build the tool with `make extract`, then run from the pgmap directory:

    ./extract --bbox=-1.078,50.788,-1.074,50.790 --save-db --name=portsmouth

Connection and prefixes come from `config.cfg`. The new extract is stored in
`<dbtablemodifyprefix>extracts` and its associated extract object/membership
tables. The command prints the generated extract ID. Each invocation creates a
new extract; it does not replace an existing one. `--save-db` requires a nonempty
bbox and cannot be combined with `--wkt` or `--out`. File output remains the
existing default.

`performed_at` records the snapshot transaction's start time. Row and atomic
checkpoints are the greatest visible IDs in the source activity table, or zero
if none exist, read in the same repeatable-read transaction as the map query.
The extract metadata, object rows, and membership rows commit together. A failed
query or insert rolls back the snapshot. Source object IDs, versions, tags,
geometry, members, roles, and metadata are preserved, including outside nodes
needed to complete ways. Contents stream into PostgreSQL; the existing map query
still retains selected object/member IDs in memory.

The source must have the amended schema 14, including `performed_at` and
`edit_activity_id` metadata columns. Existing version-14 installations need
those additions applied separately. Rebuild the tool/bindings after updating.

Exporting a stored extract
--------------------------

Build with `make export_extract`, then select by ID or unique name:

    ./export_extract --id=1 --out=portsmouth.osm.gz
    ./export_extract --name=portsmouth --out=portsmouth.o5m.gz

Supported extensions are `.osm`, `.o5m`, `.osm.gz`, and `.o5m.gz`. The tool reads
`config.cfg` by default; use `--config=/path/to/config.cfg` to select another
configuration. It uses `dbtablemodifyprefix` for extract tables.

Every stored node, way, and relation is exported in ID order within its type,
including outside completion nodes. The rectangle is written as bounds, not
used to filter the stored contents again. Objects stream in batches from one
consistent database snapshot, preserving stored usernames and source versions.
An unknown ID/name is an error; duplicate names require selecting by ID.

The output is written to a temporary file beside the destination and renamed
only on success. An existing destination is replaced on success and retained
on failure. Export does not modify the database or require a schema change.

Extract table locking
---------------------

Save/export transactions acquire the static and active main-map locks first.
Before accessing extracts, they lock all eight active-prefix extract tables in
one consistent order: metadata, nodes, ways, relations, way memberships, then
node/way/relation relation memberships. Saving uses EXCLUSIVE; exporting uses
ACCESS SHARE. Locks remain held until commit or abort. These modes allow exports
alongside saves, while serializing saves and preventing conflicting DDL. Updates
follow the same main-map-before-extract order and lock extracts EXCLUSIVE.

Updating a stored extract
-------------------------

    make update_extract
    ./update_extract --id=1
    ./update_extract --name=portsmouth

The command reads `config.cfg` (or `--config=...`), locks the main map first and
all extract tables exclusively afterwards, then updates to the latest visible
source activity checkpoint. Pending edits may create, modify or delete nodes,
ways and relations. Legacy ungrouped rows, incomplete groups, unset checkpoints,
or a source checkpoint that has moved backwards cause failure without committing
changes. Names must be unique.

Most logic lives in `dbextract.cpp`/`dbextract.h`. Activity is used to detect
pending edits and validate the interval; the extract's membership is then
recalculated in SQL using its stored query mode and the current source snapshot,
following the same selection rules as a map query. It replaces only that
extract's current objects and memberships, preserving source IDs and versions,
and advances both checkpoints atomically. It handles node movement, shared
completion nodes, and unchanged ways/relations entering or leaving due to edits
elsewhere. Membership-mode extracts are selected through the static and active
membership tables, so the cost is similar to a fresh map query of the bbox. It
does not replay individual historical versions or use sync footprints to narrow
the calculation. Temporary SQL tables avoid loading the extract into client
memory. With no pending activity the command is a no-op.

The source activity stream must be complete and retained, and derived source
bboxes must be correctly maintained. A reset/truncated/replaced source is not a
supported continuation, even if regenerated numeric checkpoints happen to
match; source identity tracking remains future work. No new schema version is
needed.

Listing stored extracts
-----------------------

`PgTransaction::ListExtracts` (`DbListExtracts` in `dbextract.cpp`) describes
every stored extract: ID, name, bbox, query mode, time of the last save or
update, checkpoints, and whether the map has later edits.
`PgTransaction::GetExtract` describes one extract and also counts its nodes,
ways and relations. The parent project shows both in its Django admin.

Deleting a stored extract
-------------------------

`PgTransaction::DeleteExtract` (`DbDeleteExtract` in `dbextract.cpp`) removes an
extract's metadata, objects and membership rows, selected by ID or unique name.
It locks the extract tables EXCLUSIVE and does not touch the map. There is no
command line tool for this; the parent project's Django admin uses it.

Comparing a stored extract with a map query
-------------------------------------------

    make compare_extract
    ./compare_extract --id=1
    ./compare_extract --name=portsmouth
    ./compare_extract --all

The command reads the stored extract from the database and runs a fresh map
query of the extract's bbox, both in one transaction snapshot, then compares
them. `--all` checks every stored extract in ID order within that one snapshot
and finishes with a summary line. Object types, IDs and versions must agree; ordering is ignored and tags,
coordinates and members are not compared. It prints object counts for each side
and lists every differing object as missing
from the extract, not in the query, or having different versions. It notes when
the map has edits later than the extract's checkpoint, in which case differences
are expected until `update_extract` is run, and when the map's `useBboxInQuery`
mode has changed since the extract was saved. Nothing is modified. The exit
status is 0 when every extract checked matches, 1 if any differs and 2 for an
error. The versions of
every object on both sides are held in memory during the comparison. The
comparison is in `DbCompareExtract` in `dbextract.cpp`, also available as
`PgTransaction::CompareExtract` and `CompareAllExtracts` through the bindings.

Tests for stored extracts live in the parent project
(`querymap/test_dbextract.py`), which exercises these functions through the
Python bindings.
