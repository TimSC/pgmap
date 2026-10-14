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

pgmap needs PostgreSQL 9.5 or later, with PostGIS, and libpqxx 7. Earlier versions of either are no longer provided for: the build stops with a message on an older libpqxx.

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

Update the pgmap config.cfg files with your new password. Use 127.0.0.1 rather than localhost, if the database is on the same machine. Set `dbport` if the database does not listen on the usual port, 5432; the line can be left out otherwise.

	cp config.cfg.template config.cfg

	nano config.cfg

Add your database config info to config.cfg. Any setting can instead be given in the environment, which replaces what the file says and works without a file: its name in capitals after `PGMAP_`, so `PGMAP_DBHOST` for dbhost, `PGMAP_DBPASS` for dbpass and `PGMAP_DUMP_PATH` for dump_path. For example:

	PGMAP_DBHOST=10.0.0.5 PGMAP_DBPASS=secret ./admin

Then check we can connect:

	./admin

Download a regional or planet map dump, for example this one: https://archive.org/details/uk-eire-fosm-2017-jan.o5m

The file format is chosen by the file name, which must end in .osm, .o5m, .pbf or .json, optionally followed by .gz (for example .o5m.gz).

Set the dump_path variable in pgmap's config.cfg to the actual path of the data to import. Then use the admin tool to create the tables, import the file, and create the indices:

    ./admin

"Import map data" reads the file named by dump_path straight into the tables, using PostgreSQL's COPY, which is much faster than conventional SQL. It needs libpqxx 7 or later.

The import is one transaction: if it fails, nothing is imported. Before importing a country or a planet, see "Recommended PostgreSQL settings for a large import" below.

You should do at least "Create tables", "Import map data" (skip if you want an empty database), "Create indicies", "Refresh max IDs", "Refresh max changeset IDs and UIDs" in order. Create indicies can take DAYS for a planet dump. Hopefully no errors occur. If you finish these steps, congratulations, you have successfully imported your map data! It might be prudent to remove superuser access for your database user, since it is no longer needed:

    sudo su postgres

    psql --dbname=db_map

    ALTER USER pycrocosm WITH NOSUPERUSER;

If you are attempting to configure pycrocosm, you can return to that README at this stage.

### Recommended PostgreSQL settings for a large import

PostgreSQL's defaults suit a small machine and are slow for loading a country or a planet. These settings are worth changing before a large import. They are PostgreSQL's own, not pgmap's: pgmap does not set any of them. `SHOW config_file;` in psql says where postgresql.conf is (on Ubuntu, /etc/postgresql/16/main/postgresql.conf), and `SHOW maintenance_work_mem;`, for example, says what a setting is now.

For "Import map data":

    wal_level = minimal
    max_wal_senders = 0

Normally everything written to a table is also written to the write-ahead log, which for an import takes about ten times the size of the input file (40 MB for a 4 MB o5m file, when measured). With `wal_level = minimal`, PostgreSQL leaves a table's rows out of the log when the table was emptied in the transaction that fills it, which is how the import works. `max_wal_senders = 0` is required with it. Both need PostgreSQL to be restarted. The admin tool prints a note when it imports with any other wal_level. These two turn off replication and log archiving, so set them back afterwards (`wal_level = replica` is the default) if the database is replicated or backed up by archiving its log.

For "Create indicies", which is the longest step:

    maintenance_work_mem = 2GB
    max_parallel_maintenance_workers = 4

`maintenance_work_mem` is the memory one index build may sort in. The default of 64MB makes a large build spill to disk and merge in many passes. Somewhere between 1GB and a quarter of the machine's memory is reasonable; each parallel worker and each autovacuum worker may use as much again, so leave room for several. `max_parallel_maintenance_workers` (default 2) lets one build use more processor cores, but only for the ordinary (B-tree) indices: the spatial (GiST) and tag (GIN) indices are built by a single process whatever it is set to.

For both steps:

    max_wal_size = 16GB
    checkpoint_timeout = 30min

A bulk load fills the write-ahead log quickly, and with the default `max_wal_size` of 1GB PostgreSQL stops to checkpoint every few seconds. A larger allowance lets it get on with the load, at the cost of that much disk space for the log while it runs.

All of these except the first two take effect on a reload (`sudo systemctl reload postgresql`), and can also be given to one database with `ALTER DATABASE db_map SET maintenance_work_mem = '2GB';`, which applies to connections made afterwards. They can be left in place after the import; only `wal_level` and `max_wal_senders` need thought.

Do not turn off `fsync` or `full_page_writes` to go faster: a crash or power cut then can corrupt the whole database, not only the import.

Only the size of the write-ahead log was measured with pgmap. The rest is PostgreSQL's general guidance for bulk loading, and how much each helps depends on the machine.

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



Index version
-------------

The indices of the main map tables are built by the admin tool's "Create
indicies", after the tables have been created and data imported. They are not
part of the schema version, and have a version of their own: when the step
has finished, it records `index_version` in the `meta` table of each table
set (static, modify and test).

Index versions are numbered with the schema: index version 15 is the set of
indices for schema version 15. The two are still recorded separately, as a
map's tables are created or upgraded by one step and indexed by another, often
much later, and a new schema version raises the index version whether or not
the indices change.

The admin tool compares the recorded version with the one it builds, and says
so above its menu when a table set is behind, for example

    The indices of the planet_static_ tables are those of schema version 15
    (the current version is 16): run "Create indicies".

"Upgrade/downgrade index version" (option h) brings the indices of every
table set to a version given: the latest is what "Create indicies" builds, and
0 takes away every index and key that step builds, after asking for
confirmation. Dropping them before loading a large amount of data into
existing tables, and building them again afterwards, is much quicker than
keeping them up as the rows go in. Versions 11 to 15 have the same indices.

A table set with no recorded version, such as one whose indices were built
before versions were recorded, is reported the same way. Running "Create
indicies" is always safe: it makes the changes of any later versions, such as
dropping an index that is no longer wanted, checks every index and adds any
that is missing, and then records the current version. Run it after upgrading
the schema of an existing map, and whenever the admin tool asks.

The indices that a schema version brings with the tables it adds (edit
activity, stored extracts, the key of the member tables) are created by the
schema upgrade and are not counted in the index version. The optional
combined indices of boxes and tags ("Create/drop bbox indices") are not
either. The current index version is 15. A map whose indices were recorded
as version 1, as the first versions of this numbered them, is taken as 15.

### Working out the boxes of ways and relations

"Update way/relation bboxes" in the admin tool stores, with every way and
relation, the box around it. A map with `useBboxInQuery` on needs them, and
they are also what an edit's record takes its boxes from. Edits keep the boxes
of what they touch up to date; this step is for everything else, such as a
map that has just been imported.

The ways are done 50,000 at a time, and each batch is kept as it finishes, so
the step shows how far it has got and loses nothing if it is stopped. To take
up a stopped run, answer 1 to "Only ways that have no box yet": ways that
already have a box are then passed over. The relations are worked out
afterwards, in the database, from their node members and the boxes of their
way members and then through relations that hold relations, however deep.

On a whole planet this is a long step, as every way has to find its nodes: an
earlier version took over eight days for the ways. Run "Create indicies"
first: the batches need the primary key of the ways, and without it the ways
are done by one statement that keeps nothing until it has finished. Set the
server to read only while it runs, or edits made meanwhile to a way not yet
reached are worked out twice, which is harmless but wasted.

### Indices for boxes and tags

"Create/drop bbox indices" in the admin tool builds the optional indices that
searches by area and by tag use: a spatial index on the stored box of each way
and relation, which a map with `useBboxInQuery` on needs, and a text index on
the tags of each node, way and relation, which the Overpass and XAPI tag
searches use. A search by tag within an area uses both.

Earlier versions built one combined index for each kind of object, on its
position or box and its tags together. The separate indices are several times
quicker to build and smaller, and a search by tag alone is much quicker with
them. A table that already has a combined index keeps it, and the step says
so: it goes on serving. To change over, drop the bbox indices and create them
again, which on a large map is worth planning for, as the text index reads the
tags of every object.

Both building and querying are helped by a generous `maintenance_work_mem`
(see the recommended settings above). The step finishes by gathering
statistics for each table it indexed, which the planner needs to tell a common
tag from a rare one.

Member tables and views (schema version 15)
-------------------------------------------

The member tables (`way_mems`, `relation_mems_n`, `relation_mems_w` and
`relation_mems_r`) say which ways and relations hold an object. From version
15 they hold the members of the live version of each way and relation, and
nothing else, and have a primary key of `(id, version, index)`:

* Before, rows were added for every version of an object that was stored and
  never removed, so the tables grew with the history of edits. Only the live
  version's rows are ever looked up. The members of an old version remain in
  its row of the history table (`oldways`, `oldrelations`).
* Before, nothing stopped the same row being added twice, and it was, each
  time an edit touched a member of a way or relation. The key makes that an
  error.

When a way or relation is edited or deleted, the rows of the version that was
live are removed and those of the new live version, if there is one, are
written. An import writes rows for live objects only.

The `visible*` views, which show a table set's own objects together with its
parent's, join their two halves with `UNION ALL` in place of `UNION`. The
halves cannot overlap, and `UNION` had PostgreSQL compare whole rows, tags and
all, to find that out.

Upgrading from version 14 (admin tool, "Upgrade/downgrade db schema", version
15) empties each member table and fills it again from the live tables, then
adds the key. On a large map this is a long step, comparable to importing the
ways: the member tables are among the largest, and the key is an index over
every row. It can be run while nothing is editing the map. pgmap works with a
map still at version 14, without removing old rows there. Downgrading to 14
drops the key and puts the views back; the tables keep what they hold.

Atomic edit activity (schema version 14)
---------------------------------------

The XML endpoints `/replication/edit_activity/<id>` and
`/replication/edit_activities` expose `atomic_edit_id` and `block_index`
attributes for grouped rows. Each record lists the objects involved in full
(`existing`, `updated`, `affectedparents`, `related`, `relatedparents`),
resolved from the stored type/ID/version references, with the bounding box
elements described under "Edit activity records" below. Rebuild the bindings
to expose the C++ reader fields before running these endpoints.

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

Earlier builds of version 14 had `sync_before`, `sync_after`, `bbox_before`
and `bbox_after` columns. They have been removed: `existing` holds what
`sync_before` did, and the other three were never populated. Asking for schema
14 again drops them from a database that has them.

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
* `/replication/edit_activities`: the latest 20 rows.
* `/replication/edit_activities?limit=50`: the latest 50. `limit` can be up to
  100, and with a range it is how many rows are sent from the start of it.
* `/replication/edit_activities?first_id=100&bbox=-1.1,50.7,-1.0,50.8`: only
  the rows that may concern that area (min lon, min lat, max lon, max lat).

ID queries return rows ordered by row ID; an unmatched collection query returns
an empty collection. IDs must be positive integers. A row range can additionally
be restricted by atomic edit ID, but that may return only part of the group.
Row ranges can split atomic edits; use atomic-edit queries when a complete group
is required. Timestamp queries remain supported but cannot be combined with ID
filters. Rebuild pgmap bindings for the new QueryEditActivityByIds method.

Edit activity (schema version 14)
---------------------------------

Each block of an upload is recorded as a row of `edit_activity`: the edited
objects before and after, the ways and relations that hold them
(`affectedparents`), and the unedited members around them (`related`), all as
type, ID and version. `related_parents` holds, in the same form, the relations
that have a related node as a member. The edit does not touch those relations,
but a consumer keeping part of the map up to date from these records needs
them: a related node can be new to it, brought in as part of a way, and then
the relations holding that node belong there too. With them a record carries
everything such a consumer needs, and it has no reason to query the map.

Four columns say where the objects of those lists are, each a PostGIS
GeometryCollection (SRID 4326) with one geometry for every entry of its list,
in the same order:

* `existing_bbox`: each `existing` object before the edit.
* `updated_bbox`: each `updated` object once the edit is made. A deleted
  object is nowhere afterwards and has `POLYGON EMPTY`, which keeps the
  positions in step.
* `affectedparents_before_bbox` and `affectedparents_after_bbox`: each
  `affectedparents` object on either side of the edit. Moving a node changes
  the extent of the ways and relations holding it without changing their
  version, so this is the only record of that change.

A node has a point at its position and a way or relation a polygon of its box.
A way's box is the stored one, or is worked out from its nodes on a map that
does not store boxes. Where the box of an object is not known, as for a
relation on such a map, the whole column is left NULL for that row. The web
API publishes each as an element of the same name holding one
`<bbox type id minlon minlat maxlon maxlat>` per object (a node's minimum and
maximum are the same), with `null="true"` in place of the numbers for a deleted
object, or on the element itself where the column is NULL.

The `bbox` filter of the web API keeps a row when any of the four collections
reaches into the area asked about, so that something keeping one area of the
map up to date can ask only for the edits that could matter to it. Both sides
of the edit count: an object can leave an area as well as enter it. A row with
one of the four columns NULL could concern anywhere and is always kept. The
comparison is with the extent of each whole collection, so a row is sometimes
kept without need, never lost.

One case is not covered by where the objects are. A copy of an area has a way
when one of the way's nodes is in the area, has all of that way's nodes, and
has the relations holding any of those nodes. A relation can therefore belong
to an area that neither it nor its members touch. When an edit creates,
modifies or deletes a relation, `relation_reach_bbox` is set to a polygon of
the extent of the ways that hold the relation's member nodes, and the filter
tests that as well. It is NULL for every other edit, which here means nowhere
more, not unknown. It and the four collections each have a GiST index. The
`bbox` column the table had before version 14, which nothing filled in, is
dropped by the schema step.

Rows are written by the web API's upload and by the admin tool's "Apply
diffs", both through `EditActivityRecorder` (dbeditactivity.h), where the
rules for what a row holds are set out. Applying diffs differs in these ways: each
diff file is one atomic edit, with one row for each of its action blocks; a
row takes its changeset and user from the first object of its block and its
time from the latest; and nothing is checked, so a diff that deletes an object
still in use is recorded as it stands. Several queries are made for every
block, which makes applying a large diff slower than it was. Nothing is
recorded on a map below schema 14. Importing map data or changeset metadata
does not write rows.

`related_parents` and the bounding box columns were added to schema 14 after
it was first in use. They are NULL in rows recorded before then, which means
not recorded. On a map already at version 14 or later, asking the admin tool
for the latest schema version adds the columns. That has to be done: until it
is, recording an edit and reading the records fail, with an error saying so.

Extract storage (schema version 14)
----------------------------------

Migration 13-to-14 creates the following tables under each configured table-set
prefix. There is one shared set of tables per prefix, supporting multiple
extracts by extract ID; each extract has one current state without an overlay.

* `extracts`: generated ID, optional name, rectangular bbox (Polygon, SRID 4326),
  query-mode flag `use_bbox_in_query`, `performed_at` timestamp, last applied
  `edit_activity_id`, and last applied `atomic_edit_id`. A NULL
  checkpoint denotes an extract whose initial snapshot is not established.
  `auto_update` (default false) says whether the extract is to be kept up to
  date automatically, and `update_url` (default blank) names the API to update
  it from, blank meaning this map's own. pgmap's own tools do not act on
  them; pycrocosm's `updateextracts` command does, using `PgExtractPatch` to
  apply edits published by another server. They were added to schema 14 after it was
  first in use; on a map already at version 14 or later, asking the admin tool
  for the latest schema version adds them.
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

Output file formats
-------------------

`dump`, `extract` and `export_extract` choose the output format from the file
name given to `--out`:

* `.osm` for OSM XML
* `.o5m` for o5m
* `.pbf` for PBF
* `.json` for OSM JSON, the format the OSM API returns for `.json` requests

Add `.gz` to any of them, for example `.osm.gz`, to gzip the file. PBF is
already compressed internally, so `.pbf.gz` gains almost nothing. Any other
name is refused before the database is read.

The tools write to a temporary file beside the destination and move it into
place only when the output is complete, so a failed run leaves an existing file
as it was. o5m and PBF store coordinates to seven decimal places, where XML
keeps everything the database holds.

Recording the edit IDs in an extract or dump
--------------------------------------------

The `extract` and `dump` tools can record which edits their output includes:

    ./extract --bbox=-1.078,50.788,-1.074,50.790 --out=portsmouth.osm.gz --edit-ids
    ./dump --out=dump.o5m.gz --edit-ids
    ./export_extract --id=1 --out=portsmouth.json --edit-ids

`--edit-ids` records two values in the file header: `edit_activity_id`, the latest
edit activity row ID, and `atomic_edit_id`, the latest atomic edit ID (zero if
there is no activity). They are read in the same transaction snapshot as the
data, so the file contains exactly the edits up to those IDs. Where they go
depends on the format:

* `.osm`: attributes of the root `<osm>` element.
* `.json`: string members at the top of the document.
* `.o5m`: o5m has no standard place for them, so they are written in a dataset
  of cppo5m's own (type `0xc0`) straight after the header. Programs that do not
  know it skip it, as the o5m format requires, so the file reads normally
  elsewhere; osmconvert prints an "unknown .o5m dataset" warning, and drops
  the IDs if it rewrites the file.
* `.pbf`: not available; the tools stop with an error.

`export_extract --edit-ids` writes the same two names, holding the stored
extract's checkpoint: the edits the extract is current to, which is behind the
map if the extract is waiting for an update. cppo5m reads the values back from
all three formats as the document's attributes.

Without the option the output is unchanged. `dump` writes `dump.o5m.gz` unless
`--out` names another file.
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

The output format follows the file name, as described under "Output file
formats" below. The tool reads
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

Importing a file as a stored extract
------------------------------------

`import_extract` is the reverse of `export_extract`: it stores the contents of
a map file as a new extract. Build with `make import_extract`, then:

    ./import_extract --in=portsmouth.osm.gz --name=portsmouth
    ./import_extract --in=area.pbf --name=area --bbox=-1.078,50.788,-1.074,50.790

The input format follows the file name, which must end in .osm, .o5m, .pbf or
.json, optionally followed by .gz. The command prints the new extract's ID and
how many objects it holds. Each invocation creates a new extract; `--name` is
optional and is not checked for uniqueness. The tool reads `config.cfg` unless
`--config` names another file, and uses `dbtablemodifyprefix` for the extract
tables.

The extract's rectangle is the bounds recorded in the file. Give `--bbox` if
the file has none, or to use a different rectangle. The contents are stored as
they are and are not filtered against the rectangle.

The extract's checkpoint is taken from the `edit_activity_id` and
`atomic_edit_id` header attributes that `export_extract --edit-ids` writes, or
from `--edit-activity-id` and `--atomic-edit-id` if given (both or neither). A
.pbf file cannot hold these attributes. An extract imported without a
checkpoint can be exported and compared, but `update_extract` refuses it. The
checkpoint is only meaningful for a file that came from this map: an update
compares it with this map's own edit activity.

Every object is stored with its ID, version, tags, members, roles and metadata.
Positions in .o5m and .pbf files have seven decimal places, so a round trip
through those formats rounds any finer coordinate. Deleted objects in the file
are skipped. A diff (.osc style content), an object that appears twice, or a
file that cannot be decoded fails the import. The extract and its contents
commit together, so a failed import leaves nothing behind.

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
