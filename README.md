pgmap
=====

C++ SWIG module for accessing microcosm's postgis OSM map GIS schema. It also provides OSM xml and o5m encoding/decoding, because it is far faster than processing this in C++ than within native python. SWIG can produce a python module containing the functionality, as well as for several other languages. 

Installation
------------

To use this library using Python 2 (note: python 3 support is planned):

	sudo apt install libpqxx-dev rapidjson-dev libexpat1-dev python-pip swig3.0 libboost-filesystem-dev osmctools

	sudo pip install --upgrade pip

If you have not already, create a virtual environment:

	sudo pip install virtualenv

	cd <src>

	virtualenv --python=/usr/bin/python pgmapenv

	source pgmapenv/bin/activate	

Go to the <src>/pycrocosm/pgmap folder or clone a new copy:

	git clone https://github.com/TimSC/pgmap.git --recursive

	cd pgmap

If you have not already, install the python module and build the tools:

	python setup.py install

	make

	cp config.cfg.template config.cfg

The SWIGWORDSIZE64 option in setup.py assumes you are using a 64 bit platform. See https://github.com/swig/swig/issues/568

Install and configure postgis
-----------------------------

    sudo apt install postgis postgresql postgresql-9.5-postgis-2.2

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

	CREATE EXTENSION postgis_sfcgal;
	
Disconnect using ctrl-D (repeatedly) to get back to your normal user. Check you can connect using psql; it often works out of the box. 

    psql -h 127.0.0.1 -d db_map -U pycrocosm --password

If necessary, enable log in by password by changing pg_hba.conf as administrator (as described in https://stackoverflow.com/a/4328789/4288232 ). When connecting, use 127.0.0.1 rather than localhost, if the database is on the same machine (postgresql treats them differently).

	locate pg_hba.conf

	sudo nano /etc/postgresql/9.5/main/pg_hba.conf

Update the pgmap config.cfg files with your new password. Use 127.0.0.1 rather than localhost, if the database is on the same machine.

	cp config.cfg.template config.cfg

	nano config.cfg

Add your database config info to config.cfg then check we can connect:

	./admin

Download a regional or planet map dump, for example this one: https://archive.org/details/uk-eire-fosm-2017-jan.o5m

The file format should be indicated by the extension .o5m.gz - if you have a different format use osmconvert:

    osmconvert ~/Desktop/extract.osm -o=extract.o5m.gz

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


