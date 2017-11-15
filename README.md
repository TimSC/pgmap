pgmap
=====

C++ SWIG module for accessing microcosm's postgis OSM map GIS schema. It also provides OSM xml and o5m encoding/decoding, because it is far faster than processing this in C++ than within native python. SWIG can produce a python module containing the functionality, as well as for several other languages. 

Installation
------------

To use this library using Python 2 (note: python 3 support is planned):

	sudo apt install libpqxx-dev rapidjson-dev libexpat1-dev python-pip swig3.0

	sudo pip install --upgrade pip

	sudo pip install virtualenv

	cd <src>

	virtualenv --python=/usr/bin/python pgmapenv

	source pgmapenv/bin/activate	

	git clone https://github.com/TimSC/pgmap.git --recursive

	cd pgmap

	python setup.py install

	make

	cp config.cfg.template config.cfg

Hopefully you have already imported some data into postgis. Add your database config info to config.cfg then check we can connect:

	python test.py

The SWIGWORDSIZE64 option assumes you are using a 64 bit platform. See https://github.com/swig/swig/issues/568

To import a database, use osm2csv to convert the data to csv format. Then use the admin tool to create the tables, copy the csv files into the database, then create the indices.

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

Optimizing postgresql
---------------------

Postgresql can run faster if shared_buffers is increased in /etc/postgresql/9.5/main/postgresql.conf to about a quarter of total memory. (Having a large amount of memory wouldn't hurt either.) This is known to improve dump performance since the query has to interate over an index.

Work in progress
----------------

* Fix changeset handling, updating tags, expanding bbox.
* Move osm2pgcopy functionality into this library.
* Probably should add doxygen documentation (or more comments generally)

Instructions in progress
------------------------

* ALTER USER microcosm WITH SUPERUSER;

* ALTER USER myuser WITH NOSUPERUSER

* grant postgres to microcosm;

