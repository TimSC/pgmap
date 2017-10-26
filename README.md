pgmap
=====

C++ SWIG module for accessing microcosm's postgis OSM map GIS schema. SWIG can produce a python module containing the functionality, as well as for several other languages.

The tools to configure and import the postgis database are here: https://github.com/TimSC/osm2pgcopy (This will eventually be merged into this library and pycrocosm.)

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

	cp config.cfg.template config.cfg

Hopefully you have already imported some data into Add your database config info to config.cfg then:

	python test.py

SWIGWORDSIZE64 assumes you are using a 64 bit platform. See https://github.com/swig/swig/issues/568

Database Design
---------------

Django has an integrated unit test system. This keeps seperate live ("mod") and test tables to avoid corrupting the live database. Although pgmap manages the map orientated tables rather than using Django, it keeps the Django concept of test and operational data unit tests run. The set of tables accessed by pgmap is called the "active" table set, which is either the "test" or "mod" table set, and determined when the main pgmap is initalized.

Another design choice, based on most of the map data is unchanging, is to maintain separate static map tables which don't change even when edits occur. This allows a lengthy map import to be done once and left unchanged. Unfortunately, this makes map query logic more complex, since both the static and active tables need to be checked for objects.

It is quite possibly to import data into the active table and leave the static tables empty.

