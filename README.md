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

