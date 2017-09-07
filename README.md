pgmap-query
===========

C++ SWIG module for accessing microcosm's postgis OSM map GIS schema. SWIG can produce a python module containing the functionality, as well as for several other languages.

The tools to configure and import the postgis database are here: https://github.com/TimSC/osm2pgcopy

To use this library:

	sudo apt install libpqxx-dev rapidjson-dev

	git clone https://github.com/TimSC/pgmap-query.git --recursive

	cd pgmap-query

	swig -python -c++ -DPYTHON_AWARE -DSWIGWORDSIZE64 pgmap.i

	python setup.py build_ext --inplace

	cp config.cfg.template config.cfg

Add your database config info to config.cfg then:

	python test.py

SWIGWORDSIZE64 assumes you are using a 64 bit platform. See https://github.com/swig/swig/issues/568


