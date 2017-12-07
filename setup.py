#!/usr/bin/env python

"""
setup.py file for SWIG pgmap
"""

# -*- coding: utf-8 -*-
from __future__ import print_function
from distutils.core import setup, Extension

pgmap_module = Extension('_pgmap',
				define_macros = [('PYTHON_AWARE', '1')],
				sources=['pgmap.i', 'util.cpp', 'dbquery.cpp', 'dbids.cpp', 'dbadmin.cpp', 'dbcommon.cpp', 'dbreplicate.cpp', 'dbdecode.cpp', 
					'dbstore.cpp', 'dbdump.cpp', 'dbfilters.cpp', 'dbchangeset.cpp', 'dbjson.cpp', 'dbmeta.cpp', 'pgmap.cpp', 'cppo5m/o5m.cpp', 
					'cppo5m/varint.cpp', 'cppo5m/OsmData.cpp', 'cppo5m/osmxml.cpp', 'cppo5m/iso8601lib/iso8601.c',
					'cppGzip/EncodeGzip.cpp', 'cppGzip/DecodeGzip.cpp'],
				swig_opts=['-c++', '-DPYTHON_AWARE', '-DSWIGWORDSIZE64'],
				libraries = ['pqxx', 'expat', 'z', 'boost_filesystem', 'boost_system'],
				language = "c++",
				extra_compile_args = ["-std=c++11"],
			)

setup (name = 'pgmap',
	version = '0.1',
	author      = "Tim Sheerman-Chase",
	description = """pgmap swig module""",
	ext_modules = [pgmap_module],
	py_modules = ["pgmap"],
	)

