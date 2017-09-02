#!/usr/bin/env python

"""
setup.py file for SWIG pgmap
"""

from distutils.core import setup, Extension

pgmap_module = Extension('_pgmap',
				define_macros = [('PYTHON_AWARE', '1')],
				sources=['pgmap_wrap.cxx', 'util.cpp', 'db.cpp', 'pgmap.cpp', 'cppo5m/o5m.cpp', 'cppo5m/varint.cpp', 'cppo5m/OsmData.cpp', 'cppo5m/osmxml.cpp', 'cppGzip/EncodeGzip.cpp'],
				libraries = ['pqxx', 'z'],
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

