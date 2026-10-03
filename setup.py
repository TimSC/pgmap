#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
setup.py file for SWIG pgmap
"""
from __future__ import print_function
import re
import subprocess
import shutil
from packaging.version import Version
from setuptools import setup, Extension
import setuptools.command.build_ext
import setuptools.command.build_py


class Build_Py_With_Swig(setuptools.command.build_py.build_py):
	def run(self):
		# SWIG generates pgmap.py during build_ext. Generate it before build_py
		# copies Python modules, otherwise a clean wheel contains only _pgmap.
		self.run_command('build_ext')
		super().run()


class Build_Ext_find_swig3(setuptools.command.build_ext.build_ext):
	def find_swig(self):
		return get_swig_executable()


def get_swig_executable():
	"Get SWIG executable"
	swig_executable = None
	swig_minimum_version = "3.0.2"
	for executable in ["swig", "swig3.0"]:
		swig_executable = shutil.which(executable)
		if swig_executable is not None:
			output = subprocess.check_output([swig_executable, "-version"]).decode('utf-8')
			swig_version = re.findall(r"SWIG Version ([0-9.]+)", output)[0]
			if Version(swig_version) >= Version(swig_minimum_version):
				break
			swig_executable = None
	if swig_executable is None:
		raise OSError("Unable to find SWIG version %s or higher." % swig_minimum_version)
	print("Found SWIG: %s (version %s)" % (swig_executable, swig_version))
	return swig_executable


pgmap_module = Extension('_pgmap',
	define_macros=[('PYTHON_AWARE', '1')],
	sources=['pgmap.i', 'util.cpp', 'dbquery.cpp', 'dbids.cpp', 'dbadmin.cpp', 'dbcommon.cpp', 'dbreplicate.cpp', 'dbdecode.cpp',
		'dbstore.cpp', 'dbdump.cpp', 'dbfilters.cpp', 'dbchangeset.cpp', 'dbjson.cpp', 'dbmeta.cpp', 'dbusername.cpp',
		'dboverpass.cpp', 'dbeditactivity.cpp', 'dbprepared.cpp', 'pgcommon.cpp', 'pgmap.cpp', 'cppo5m/o5m.cpp',
		'cppo5m/varint.cpp', 'cppo5m/OsmData.cpp', 'cppo5m/osmxml.cpp', 'cppo5m/iso8601lib/iso8601.c',
		'cppo5m/utils.cpp', 'cppo5m/pbf.cpp', 'cppo5m/pbf/fileformat.pb.cc', 'cppo5m/pbf/osmformat.pb.cc',
		'cppGzip/EncodeGzip.cpp', 'cppGzip/DecodeGzip.cpp'],
	swig_opts=['-c++', '-DPYTHON_AWARE', '-DSWIGWORDSIZE64'],
	libraries=['pqxx', 'expat', 'z', 'boost_filesystem', 'boost_system', 'protobuf', 'boost_iostreams'],
	language="c++",
	extra_compile_args=["-std=c++17", '-DPY_SSIZE_T_CLEAN'],
)


setup(
	ext_modules=[pgmap_module],
	py_modules=["pgmap"],
	cmdclass={"build_ext": Build_Ext_find_swig3, "build_py": Build_Py_With_Swig},
)
