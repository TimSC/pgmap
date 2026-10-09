#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
setup.py file for SWIG pgmap
"""
from __future__ import print_function
import os
import re
import shlex
import subprocess
import shutil
from concurrent.futures import ThreadPoolExecutor
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

	def build_extensions(self):
		self.compiler.compile = make_parallel_compile(self.compiler)
		super().build_extensions()


def make_parallel_compile(compiler):
	"""Compile the source files of an extension in parallel.

	setuptools compiles sources one at a time (its --parallel option only
	builds separate extensions concurrently). Set PGMAP_BUILD_JOBS to limit
	the number of compiler processes; the default is the number of CPUs."""
	jobs = int(os.environ.get("PGMAP_BUILD_JOBS") or os.cpu_count() or 1)

	def compile(sources, output_dir=None, macros=None, include_dirs=None, debug=0,
			extra_preargs=None, extra_postargs=None, depends=None):
		macros, objects, extra_postargs, pp_opts, build = compiler._setup_compile(
			output_dir, macros, include_dirs, sources, depends, extra_postargs)
		cc_args = compiler._get_cc_args(pp_opts, debug, extra_preargs)

		def compile_one(obj):
			if obj not in build:
				return
			src, ext = build[obj]
			compiler._compile(obj, src, ext, cc_args, extra_postargs, pp_opts)

		with ThreadPoolExecutor(max_workers=jobs) as executor:
			list(executor.map(compile_one, objects))
		return objects

	return compile


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


# Compiler flags appended after Python's defaults (-g -O2), so they take
# precedence. Override with PGMAP_CFLAGS, e.g. PGMAP_CFLAGS="-g -O0" for
# debugging. An empty value uses Python's defaults.
PGMAP_CFLAGS = shlex.split(os.environ.get("PGMAP_CFLAGS", "-g0 -O1"))


pgmap_module = Extension('_pgmap',
	define_macros=[('PYTHON_AWARE', '1')],
	sources=['pgmap.i', 'util.cpp', 'dbquery.cpp', 'dbids.cpp', 'dbadmin.cpp', 'dbcommon.cpp', 'dbreplicate.cpp', 'dbdecode.cpp',
		'dbextract.cpp', 'dbstore.cpp', 'dbdump.cpp', 'dbfilters.cpp', 'dbchangeset.cpp', 'dbjson.cpp', 'dbmeta.cpp', 'dbusername.cpp',
		'dboverpass.cpp', 'dbeditactivity.cpp', 'dbprepared.cpp', 'pgcommon.cpp', 'pgmap.cpp',
		'cppo5m/model.cpp', 'cppo5m/sink.cpp', 'cppo5m/pysink.cpp', 'cppo5m/decoder.cpp', 'cppo5m/varint.cpp', 'cppo5m/osmtime.cpp',
		'cppo5m/o5m.cpp', 'cppo5m/osmxml.cpp', 'cppo5m/osmchangexml.cpp', 'cppo5m/osmjson.cpp', 'cppo5m/pbf.cpp', 'cppo5m/filters.cpp',
		'cppo5m/io.cpp', 'cppo5m/iso8601lib/iso8601.c', 'cppo5m/pbf/fileformat.pb.cc', 'cppo5m/pbf/osmformat.pb.cc',
		'cppGzip/EncodeGzip.cpp', 'cppGzip/DecodeGzip.cpp'],
	swig_opts=['-c++', '-DPYTHON_AWARE', '-DSWIGWORDSIZE64'],
	libraries=['pqxx', 'expat', 'z', 'boost_filesystem', 'boost_system', 'protobuf'],
	language="c++",
	extra_compile_args=["-std=c++17", '-DPY_SSIZE_T_CLEAN'] + PGMAP_CFLAGS,
)


setup(
	ext_modules=[pgmap_module],
	py_modules=["pgmap"],
	cmdclass={"build_ext": Build_Ext_find_swig3, "build_py": Build_Py_With_Swig},
)
