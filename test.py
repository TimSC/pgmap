# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from __future__ import print_function
import pgmap
import io
import string

def ReadConfig(fina):
	fi = open(fina, "rt")
	settings = {}
	for li in fi.readlines():
		lisp = map(string.strip, li.split(":"))
		if len(lisp) < 2:
			continue
		settings[lisp[0]] = lisp[1]
	return settings

if __name__=="__main__":

	settings = ReadConfig("config.cfg")

	p = pgmap.PgMap("dbname={} user={} password='{}' hostaddr={} port=5432".format(
		settings["dbname"], settings["dbuser"], settings["dbpass"], settings["dbhost"]), 
		settings["dbtableprefix"], settings["dbtabletestprefix"])
	print ("Connected to database", p.Ready())

	t = p.GetTransaction(b"ACCESS SHARE")

	if 0:
		sio = io.BytesIO()
		#enc = pgmap.PyO5mEncode(sio)
		enc = pgmap.PyOsmXmlEncode(sio)

		print (t.MapQuery((-1.1473846,50.7360206,-0.9901428,50.8649113), 0, enc))

		data = sio.getvalue()
		print (len(data), "bytes")

	if 1:
		osmData = pgmap.OsmData()
		objectIds = [1000594005591, 1000595178493, 1000594446551]
		t.GetObjectsById("node", pgmap.seti64(objectIds), osmData);
		print (len(osmData.nodes))
		for i in range(len(osmData.nodes)):
			node = osmData.nodes[i]
			print (type(node))
			print (node.objId, node.lat, node.lon)

	t.Commit()

