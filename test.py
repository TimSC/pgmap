from __future__ import print_function
import pgmap
import io

if __name__=="__main__":

	p = pgmap.PgMap("dbname=db_map user=microcosm password='vWm0wNKVxd14' hostaddr=127.0.0.1 port=5432", "portsmouth_")
	print ("Connected to database", p.Ready())
	sio = io.BytesIO()
	#enc = pgmap.PyO5mEncode(sio)
	enc = pgmap.PyOsmXmlEncode(sio)

	print (p.MapQuery((-1.1473846,50.7360206,-0.9901428,50.8649113), 50000, enc))

	data = sio.getvalue()
	print (len(data), "bytes")

