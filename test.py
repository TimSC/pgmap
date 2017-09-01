from __future__ import print_function
import pgmap

if __name__=="__main__":

	p = pgmap.PgMap("dbname=db_map user=microcosm password='vWm0wNKVxd14' hostaddr=127.0.0.1 port=5432", "portsmouth_")
	print (p.Ready())
	
