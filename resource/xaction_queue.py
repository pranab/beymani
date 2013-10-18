#!/usr/bin/python

import sys
import redis

op = sys.argv[1]
r = redis.StrictRedis(host='localhost', port=6379, db=0)

if (op == "setModel"):
	modelFile = sys.argv[2]
	with open (modelFile, "r") as myfile:
    		modelData=myfile.read()

	r.set('xactionMarkovModel', modelData)
elif (op == "getModel"):
	model = r.get("xactionMarkovModel")
	print model    
elif (op == "writeQueue"):
	xactionFile = sys.argv[2]
	with open (xactionFile, "r") as myfile:
		for line in myfile.readlines():
			#print line.rstrip('\n')
			r.lpush("xactionQueue", line.rstrip('\n'))
elif (op == "readQueue"):
	while True:
		line = r.rpop("xactionQueue")
		if line is not None:
			print line
		else:
			break
elif (op == "queueLength"):
	qlen = r.llen("xactionQueue")
	print qlen
elif (op == "readOutQueue"):
	while True:
		out = r.rpop("fraudQueue")
		if out is not None:
			print out
		else:
			break
elif (op == "outQueueLength"):
	qlen = r.llen("fraudQueue")
	print qlen
