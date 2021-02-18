import argparse
import xmlrpc.client
import time
if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	args = parser.parse_args()

	hostport = args.hostport

	try:
		# print(hostport)
		client1 = xmlrpc.client.ServerProxy('http://127.0.0.1:8084' )
		client2 = xmlrpc.client.ServerProxy('http://127.0.0.1:8085' )
		client3 = xmlrpc.client.ServerProxy('http://127.0.0.1:8083' )
		while True:
			start = time.time()
			while client3.surfstore.isLeader():
				if not client1.surfstore.isCrashed():
					client1.surfstore.crash()
				if not client2.surfstore.isCrashed():
					client2.surfstore.crash()
			end = time.time()
			print(start,end,end-start)
			break
			# if client3.surfstore.isLeader():
			# 	client3.surfstore.crash()
			# if client1.surfstore.isCrashed() and client2.surfstore.isCrashed() :
			# 	break
		# Test ping
		# client = xmlrpc.client.ServerProxy('http://' + hostport)
		# client.surfstore.ping()
		# print("Ping() successful")
		# client.surfstore.isLeader()
		# client.surfstore.crash()
		# print(client.surfstore.isCrashed())
		# client.surfstore.crash()
		# client.surfstore.restore()
		# print(client.surfstore.isCrashed())

	# client.surfstore.updatefile("Test.txt", 3, [1,2,3])
		# client.surfstore.tester_getversion("Test.txt")

	except Exception as e:
		print("Client: " + str(e))
