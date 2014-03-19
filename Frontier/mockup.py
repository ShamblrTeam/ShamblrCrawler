# Socket is used for our Sockets
import socket

# Json is used for our API Passing
import json

# The queue.Queue Object is used as our queue
import queue

# Threading allows us to handle multiple sockets at once
import threading

# This is for random thread allocation
# dont get scared that this is in here
import random

# These are used for exception handling
import sys
import os


#our hash table that stores used values
hash_table = set()

#our frontier queue which handles queueing
frontier_queue = queue.Queue()

#our API Key Queue which handles our API Keys
api_key_queue = queue.Queue()

#Api key file location
api_key_file_location = "API_KEY_LIST.data"

#Handles if the program should print stuff
verbose = True

# Host of the machine
#This defaults to all network locations for the maching
common_host = ""

#the port of the main socket
main_thread_port_list = [8888]

#sockets range
sockets_min_range = 30000
sockets_max_range = 40000

# This is the worker Function
# It is used by the threading process to build a socket
# and then communcate to the API Fetch clients
# Then it parses the request and returns appropriately
def worker(thread_number,socket_number):
	print ("WOWOW")
	#build the socket 
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind((common_host,socket_number))
	s.listen(1)
	conn, addr = s.accept()

	#handle socket problems
	try:
		if verbose:
			print ('Connected by', addr)

		#build a bytestring of input
		data = bytes([])
		while True:
			new_data = conn.recv(1024)
			if not new_data: break
			data += new_data
		data = str(data,'UTF-8')
		if verbose:
			print ("Data Recieved: ",data)

		#turn those bytes into a json request
		try:
			json_data = json.loads(data)
		except Exception as e:
			print ("Failed to parse JSON")

		#check to make sure the request has a type 
		if "request_type" in json_data:

			# Queue blogs if they do not exist on the SET
			if json_data["request_type"] == "queue_blogs":

				#close and dereference the connection
				#This is done in order to not waste sockets
				conn.close()
				conn = None
				if verbose:
					print ("Thread " + str(thread_number) + " : Queue Blogs")

				#check if we have the blog list in our data
				if "blog_list" in json_data:
					print ("\tThread " + str(thread_number) + " : Add to Blog List")

					#go through the list of blogs and add them to our set and queue if need be
					for a in json_data["blog_list"]:

						if a not in hash_table:

							#add to the hash table
							try:
								hash_table.add(a)
							except Exception as e:
								print("Hash Table Fail: Length of Hash Table:"+str(len(hash_table)))
							print ("\t\tThread " + str(thread_number) + " : Adding : " + str(a))

							#add to the queue
							#queue is threadsafe by default
							#so it will block if being accessed
							frontier_queue.put(a)

			#Returns a new blog to use if we have on in the queue
			elif json_data["request_type"] == "new_blog_request":
				if verbose:
					print ("Thread " + str(thread_number) + " : Get New Blog")

				#wrapper for the queue
				try:

					#get the new blog from the queue
					new_blog = str(frontier_queue.get(timeout=.25))

					#build the json for the return string
					send_data = {	"worked":True,
									"request_type":"new_blog",
									"new_blog":new_blog,
								}

				#catch if the queue is empty or if something else happened
				except Exception as e:
					#return no new key
					send_data = {	"worked":False,
									"request_type":"new_blog",
								}
					pass

				#send the message
				print (send_data)	
				conn.send(str.encode(json.dumps(send_data)))
				conn.shutdown(socket.SHUT_WR)

			#get status report on data structures
			elif json_data["request_type"] == "status_report":
				if verbose:
					print ("Thread " + str(thread_number) + " : Status Report Request")

				#get the new sizes of the data structures
				current_hash_table_size = len(hash_table)
				current_queue_size = int(frontier_queue.qsize())

				#build the json for the return string
				send_data = {	"worked":True,
								"request_type":"status_report",
								"hash_table_size":current_hash_table_size,
								"queue_size":current_queue_size,
								}

				#send the message
				conn.send(str.encode(json.dumps(send_data)))
				conn.shutdown(socket.SHUT_WR)

			#give a new api key
			elif json_data["request_type"] == "api_key_get":

				# take a new api key off of the queue and send it out
				# then put it back on the stack
				try:
					new_api_key = str(api_key_queue.get(timeout=.25))
					send_data = {	"worked":True,
									"request_type":"api_key_get",
									"new_blog":new_api_key,
								}
					api_key_queue.put(new_api_key)
				except Exception as e:
					#return no new key
					send_data = {	"worked":False,
									"request_type":"api_key_get",
								}
					pass
				conn.send(str.encode(json.dumps(send_data)))
				conn.shutdown(socket.SHUT_WR)

			#make sure we catch all shitty requests
			else:
				#build the json for the return string
				send_data = {	"worked":False,
								"request_type":"NOT RECOGNIZED",
								}

				#send the message
				conn.send(str.encode(json.dumps(send_data)))
				conn.shutdown(socket.SHUT_WR)

	#catch all thread exceptions so that we know what happened
	except Exception as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print ("Error in Thread " + str(thread_number) + ": " + str(e) + " on line " + str(exc_tb.tb_lineno))

	#make sure the connection closed
	finally:
		if conn != None:
			conn.close()

# this function checks for open sockets within a range
# if the socket it tried didnt match, it just exits with false
def get_open_socket():
	s = None
	#watch for socket exceptions
	try:

		#check to see if a random port is open, and return it if it is
		try_port = random.randint(sockets_min_range,sockets_max_range)
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.bind((common_host,try_port))
		return True,try_port

	#catch any socket exceptions
	except Exception as e:
		return False,None

	#make sure the socket was closed so that we can use it later
	finally:
		if s != None:
			s.close()

# Build the main Socket
def main_socket_get():
	for port in main_thread_port_list:
		success = True
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.bind((common_host,port))
		except Exception as e:
			print("Could Not Link To Socket " + str(port))
			success = False
			pass
		if success:
			print("Linked To Socket " + str(port))
			return True,port, s
	return False, None, None


# This is the main function It handles the main thread
# This function Will hold the main socket that recieves socket requests
# Then it will return the opened socket thread to the client
# if the worker never recieves a connection, it just closes silently
# this allows us to operate under the assumption that the threads
# will not kill the program, and that the only code that can kill the program
# is the main function
def main(socket_num):

	#make sure we know what the exception means
	try:

		#document each thread by a number
		#this can be removed if we dont need it any more
		thread_count = 0
		conn = None

		# this is the loop that keeps the main socket open
		while True:

			#open the socket and start looking for a request
			return_val,port_num,s = main_socket_get()
			if not return_val:
				continue
			s.listen(1)
			conn, addr = s.accept()

			#build a bytestring from the incoming message
			#and then serialize that
			data = bytes([])
			while True:
				new_data = conn.recv(1024)
				if not new_data: break
				data += new_data
			data = str(data,'UTF-8')
			try:
				json_data = json.loads(data)
			except Exception as e:
				print ("Failed to parse JSON")
				conn.shutdown(socket.SHUT_WR)
				conn.close()
				conn = None
				s = None
				continue

			#check to make sure there is a request
			if "request_type" in json_data:

				#check to make sure it is a socket request
				if json_data["request_type"] == "socket_request":

					# Look for an open socket until we find one
					while True:

						#look for an open socket
						ret,opened_port = get_open_socket()
						
						# check to make sure that the socket worked correctly
						if ret:

							#build the socket thread and then start it off
							thread_count += 1
							t = threading.Thread(target=worker, args = (thread_count,opened_port,))
							t.start()

							#build our send json
							send_data = {	"worked":True,
											"request_type":"socket_request",
											"socket_number":opened_port,
							}

							# send the json to the API FETCHER
							conn.send(str.encode(json.dumps(send_data)))

							break

			# let the API Fetcher know that Its sending stupid shit
			else:
				send_data = {	"worked":False,
								"request_type":"NOT RECOGNIZED",
							}
				conn.send(str.encode(json.dumps(send_data)))

			#close the connection and dereference it
			conn.shutdown(socket.SHUT_WR)
			conn.close()
			conn = None
			s = None

	#make sure we Document a Main Socket Error
	except OSError as e:
		exc_type, exc_obj, exc_tb = sys.exc_info()
		fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
		print("---------------\nMain Thread Locked Up\n----------------\n")
		print(str(e) + " on Line " + str(exc_tb.tb_lineno))
		print ("\n----------------\n")
		print (str(opened_port))
		print ("\n----------------\n")
		conn = None

	# Close the Connection at the end of the program 
	# we want to make sure we do this any time we have a socket 
	# or socket accessor
	finally:
		print("Hash Table Length: " + str(len(hash_table)))
		print("Queue Length: " + int(frontier_queue.qsize()))
		if conn != None:
			conn.close()

#this runs the program if it is the main program
if __name__ == "__main__":
	#fill api key queue
	for a in open(api_key_file_location,'r').readlines():
		api_key_queue.put(a.strip('\n'))
	main(8888)