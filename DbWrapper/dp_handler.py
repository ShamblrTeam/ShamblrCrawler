import psycopg2
import socket
import json
import random
import threading
import sys
import os
import time


#Handles if the program should print stuff
verbose = False 

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

			if json_data["request_type"] == "save_blogs":

				# get the blogs and the links from the request
				try:
					insert_values = []
					blog_list = json_data["blogs"]
					link_list = json_data["links"]
					if len(blog_list) != len(link_list):
						raise Exception
					
					# append these together into a list
					for a in range(len(blog_list)):
						insert_values.append({"name":blog_list[a],"link":link_list[a]})

					#now build the db stuff and insert into the db
					conn_string = "host='localhost' dbname='cs585' user='cs585' "
					db_conn = psycopg2.connect(conn_string)
					cursor = db_conn.cursor()

					# this is more efficient than commit a lot of transactions.
					# @see: http://wiki.postgresql.org/wiki/Psycopg2_Tutorial
					try:
						cursor.executemany("""INSERT INTO blog VALUES (%(name)s, %(link)s)""", insert_values)
						db_conn.commit()
					except Exception as e:
						db_conn.rollback()
						pass

					cursor.close()
					db_conn.close()
					send_data = {	"worked":True,
								"request_type":"save_blogs",
								}

				except Exception as e:
					print ("WOW: " + str(e))
					send_data = {	"worked":False,
								"request_type":"save_blogs",
								}

			elif json_data["request_type"] == "save_posts":
			
				# get the blogs and the links from the request
				try:
					insert_values = []
					post_list = json_data["posts"]

					#now build the db stuff and insert into the db
					conn_string = "host='localhost' dbname='cs585' user='cs585' "
					db_conn = psycopg2.connect(conn_string)
					cursor = db_conn.cursor()

					# add DB timestamps to posts
					for a in range(len(post_list)):
						t = post_list[a]["timestamp_created"]
						post_list[a]["db_timestamp_created"] = psycopg2.Timestamp(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)

					try:
						cursor.executemany("""INSERT INTO post VALUES 
							(%(post_id)d, %(link)s, %(blog_name)s, 
							 %(type)s, %(content)s, %(db_timestamp_created)s, %(note_count)d)""", post_list)
						db_conn.commit()
					except Exception as e:
						db_conn.rollback()
						pass

					cursor.close()
					db_conn.close()
					send_data = {	"worked":True,
								"request_type":"save_blogs",
								}

				except Exception as e:
					print ("WOW: " + str(e))
					send_data = {	"worked":False,
								"request_type":"save_blogs",
								}

			elif json_data["request_type"] == "save_notes":
				
				# get the blogs and the links from the request
				try:
					insert_values = []
					note_list = json_data["notes"]

					#now build the db stuff and insert into the db
					conn_string = "host='localhost' dbname='cs585' user='cs585' "
					db_conn = psycopg2.connect(conn_string)
					cursor = db_conn.cursor()
					for a in note_list:
						try:
							t = time.gmtime(int(a["timestamp"]))
							cursor.execute("insert into note values(%s,%s,%s,%s);",
									(	a["post_id"],
										a["type"],
										psycopg2.Timestamp(t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec),
										a["blog_name"]
									)
								)
							db_conn.commit()
						except Exception as e:
							print ("sdfsdfdsf" , str(e))
							db_conn.rollback()
							pass
					db_conn.commit()
					cursor.close()
					db_conn.close()
					send_data = {	"worked":True,
								"request_type":"save_blogs",
								}

				except Exception as e:
					print ("WOW: " + str(e))
					send_data = {	"worked":False,
								"request_type":"save_blogs",
								}


			#make sure we catch all shitty requests
			else:
				#build the json for the return string
				send_data = {	"worked":True,
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
		print ("Trying Port " + str(try_port))
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.bind((common_host,try_port))
		return True,try_port

	#catch any socket exceptions
	except Exception as e:
		print (e)
		return False,None

	#make sure the socket was closed so that we can use it later
	finally:
		if s != None:
			s.close()
			s = None


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

				else:
					print ("WAT")

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
		if conn != None:
			conn.close()


if __name__ == "__main__":
	main(8888)