# Echo client program
import socket
import json
import time
import random
import sys


def do_stuff():
	try:
		PORT_INDEX = 0
		while True:
			HOST = 'localhost'                  # The remote host
			PORTS = [8888]  # The same port as used by the server
			success = True
			try:
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect((HOST, PORTS[PORT_INDEX]))
			except Exception as e:
				print("Could Not Link To Socket " + str(PORTS[PORT_INDEX]))
				success = False
				pass
			if not success:
				PORT_INDEX = (PORT_INDEX + 1)%len(PORTS)
				continue
			print("Linked To Socket " + str(PORTS[PORT_INDEX]))
			s.send(str.encode(json.dumps({"request_type":"socket_request"})))
			s.shutdown(socket.SHUT_WR)
			data = bytes([])
			while True:
				new_data = s.recv(1024)
				if not new_data: break
				data += new_data
			s.close()
			data = str(data,'UTF-8')
			print (data)
			json_data = json.loads(data)			

			#change the input data to your liking			

			#This one is for adding to the queue
			def random_name():
				return ''.join([chr(random.randint(65,90))]+[chr(random.randint(97,122)) for x in range(10)])			

			names = [random_name() for x in range(50)]
			#input_data = {	"request_type":"queue_blogs","blog_list": names,}			

			#This one is for taking from the queue
			#input_data = {	"request_type":"new_blog_request",}			

			#This one puts in blogs
			input_data = {	"request_type":"save_blogs",
							"blogs":["wewewewewe","har","wowoweee"],
							"links":["sdfsdfsdf","http://harharharharhar","http://sdfsdf"],
						}	
			
			#puts in posts
			input_data = {	"request_type":"save_posts",
							"blogs":["wewewewewe","har","wowoweee"],
							"links":["sdfsdfsdf","http://harharharharhar","http://sdfsdf"],
						}	

			#Puts in notes
			input_data = {	"request_type":"save_notes",
							"notes": [{	"post_id":1,"type":"text""timestamp":1000100,"blog_name":"Wow",}],
						}	
					

			if "request_type" in json_data:
				if json_data["request_type"] == "socket_request":
					if "worked" in json_data:
						if json_data["worked"] == True:
							s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
							s.connect((HOST, int(json_data["socket_number"])))
							send_data = json.dumps(input_data)
							print("Send Data: " + str(send_data))
							s.send(str.encode(send_data))
							s.shutdown(socket.SHUT_WR)
							data = bytes([])
							while True:
								new_data = s.recv(1024)
								if not new_data: break
								data += new_data
							data = str(data,'UTF-8')
							if data != "":
								json_data = json.loads(data)
							print(json_data)
							s.close()
			sys.exit()
	except Exception as e:
		return True

while True:
	time.sleep(.1)
	do_stuff()
	sys.exit()