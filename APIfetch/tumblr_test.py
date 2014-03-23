
# for the API Fetch
import urllib.request
import json
import sys

# for the socket check
import socket
import random
import time


#get a new blog from the frontier
def get_blog_from_frontier(host,port):
	#connect to the frontier to get a socket to communicate with
	connection_success = False
	connection_success_fails = 0
	while not connection_success:
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((host, port))
			connection_success = True
		except Exception as e:
			print("Could Not Link To Socket " + str(port))
			connection_success_fails += 1
			if connection_success_fails > 5:
				print("Max Link Fails to socket " + str(port))
				return False,None
			time.sleep(.1*random.randint(1,5))
			pass

	#send the json request for a socket
	s.send(str.encode(json.dumps({"request_type":"socket_request"})))
	s.shutdown(socket.SHUT_WR)

	#recieve the response
	try:
		data = bytes([])
		while True:
			new_data = s.recv(1024)
			if not new_data: break
			data += new_data
		s.close()
		data = str(data,'UTF-8')
	except Exception as e:
		print("Didn't recieve response from Frontier")
		return False,None
	
	#load the data using json load
	try:
		json_data = json.loads(data)
	except Exception as e:
		print("Json Return on Socket Request Malformed" + str(data))
		return False,None

	#build our queue_blogs json
	input_data = {"request_type":"new_blog_request",}	

	# get an available socket number
	try:
		if not json_data["worked"]:
			raise Exception("Socket Request Failed")
		queue_blogs_socket_number = (json_data["socket_number"])
	except Exception as e:
		print(e)
		return False,None

	#open that socket
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host, queue_blogs_socket_number))
	except Exception as e:
		print("Payload Socket Didnt Open: " + str(e))
		return False,None

	#send it our payload
	try:
		send_data = json.dumps(input_data)
		s.send(str.encode(send_data))
		s.shutdown(socket.SHUT_WR)
	except Exception as e:
		print("Could Not Send Payload: " + str(e))
		return False,None

	#recieve the response
	data = bytes([])
	while True:
		new_data = s.recv(1024)
		if not new_data: break
		data += new_data
	s.close()
	try:
		data = str(data,'UTF-8')
	except Exception as e:
		print("Bytes Return on Socket Request Malformed")
		return False,None
	
	#load the data using json load
	try:
		json_data = json.loads(data)
	except Exception as e:
		print("Json Return on Socket Request Malformed" + str(data))
		return False,None

	#extract the new blog from the json
	try:
		if not json_data["worked"]:
			return False,None
		return True,json_data["new_blog"]
	except Exception as e:
		print("Json Return on New Blog Request Failed: " + str(e))
		return False, None

# get the blogs from notes
def get_blogs_from_notes(blog_name,api_key,offset=None,limit=None):

	def form_post(post):
		formed_post = {}
		formed_post["blog_name"] = post["blog_name"]
		formed_post["post_id"] = post["id"]
		formed_post["post_link"] = post["post_url"]
		formed_post["timestamp"] = post["timestamp"]
		formed_post["note_count"] = post["note_count"]
		formed_post["tags"] = post["tags"]
		formed_post["type"] = post["type"]

		formed_post["title"] = ""
		if "title" in post:
			formed_post["title"] = post["title"]
		else:
			formed_post["title"] = None

		try:
			if formed_post["type"] == "text":
				formed_post["content"] = str(post["body"])
			elif formed_post["type"] == "photo":
				#consider photoset
				formed_post["title"] = post["caption"]
				formed_post["content"] = post["photos"][0]["original_size"]["url"]
			elif formed_post["type"] == "quote":
				formed_post["content"] = str(post["text"]) + str(post["source"])
			elif formed_post["type"] == "link":
				formed_post["content"] = post["url"]
			elif formed_post["type"] == "chat":
				formed_post["content"] = str(post["body"])
			elif formed_post["type"] == "audio":
				formed_post["content"] = post["audio_url"]
			elif formed_post["type"] == "video":
				formed_post["title"] = post["caption"]
				formed_post["content"] = post["permalink_url"]
			elif formed_post["type"] == "answer":
				formed_post["content"] = "WOW"
			else:
				raise Exception
		except Exception as e:
			#answer posts are going to be thrown out
			print(formed_post)
			print("Invalid post type found, something bad has happened")
			return False
		return formed_post

	def get_notes_from_post(post,postid):
		note_list = []
		for item in post["notes"]:
			note = {}
			note["timestamp"] = item["timestamp"]
			note["blog_name"] = item["blog_name"]
			note["type"] = item["type"]
			note["post_id"] = postid
			note_list.append(note)
		return note_list


	#return list
	blogs = []
	links = []
	posts = []
	note_list = []

	# build url for api
	try:
		authentication = '?api_key=' + api_key
		url = 'http://api.tumblr.com/v2/blog/' + blog_name +".tumblr.com"
		parameters = "&notes_info=true&reblog_info=true"
		if limit != None:
			parameters += '&limit='+str(int(limit))
		if offset != None:
			parameters += '&offset='+str(int(offset))
		url += '/posts'+ authentication + parameters
	except Exception as e:
		print("Could not build")
		return False,[],[],[],[]

	# retrieve html
	try:
		response = urllib.request.urlopen(url)
		html = response.read()
	except Exception as e:
		print("Could not get Html",str(url))
		return False,[],[],[],[]

	# parse html into json
	try:
		x = json.loads(html.decode('UTF-8'))
	except Exception as e:
		print("Could not Parse to Json")
		return False,[],[],[],[]

	# look for "unique blogs"
	try:
		if "response" in x:
			if "posts" in x["response"]:
				for a in x["response"]["posts"]:
					post = form_post(a)
					print(post)
					if post != False:
						posts.append(post)
						if "notes" in a:
							note_list += get_notes_from_post(a,post["post_id"])
							for b in a["notes"]:
								if "blog_name" in b:
									if b["blog_name"] not in blogs:
										blogs.append(b["blog_name"])
										links.append(b["blog_url"])
	except Exception as e:
		print("Could Not Parse Json into Unique Blogs")
		return False,[],[],[],[]

	# return list of unique blogs in a list
	print (note_list)
	return True,list(blogs),list(links),list(posts),list(note_list)

# sends the blogs to the frontier
def send_blogs_to_DB(host,port,blogs,links):

	#connect to the frontier to get a socket to communicate with
	connection_success = False
	connection_success_fails = 0
	while not connection_success:
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((host, port))
			connection_success = True
		except Exception as e:
			print("Could Not Link To Socket " + str(port))
			connection_success_fails += 1
			if connection_success_fails > 5:
				print("Max Link Fails to socket " + str(port))
				return False
			time.sleep(.1*random.randint(1,5))
			pass

	#send the json request for a socket
	s.send(str.encode(json.dumps({"request_type":"socket_request"})))
	s.shutdown(socket.SHUT_WR)

	try:
		#recieve the response
		data = bytes([])
		while True:
			new_data = s.recv(1024)
			if not new_data: break
			data += new_data
		s.close()
	except Exception as e:
		print("Socket Was Reset")
		return False
	try:
		data = str(data,'UTF-8')
	except Exception as e:
		print("Bytes Return on Socket Request Malformed")
		return False
	
	#load the data using json load
	try:
		json_data = json.loads(data)
	except Exception as e:
		print("Json Return on Socket Request Malformed" + str(data))
		return False

	#build our queue_blogs json
	input_data = 	{
						"request_type": "save_blogs",
						"blogs": blogs,
						"links":links,
					}

	# get an available socket number
	try:
		if not json_data["worked"]:
			raise Exception("Socket Request Failed")
		queue_blogs_socket_number = (json_data["socket_number"])
	except Exception as e:
		print(e)
		return False

	#open that socket
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host, queue_blogs_socket_number))
	except Exception as e:
		print("Payload Socket Didnt Open: " + str(e))
		return False

	#send it our payload
	try:
		send_data = json.dumps(input_data)
		s.send(str.encode(send_data))
		s.shutdown(socket.SHUT_WR)
		s.close()
	except Exception as e:
		print("Could Not Send Payload: " + str(e))
		return False
	return True

#send posts to db
def send_posts_to_DB(host,port,posts):

	#connect to the frontier to get a socket to communicate with
	connection_success = False
	connection_success_fails = 0
	while not connection_success:
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((host, port))
			connection_success = True
		except Exception as e:
			print("Could Not Link To Socket " + str(port))
			connection_success_fails += 1
			if connection_success_fails > 5:
				print("Max Link Fails to socket " + str(port))
				return False
			time.sleep(.1*random.randint(1,5))
			pass

	#send the json request for a socket
	s.send(str.encode(json.dumps({"request_type":"socket_request"})))
	s.shutdown(socket.SHUT_WR)

	try:
		#recieve the response
		data = bytes([])
		while True:
			new_data = s.recv(1024)
			if not new_data: break
			data += new_data
		s.close()
	except Exception as e:
		print("Socket Was Reset")
		return False
	try:
		data = str(data,'UTF-8')
	except Exception as e:
		print("Bytes Return on Socket Request Malformed")
		return False
	
	#load the data using json load
	try:
		json_data = json.loads(data)
	except Exception as e:
		print("Json Return on Socket Request Malformed" + str(data))
		return False

	#build our queue_blogs json
	input_data = 	{
						"request_type": "save_posts",
						"posts": posts,
					}

	# get an available socket number
	try:
		if not json_data["worked"]:
			raise Exception("Socket Request Failed")
		queue_blogs_socket_number = (json_data["socket_number"])
	except Exception as e:
		print(e)
		return False

	#open that socket
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host, queue_blogs_socket_number))
	except Exception as e:
		print("Payload Socket Didnt Open: " + str(e))
		return False

	#send it our payload
	try:
		send_data = json.dumps(input_data)
		s.send(str.encode(send_data))
		s.shutdown(socket.SHUT_WR)
		s.close()
	except Exception as e:
		print("Could Not Send Payload: " + str(e))
		return False
	return True

#send notes to db
def send_notes_to_DB(host,port,notes):

	#connect to the frontier to get a socket to communicate with
	connection_success = False
	connection_success_fails = 0
	while not connection_success:
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((host, port))
			connection_success = True
		except Exception as e:
			print("Could Not Link To Socket " + str(port))
			connection_success_fails += 1
			if connection_success_fails > 5:
				print("Max Link Fails to socket " + str(port))
				return False
			time.sleep(.1*random.randint(1,5))
			pass

	#send the json request for a socket
	s.send(str.encode(json.dumps({"request_type":"socket_request"})))
	s.shutdown(socket.SHUT_WR)

	try:
		#recieve the response
		data = bytes([])
		while True:
			new_data = s.recv(1024)
			if not new_data: break
			data += new_data
		s.close()
	except Exception as e:
		print("Socket Was Reset")
		return False
	try:
		data = str(data,'UTF-8')
	except Exception as e:
		print("Bytes Return on Socket Request Malformed")
		return False
	
	#load the data using json load
	try:
		json_data = json.loads(data)
	except Exception as e:
		print("Json Return on Socket Request Malformed" + str(data))
		return False

	#build our queue_blogs json
	input_data = 	{
						"request_type": "save_notes",
						"notes": notes,
					}

	# get an available socket number
	try:
		if not json_data["worked"]:
			raise Exception("Socket Request Failed")
		queue_blogs_socket_number = (json_data["socket_number"])
	except Exception as e:
		print(e)
		return False

	#open that socket
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host, queue_blogs_socket_number))
	except Exception as e:
		print("Payload Socket Didnt Open: " + str(e))
		return False

	#send it our payload
	try:
		send_data = json.dumps(input_data)
		s.send(str.encode(send_data))
		s.shutdown(socket.SHUT_WR)
		s.close()
	except Exception as e:
		print("Could Not Send Payload: " + str(e))
		return False
	return True

# sends the blogs to the frontier
def send_blogs_to_frontier(host,port,blogs):

	#connect to the frontier to get a socket to communicate with
	connection_success = False
	connection_success_fails = 0
	while not connection_success:
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((host, port))
			connection_success = True
		except Exception as e:
			print("Could Not Link To Socket " + str(port))
			connection_success_fails += 1
			if connection_success_fails > 5:
				print("Max Link Fails to socket " + str(port))
				return False
			time.sleep(.1*random.randint(1,5))
			pass

	#send the json request for a socket
	s.send(str.encode(json.dumps({"request_type":"socket_request"})))
	s.shutdown(socket.SHUT_WR)

	try:
		#recieve the response
		data = bytes([])
		while True:
			new_data = s.recv(1024)
			if not new_data: break
			data += new_data
		s.close()
	except Exception as e:
		print("Socket Was Reset")
		return False
	try:
		data = str(data,'UTF-8')
	except Exception as e:
		print("Bytes Return on Socket Request Malformed")
		return False
	
	#load the data using json load
	try:
		json_data = json.loads(data)
	except Exception as e:
		print("Json Return on Socket Request Malformed" + str(data))
		return False

	#build our queue_blogs json
	input_data = 	{
						"request_type": "queue_blogs",
						"blog_list": blogs,
					}

	# get an available socket number
	try:
		if not json_data["worked"]:
			raise Exception("Socket Request Failed")
		queue_blogs_socket_number = (json_data["socket_number"])
	except Exception as e:
		print(e)
		return False

	#open that socket
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host, queue_blogs_socket_number))
	except Exception as e:
		print("Payload Socket Didnt Open: " + str(e))
		return False

	#send it our payload
	try:
		send_data = json.dumps(input_data)
		s.send(str.encode(send_data))
		s.shutdown(socket.SHUT_WR)
		s.close()
	except Exception as e:
		print("Could Not Send Payload: " + str(e))
		return False
	return True

def get_api_key_from_frontier(host,port):

	connection_success = False
	connection_success_fails = 0
	while not connection_success:
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((host, port))
			connection_success = True
		except Exception as e:
			print("Could Not Link To Socket " + str(port))
			connection_success_fails += 1
			if connection_success_fails > 5:
				print("Max Link Fails to socket " + str(port))
				return False,None
			time.sleep(random.randrange(.1,.5))
			pass

	#send the json request for a socket
	s.send(str.encode(json.dumps({"request_type":"socket_request"})))
	s.shutdown(socket.SHUT_WR)

	#recieve the response
	data = bytes([])
	while True:
		new_data = s.recv(1024)
		if not new_data: break
		data += new_data
	s.close()
	try:
		data = str(data,'UTF-8')
	except Exception as e:
		print("Bytes Return on Socket Request Malformed")
		return False,None
	
	#load the data using json load
	try:
		json_data = json.loads(data)
	except Exception as e:
		print("Json Return on Socket Request Malformed" + str(data))
		return False,None

	#build our queue_blogs json
	input_data = {"request_type":"api_key_get",}	

	# get an available socket number
	try:
		if not json_data["worked"]:
			raise Exception("Socket Request Failed")
		queue_blogs_socket_number = (json_data["socket_number"])
	except Exception as e:
		print(e)
		return False,None

	#open that socket
	try:
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((host, queue_blogs_socket_number))
	except Exception as e:
		print("Payload Socket Didnt Open: " + str(e))
		return False,None

	#send it our payload
	try:
		send_data = json.dumps(input_data)
		s.send(str.encode(send_data))
		s.shutdown(socket.SHUT_WR)
	except Exception as e:
		print("Could Not Send Payload: " + str(e))
		return False,None

	#recieve the response
	data = bytes([])
	while True:
		new_data = s.recv(1024)
		if not new_data: break
		data += new_data
	s.close()
	try:
		data = str(data,'UTF-8')
	except Exception as e:
		print("Bytes Return on Socket Request Malformed")
		return False,None
	
	#load the data using json load
	try:
		json_data = json.loads(data)
	except Exception as e:
		print("Json Return on Socket Request Malformed" + str(data))
		return False,None

	#extract the new blog from the json
	try:
		if not json_data["worked"]:
			return False,None
		return True,json_data["new_api_key"]
	except Exception as e:
		print("Json Return on New API Key Failed: " + str(e))
		return False, None

if __name__ == "__main__":

	host = 'helix.vis.uky.edu'
	port = 8888

	db_host = '172.31.40.208'
	db_port = 8888


	# first try to get the api key from 

	try:
		ret = False
		while not ret:
			ret,api_key = get_api_key_from_frontier(host,port)
	except Exception as e:
		print ("Could not get an API Key") 

	try:
		#first send a starting blog to the frontier
		fail_count = 0
		while True:
			seed_blogs = ["just1boi"]
			ret = send_blogs_to_frontier(host,port,seed_blogs)
			if ret:
				break
			fail_count += 1
			if fail_count > 10:
				print("Failed on Send Blogs, Number of Blogs Visited: " + str(blogs_visited))
				sys.exit()
			time.sleep(.1)
	except Exception as e:
		print("Could not add to the queue")
		sys.exit()

	try:

		blogs_visited = 0
		# main loop
		while True:

			#get a new blog from the frontier
			fail_count = 0
			new_blog = ''
			print("Get a New Blog From our Frontier")
			while True:
				ret,new_blog = get_blog_from_frontier(host,port)
				if ret:
					break
				fail_count += 1
				if fail_count > 10:
					print("Failed on Frontier New Blog Access, Number of Blogs Visited: " + str(blogs_visited))
					sys.exit()
				time.sleep(.1)

			#get the blogs from the notes of the new blog
			fail_count = 0
			insert_blogs = []
			print("Get Notes From Tumblr")
			while True:
				ret,insert_blogs,insert_links,insert_posts,insert_notes = get_blogs_from_notes(new_blog,api_key)
				if ret:
					break
				fail_count += 1
				if fail_count > 10:
					print("Failed on tumblr access, Number of Blogs Visited: " + str(blogs_visited))
					sys.exit()
				time.sleep(1)

			#insert blogs into db
			fail_count = 0
			print("Insert New Blogs to our database")
			while True:
				ret = send_blogs_to_DB(db_host,db_port,insert_blogs,insert_links)
				if ret:
					break
				fail_count += 1
				if fail_count > 10:
					print("Failed on Send Blogs, Number of Blogs Visited: " + str(blogs_visited))
					sys.exit()
				time.sleep(.1)

			blogs_visited += 1
			if blogs_visited %10 == 0:
				print("Visited " + str(blogs_visited) + " blogs successfully")

			#insert posts into db
			fail_count = 0
			print("Insert New Posts to our database")
			while True:
				ret = send_posts_to_DB(db_host,db_port,insert_posts)
				if ret:
					break
				fail_count += 1
				if fail_count > 10:
					print("Failed on Send Posts, Number of Blogs Visited: " + str(blogs_visited))
					sys.exit()
				time.sleep(.1)

			blogs_visited += 1
			if blogs_visited %10 == 0:
				print("Visited " + str(blogs_visited) + " blogs successfully")

			#insert notes into DB
			fail_count = 0
			print("Insert New Notes to our database")
			while True:
				print (insert_notes)
				ret = send_notes_to_DB(db_host,db_port,insert_notes)
				if ret:
					break
				fail_count += 1
				if fail_count > 10:
					print("Failed on Send Notes, Number of Blogs Visited: " + str(blogs_visited))
					sys.exit()
				time.sleep(.1)

			blogs_visited += 1
			if blogs_visited %10 == 0:
				print("Visited " + str(blogs_visited) + " blogs successfully")


			#insert the blogs into our frontier
			fail_count = 0
			print("Insert New Blogs to our Frontier")
			while True:
				ret = send_blogs_to_frontier(host,port,insert_blogs)
				if ret:
					break
				fail_count += 1
				if fail_count > 10:
					print("Failed on Send Blogs, Number of Blogs Visited: " + str(blogs_visited))
					sys.exit()
				time.sleep(.1)

			blogs_visited += 1
			if blogs_visited %10 == 0:
				print("Visited " + str(blogs_visited) + " blogs successfully")

	finally:
		print("Ending: " + str(blogs_visited))
