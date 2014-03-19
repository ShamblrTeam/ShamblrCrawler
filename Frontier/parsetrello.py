#Get api-keys.json from the trello board by hitting share and more and then export json
#Put into same directory as this, ?????, done
import json
data_file = open("api-keys.json")
outfile = open("API_KEY_LIST.data", 'w')
data = json.load(data_file)
items = data["checklists"][0]["checkItems"]
for item in items:
  outfile.write(item["name"]+ "\n")
