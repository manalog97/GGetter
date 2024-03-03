'''Program to join the (splitted) JSON files in a single JSON file
Logic:
1) IN THIS CASE I will split the name based on "_". This can vary depending from the single folder we are considering
2) I create a set of all the newsgroup present in the folder
3) For each newsgroup, I will load each json and save it in the final file.
4) I will print the count of messages and, if present, the count of duplicate messages, based on links
'''
import csv
import json
import os

if not os.path.exists('./uniti'):
    os.makedirs('uniti')
if not os.path.exists('./linkjson'):
    os.makedirs('linkjson')

#Finding all newgroup in folder
newsgroups=set()
for filename in os.listdir('json/'):
	if filename.endswith('.json'):
		newsgroup=filename.split('_')[0]
		newsgroups.add(newsgroup) #IN THIS CASE I AM SPLITTING BY _)
		#Debug print
		print(f'I am adding file: {filename} to newsgroup {newsgroup}')

#I find all the JSON files and add to the new JSON file

for newsgroup in newsgroups:
	print(f"Starting newsgroup {newsgroup}...")
	dataset=[]
	link_set=set()
	items_count=0
	filecount=0
	#Creating the new JSON
	json_write=open(os.path.join('uniti',newsgroup+'.json'),'a')
	
	
	for filename in os.listdir('json/'):
		if filename.startswith(newsgroup+'_') and filename.endswith('.json'):
			with open(os.path.join('json',filename),'r') as jsonread:
				print(f"Opening: {filename}")	
				data=json.load(jsonread)
				for item in data:
					dataset.append(item)
					link_set.add(item['original_url'])
					items_count+=1
				filecount+=1
				print(f"File number {filecount}")
	json.dump(dataset, json_write)
	print(f"Newsgroup: {newsgroup}, Merged files: {filecount}, Conversations found: {items_count}, Unique conversations: {len(link_set)}")
	with open(os.path.join('linkjson',newsgroup+'.csv'),'a') as csvlinkfile:
		writer=csv.writer(csvlinkfile)
		for link in link_set:
			writer.writerow([link])
	json_write.close()
print("\nAll JSON files present in json/ folder are now merged in the folder merged/")
