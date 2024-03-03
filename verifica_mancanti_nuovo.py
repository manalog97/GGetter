import json
import csv
import os

if not os.path.exists('./mancanti_finali'):
    os.makedirs('mancanti_finali')

# Set up a log file to record missing CSV and JSON files
log_file = open("missing_files_log_FINALE.txt", "a")

# Define a function to log missing files
def log_missing_file(newsgroup, file_type):
    log_file.write(f"{file_type},{newsgroup}\n")

for filename in os.listdir('JSON'):
    if filename.endswith('json'):
        with open(os.path.join('JSON', filename), 'r') as jsonfile:
            newsgroup = filename.split('.json')[0]
            linkset_json = set()
            linkset_csv_originale = set()

            try:
                dati = json.load(jsonfile)
            except json.JSONDecodeError:
                log_missing_file(newsgroup, "JSON [DECODEERROR]")
                continue

            for item in dati:
                linkset_json.add(item['original_url'])
            print(f"Conversazioni in {newsgroup} = {len(linkset_json)}")         
            csv_file_path = os.path.join('Nuovo_Scrape', 'lista_link_'+newsgroup + '.csv')
            #Now let's get all the new JSONs from mancanti and mancanti2
            for additionaljson in os.listdir('mancanti'):
                if additionaljson.startswith(newsgroup+'.0') and additionaljson.endswith('.json'):
                    with open(os.path.join('mancanti',additionaljson), 'r') as additionaljsonfile:
                        additionaldata=json.load(additionaljsonfile)
                        for additionalitem in additionaldata:
                            linkset_json.add(additionalitem['original_url'])
                        print(f"Aperto in aggiunta a {newsgroup} il file {additionaljson}")
                        print(f"Conversazioni in {newsgroup} = {len(linkset_json)}")   
            for additionaljson in os.listdir('mancanti2'):
                if additionaljson.startswith(newsgroup+'.0') and additionaljson.endswith('.json'):
                    with open(os.path.join('mancanti2',additionaljson), 'r') as additionaljsonfile:
                        additionaldata=json.load(additionaljsonfile)
                        for additionalitem in additionaldata:
                            linkset_json.add(additionalitem['original_url'])      
                        print(f"Aperto in aggiunta a {newsgroup} il file {additionaljson}")    
                        print(f"Conversazioni in {newsgroup} = {len(linkset_json)}")        
            try:
                with open(csv_file_path, 'r') as csvoriginale_file:
                    csvreader = csv.reader(csvoriginale_file)
                    for row in csvreader:
                        linkset_csv_originale.add(row[0])
            except FileNotFoundError:
                log_missing_file(newsgroup, "CSV")
                continue

            mancanti = linkset_csv_originale - linkset_json
            print(f"Ci sono {len(mancanti)} link mancanti in {newsgroup}")

            with open(os.path.join('mancanti_finali', newsgroup), 'a') as mancanti_file:
                csvwriter = csv.writer(mancanti_file)
                for link in mancanti:
                    csvwriter.writerow([link])

            # Check if the corresponding JSON file exists
            if not os.path.exists(os.path.join('JSON', f"{newsgroup}.json")):
                log_missing_file(newsgroup, "JSON")
                log_missing_file(newsgroup, "JSON")
# Close the log file
log_file.close()
