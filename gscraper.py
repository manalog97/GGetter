###Gscraper.py###

import os
from bs4 import BeautifulSoup
import csv
import requests
from datetime import datetime
import re
import time
import json 
from selenium.webdriver import Firefox
from selenium.webdriver.firefox.service import Service
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException
from selenium import webdriver
import threading
import queue
import concurrent.futures
#Headers
headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:100.0) Gecko/20100101 Firefox/100.0"}
timeout=5

#Months
MONTHS = {
    'Jan': 1,
    'Feb': 2,
    'Mar': 3,
    'Apr': 4,
    'May': 5,
    'Jun': 6,
    'Jul': 7,
    'Aug': 8,
    'Sep': 9,
    'Oct': 10,
    'Nov': 11,
    'Dec': 12
}

def download_thread_selenium(url,driver):
    try:
        driver.get(url)
        #A method for scrolling the page

        # Get scroll height.
        last_height = driver.execute_script("return document.body.scrollHeight")

        while True:

            # Scroll down to the bottom.
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait to load the page.
            time.sleep(1.3)

            # Calculate new scroll height and compare with last scroll height.
            new_height = driver.execute_script("return document.body.scrollHeight")

            if new_height == last_height:
                break

        last_height = new_height
        # Get the page source
        html_content = driver.page_source
        return html_content
    
    except:
         return 1

def download_thread(url,newsgroup,newsgroup_data,driver):
    try:
        r = requests.get(url,headers=headers,timeout=timeout)
        if r.status_code==200:
            html = r.text
            exit_code, exit_string = extract_thread(html,newsgroup,url,newsgroup_data,driver)
            if  exit_code == 0: #Sistemare il ritorno da funzione
                 return 0,'',''
            else:
                 return 1,exit_string,''
        else:
            return 1,'HTTP_CODE',r.status_code
    except requests.exceptions.Timeout as e:
            return 1,'Timeout',''
    except requests.ConnectionError as e:
            return 1,'ConnectionError',''
    except requests.HTTPError as e:
            return 1,'ConnectionError',''
    #except:
            #with open("report.txt", 'a') as r:
               #r.write(f"\n{timestamp()} Iteration: {attempt} Fully downloaded: {len(complete_set-failed_set)} Partially or not downloaded: {len(failed_set)}")
            #return 1,'Other exception',''

    pass
    return (html)

def extract_thread(html,newsgroup,original_url,newsgroup_data,driver):
    splitted=newsgroup.split('_')
    if len(splitted)==2:
        newsgroup=splitted[0]    
    try:
        soup = BeautifulSoup(html, 'html.parser')

        discussion_div = soup.find('div', class_='RBM0ic')

        title_div = discussion_div.find('div', class_='ThqSJd')
        title = title_div.text.strip()

        messages = []
        message_divs = discussion_div.find_all('div', class_='ptW7te')
        if  len(message_divs) > 98:
           # print("Number of <div> elements found, I load Firefox:", len(message_divs))
            #Cazzo, non sto prendendo tutti i div, devo usare selenium
            try:
                html = download_thread_selenium(original_url,driver)
                soup = BeautifulSoup(html, 'html.parser')

                discussion_div = soup.find('div', class_='RBM0ic')

                title_div = discussion_div.find('div', class_='ThqSJd')
                title = title_div.text.strip()

                messages = []
                message_divs = discussion_div.find_all('div', class_='ptW7te')  
               # print("Number of <div> elements found after Firefox:", len(message_divs))
            except Exception as e:
                return 1,'str(e)'

        
        for message_div in message_divs:
            author = message_div.find_previous('div', class_='LgTNRd').text.strip()
            date_string = message_div.find_previous('div', class_='ELCJ4d').text.strip()

            # REGEX per recuperare le date
            match = re.search(r'(\w{3}) (\d{1,2}), (\d{4}), (\d{1,2}:\d{2}:\d{2})\s?(\w{2})', date_string)
            if match:
                month_abbr = match.group(1)
                month = MONTHS.get(month_abbr)
                day = int(match.group(2))
                year = int(match.group(3))
                time = match.group(4)
                am_pm = match.group(5)
            else:
                month = day = year = time = am_pm = None

            if None in (month, day, year, time, am_pm):
                # Se ci sono problemi con le regex, non perdo nulla lo stesso
                date_problem = date_string
                month = day = year = time = am_pm = None
            else:
                date_problem = None

            text = message_div.get_text(separator="\n")
            messages.append({'author': author, 'month': month, 'day': day, 'year': year, 'time': time, 'am_pm': am_pm, 'text': text, 'dt': date_problem})

            thread_dictionary = {
                    'title': title,
                    'newsgroup': newsgroup,
                    'original_url': original_url,
                    'messages': messages
            }
        newsgroup_data.append(thread_dictionary)
        #print(f"\nEstratto un thread di {newsgroup} con {len(messages)} messaggi")
        return 0,''
    except Exception as e:
        return 1,'str(e)'

def save_to_db(newsgroup_data, newsgroup, iteration, rerun):
    #Gestisco gli eventuali SPLIT
    splitted=newsgroup.split('_')
    if len(splitted)==2:
        newsgroup=splitted[0]
        filename=f'json/{newsgroup}_{splitted[1]}.{rerun}.{iteration}.json'
    else:
        filename=f'json/{newsgroup}.{rerun}.{iteration}.json'
    with open(filename, 'w') as json_file:
        json.dump(newsgroup_data, json_file)

from selenium import webdriver
import csv
import os

def get_newsgroup(newsgroup, attempt):
    print(f"\nStarting newsgroup {newsgroup}")
    options = webdriver.FirefoxOptions() #Needed when messages > 100
    options.add_argument('--headless')
    driver = webdriver.Firefox(executable_path="/usr/bin/geckodriver", options=options)
    successes = 0
    failures = 0
    newsgroup_data = []
    try:
        if (attempt==0):
            filename=newsgroup+'.csv'
        else:
            filename='failed/'+newsgroup+'_failed_'+str(attempt-1)+'.csv'

        with open(filename, 'r', encoding='utf-8') as csvfile:
            try:
                csvreader = csv.reader(csvfile)
                for thread in csvreader: # thread[0] = URL
                    try:
                        exit_code, exit_string1, exit_string2 = download_thread(thread[0], newsgroup, newsgroup_data, driver)
                        if exit_code == 0:
                            successes += 1
                        else:
                            failures += 1
                            # Structure of failurefile: link, type of error, additional info
                            with open(os.path.join("failed", newsgroup + '_failed_' + str(attempt) + '.csv'), 'a') as failurefile:      
                                csvwriter = csv.writer(failurefile)
                                csvwriter.writerow([thread[0], exit_string1, exit_string2])
                    except Exception as e:
                        print(f"Error processing thread: {thread[0]}. Error: {e}")
            except Exception as e:
                print(f"Error reading CSV file: {newsgroup}.csv. Error: {e}")
    except Exception as e:
        print(f"Error opening CSV file: {newsgroup}.csv. Error: {e}")

    with open(newsgroup + '_report.csv.r', 'a') as r:
        csvwriter = csv.writer(r)
        csvwriter.writerow([timestamp(), attempt, successes, failures])

    save_to_db(newsgroup_data, newsgroup, attempt, 0)
    driver.close()

    if failures == 0:
        return 0
    else:
        return 1


def process_newsgroup(newsgroup, attempt, newsgroups_failed, lock):
    print(newsgroup)
    exit_code = get_newsgroup(newsgroup, attempt)
    print(exit_code)
    if exit_code == 1:
        with lock:
            newsgroups_failed.add(newsgroup)

def multithread_launch(newsgroup_set, attempt):
    newsgroups_failed = set()
    max_threads = 8
    lock = threading.Lock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        executor.map(lambda ng: process_newsgroup(ng, attempt, newsgroups_failed, lock), newsgroup_set)

    return newsgroups_failed



def singlethread_launch(newsgroup_dict,attempt):
    pass
    newsgroups_failed=set()
    for newsgroup in newsgroup_dict:
        if get_newsgroup(newsgroup,attempt) == 1:
             newsgroups_failed.add(newsgroup)
    return newsgroups_failed
def timestamp():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

##MAIN
##Raw link list should be merged and deduplicated before using the "dedup.py tool"
##Filenames MUST BE IN THE FORM "newgroup.csv" ex => it.comp.retrocomputing.csv
# 1) Open all the csv files in the current directory
# 2) Spawn multithreading across csv file, using get_newsgroup function

#Directories
os.makedirs('failed', exist_ok=True)
os.makedirs('json', exist_ok=True)

#Counters
newsgroup_to_scrape = 0 #Counter of the newsgroup we are going to scrape

#I put all newsgroup to scrape in a dictionary
newsgroups_dict = set()
for filename in os.listdir('.'):
    if filename.endswith('csv'):
        newsgroup=filename[:-4]
        newsgroups_dict.add(newsgroup)
        newsgroup_to_scrape+=1
with open("final_report.txt", 'a') as r:
      r.write(f"\n{timestamp()} Planning to download {newsgroup_to_scrape}. Good luck. ")
attempt = 0

#Multithreading launch here
failed_set = multithread_launch(newsgroups_dict,attempt)
print("DEBUG")

#Single-thread debug implementation  
#failed_set = singlethread_launch(newsgroups_dict,attempt)   

complete_set = newsgroups_dict
newsgroups_dict = failed_set
print(type(complete_set))
with open("final_report.txt", 'a') as r:
      r.write(f"\n{timestamp()} Iteration: {attempt} Fully downloaded: {len(complete_set-failed_set)} Partially or not downloaded: {len(failed_set)}")
# Management of multiple iterations
max_attempts = 10 #Will retry to download missing url for 10 times
wait = 2 #After having tried to download the entire set of newsgroup, wait for this time until trying again. It should be an high value



while (len(failed_set)>0) and attempt < max_attempts:
    attempt+=1
    #Multithreading launch here
    failed_set = multithread_launch(newsgroups_dict,attempt)
    #Single-thread debug implementation  
    #failed_set = singlethread_launch(newsgroups_dict,attempt)     
    with open("final_report.txt", 'a') as r:   
        r.write(f"\n{timestamp()} Iteration: {attempt} Fully downloaded: {len(complete_set-failed_set)} Downloaded at this iteration: {len(newsgroups_dict-failed_set)} Partially or not downloaded: {len(failed_set)}")
    newsgroups_dict=failed_set
    print(f"\Waiting for {wait} second...")
    time.sleep(wait)


with open("final_report.txt", 'a') as r:
      r.write(f"\n{timestamp()} Iterations are over. Fully downloaded: {len(complete_set-failed_set)} Partially or not downloaded: {len(failed_set)}")
      r.write("\nList of failed newsgroups: ")
      for failed in failed_set:
           r.write("\n")
           r.write(failed)




