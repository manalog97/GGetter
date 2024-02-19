import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
from bs4 import BeautifulSoup
import csv
import re
import os
import json
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
#from bs4 import BeautifulSoup
import argparse

#Logica del programma
# 1) Apro un csv contenente la lista dei newsgroup (formato: newsgroup,anno_di_partenza)
# 2) Ciclo sulla lista dei newsgroup, per ogni newsgroup ciclo prima sugli anni (anno_di_partenza => 2024)
# 3) Poi ciclo sui mesi


def is_loading(driver):
        condition = driver.execute_script("return document.readyState != 'complete'")
        while condition:
            time.sleep(0.5)
            condition = driver.execute_script("return document.readyState != 'complete'")

    
def get_links(driver, year, month, newsgroup,links,lev): 
    if(lev==0):
        #print("Siamo al primo livello di ricorsione")
        pass
    else:
        pass
        #print(f"Siamo al livello {lev} di ricorsione, stiamo ereditando {len(links)} links")
    try:
        # Find all links containing '/g/newsgroup/c' in href attribute inside the div UGgcDd
        xpath = f'//a[contains(@href, "/g/{newsgroup}/c")]'
        original_links = WebDriverWait(driver, 1).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
        # Deduplicate results
        links_to_update=set((link.get_attribute('href') for link in original_links))
        #print(f"Aggiungo {len(links_to_update)} link")
        links.update(links_to_update)
        #links = set(links)
        return links
    except TimeoutException:
        with open("log.txt", "a") as log:
            log.write(f"\nTimeout exception in {year}-{month} (get links)")
        return links
    except StaleElementReferenceException:
        # Ricorsione
        #print("Ricorsione avviata...")
        time.sleep(0.2) #Waiting for half second...
        return get_links(driver, year, month, newsgroup,links,lev+1)
    except NoSuchElementException:
        xpath = f'//a[contains(@href, "/g/{newsgroup}/c")]'
        original_links = WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
        # Deduplicate results
        links_to_update=set((link.get_attribute('href') for link in original_links))
        #print(f"Aggiungo {len(links_to_update)} link")
        links.update(links_to_update)
        #links = set(links)

def click_next_page(driver,year,month):
        is_loading(driver)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        try:
            # Wait for the next button to be clickable
            next_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, '//div[@role="button" and @aria-label="Next page"]')))
            driver.execute_script("arguments[0].scrollIntoView();", next_button)
            driver.execute_script("arguments[0].click();", next_button)
            #print("Dovrei aver clickato")
        except (TimeoutException, StaleElementReferenceException):
            with open("log.txt", "a") as log:
                log.write(f"\nC'è stato un problema in {year}-{month} (click next page)")
            pass
        is_loading(driver)

def noresults(driver):
    try:
        isittheend_element = driver.find_element(By.CLASS_NAME, "UFO5fb ")
        isittheend_text = isittheend_element.text
        if isittheend_text == "Try another search":
            # this is the end, my only friend, the end
            return True
    except NoSuchElementException:
        return False

def isthistheend(driver):
    try:
        isittheend_element = driver.find_element(By.CLASS_NAME, "aEb7Ed")
        isittheend_text = isittheend_element.text
        if isittheend_text.split()[0].split("–")[1] == isittheend_text.split()[2]:         # this is the end, my only friend, the end
            return int(isittheend_text.split()[2])
    except NoSuchElementException:
            return -1
    try:
        isittheend_element = driver.find_element(By.CLASS_NAME, "UFO5fb ")
        isittheend_text = isittheend_element.text
        if isittheend_text == "Try another search":
            # this is the end, my only friend, the end
            return -2
    except NoSuchElementException:
        return -1

def scrape_month(newsgroup,year,month,link_writer,driver,debug):
    #Inizializzo i contatori
    pagina=0 #Contatore pagina di ricerca Google Groups
    if debug:
        print(f"\nScarico mese: {month} anno: {year}")
    base_url = "https://groups.google.com/g/"
    if month == 12: #Lo scraping è effettuato dal primo giorno del mese al primo del mese successivo, gestisco il caso in cui il mese sia Dicembre
        next_month = 1
        next_month_year = year + 1
    else:
        next_month = month + 1
        next_month_year = year
     # Verifica se i mesi necessitano dello zero iniziale
    month_string = str(month) if month >= 10 else f"0{month}"
    next_month_string = str(next_month) if next_month >= 10 else f"0{next_month}"
    url = f"{base_url}{newsgroup}/search?q=after%3A{year}-{month_string}-01%20before%3A{next_month_year}-{next_month_string}-01"

    #Carico la pagina
    try: 
        driver.get(url)
    except TimeoutException:
        print("Page load timed out, reloading...")
        driver.refresh()  # Reload the page    
    is_loading(driver)

    # I link della prima pagina corrispondente al mese vengono memorizzati
    # Saranno poi ricontrollati ogni volta che si clicca su next page perchè google groups ricomincia dall'inizio quando si arriva alla fine
    #QUESTA ERA LA PRIMA LOGICA, MA DAVA PROBLEMI
    #ORA PROVIAMO A VERIFICARE SE IN aEb7Ed ho un testo del tipo "391–406 of 406" che segnala l'arrivo all'ultima pagina... funzionerà?
    # Se la pagina non contiene link, la salto
    if noresults(driver):
        return 0,0
    changemonth=False
    first_links=set()
    checkend=isthistheend(driver)
    if not changemonth:
        first_links = get_links(driver,year,month,newsgroup,set(),0)
        if len(first_links)==0:
            changemonth=True #Passo al mese successivo
        elif checkend > 0 or checkend==-2:
            changemonth=True
        else:
            #Salvo i link e li stampo a schermo per debug
            for link in first_links:
                row=[link,1,month,year]
                link_writer.writerow(row)
                if debug:
                    #print(link)
                    pass
    link_trovati=0
    #Ho trovato i primi link e li ho memorizzati
    print(f"Link prima pagina: {len(first_links)}")
    #flag=False #Condizione di uscita dal ciclo
    total_links = first_links
    target_links=checkend
    while not changemonth:
        changemonth=False
        #Cambio pagina
        is_loading(driver)
        links=set()
        lenbefore=len(total_links)
        links=get_links(driver,year,month,newsgroup,set(),0) #Vedo se ci sono link in sospeso dalla pagina prima
        total_links.update(links)    
        time.sleep(0.2)    
        click_next_page(driver,year,month) 
        links=get_links(driver,year,month,newsgroup,set(),0) #Ottengo i link
        total_links.update(links)
        #debug
        #print(f"Link Trovati: {len(links)}")
        #Controllo condizioni di uscita
        if len(links)==0:
            if(debug):
                print("Non ci sono link (interno ciclo), passo al mese successivo - Questa cosa non dovrebbe MAI apparire, controllare")
            #changemonth=True
            #break #Non ci sono link, esco dal ciclo
        #print("DBG2")
        checkend = isthistheend(driver)
        #print(f"\ncheckend: {checkend}")
        #print("DBG3")
        if checkend>0: #Se la funzione non ritorna -1, allora ritorna il numero di link previsti da scaricare
            changemonth = True
            if debug:
                pass
                #print("This is the end, my only friend, the end")
            target_links=checkend
            #print(f"Target={target_links}")
            time.sleep(0.1)
            links = get_links(driver, year, month, newsgroup, links, 0)
            total_links.update(links)
            lenafter=len(total_links)
            print(f"Link trovati all'ultima pagina: {lenafter-lenbefore}")
        for link in links:
            if link in first_links and pagina>100:
                if debug:
                    pass
                    print("Ritrovato primi link, passo al mese successivo [funzione disabilitata]")
                    with open("log.txt", "a") as log:
                        log.write(f"\nC'è stato un problema Serio {year}-{month} salvate 100 pagine (3000 messaggi in un mese) controllare se è corretto")

                changemonth=True
                #break
        if changemonth==True:
            break
        # Controlli superati, salvo i link :)
        pagina=pagina+1
        link_trovati=0
        for link in links:
            link_trovati += 1
            if debug:
                #print(link)    
                pass
            changemonth=False  
        print(f"Link trovati a pagina {pagina} = {link_trovati}") 
        if link_trovati<30: #Le pagine dovrebbero contenere 30 link ciascuna, potrebbe non averli trovati tutti...
            tentativo = 0
            found_30 = False
            while tentativo < 2 and not found_30:
                print(f"Trovati meno di 30 link. Tentativo {tentativo+1}")
                time.sleep(0.2)
                get_links(driver,year,month,newsgroup,links,0)
                total_links.update(links)
                link_trovati=0
                for link in links:
                    link_trovati += 1
                if link_trovati>=30:
                    found_30 = True
                    break
                else:
                    tentativo +=1


    #Ciclo terminato, salvo i link unici
    lung_dbg2=len(total_links)

    if target_links == lung_dbg2:
        pass
    else:
        #Un ultimo tentativo...
        time.sleep(0.5)
        links=get_links(driver,year,month,newsgroup,set(),0) #Ottengo i link
        total_links.update(links)
        if lung_dbg2==target_links:
            print(f"I link persi sono stati recuperati.")
        else:
            with open("log.txt", "a") as log:
                 log.write(f"\nC'è stato un problema in {year}-{month}: mi aspettavo {target_links}, ne ho scaricate {lung_dbg2}")
            with open("lost.txt", "a") as lost:
                 lost.write(f"\n{newsgroup},{year},{month},{target_links},{lung_dbg2}")
    #Finalmente, salvo i link
    for link in total_links:
        row=[link,pagina,month,year]
        link_writer.writerow(row)
    if debug:
        print(f"Trovati nel mese {month} {len(total_links)} link univoci. Il target è {target_links}")
    return len(total_links),target_links

def notAvailable(driver,newsgroup):
    driver.get("https://groups.google.com/g/"+newsgroup)
    return bool(driver.find_elements(By.XPATH, '//p[@class="gnacYd" and text()="Content unavailable"]'))




import argparse

resume_year_set = False
resume_month_set = False
resume_year_value = None
resume_month_value = None
single_newsgroup_scrape = False
headless = False
profile_path_set = False

# Parse command line arguments
parser = argparse.ArgumentParser(description="Options")
parser.add_argument("--resume-from-year", type=int, help="Resume from year")
parser.add_argument("--resume-from-month", type=int, help="Resume from month")
parser.add_argument("--newsgroup", type=str, help="Scrape a single newsgroup [PLEASE SPECIFY STARTING YEAR AND MONTH]")
parser.add_argument("--headless", action='store_true', help="Set if you want Firefox to run in headless mode or no")
parser.add_argument("--profile-path", type=str, help="Set Firefox profile path")
args = parser.parse_args()

# Check if options are provided and set the variables accordingly
if args.resume_from_year is not None:
    resume_year_set = True
    resume_year_value = args.resume_from_year
if args.resume_from_month is not None:
    resume_month_set = True
    resume_month_value = args.resume_from_month
if args.newsgroup is not None:
    single_newsgroup_scrape = True
    single_newsgroup = args.newsgroup
if args.headless:
    headless = True
if args.profile_path is not None:
    profile_path_set = True
    profile_path = args.profile_path

# Print the values if they are set
if resume_year_set:
    print("Resume from year: [ONLY FIRST NEWSGROUP OF THE LIST!]", resume_year_value)
if resume_month_set:
    print("Resume from month: [ONLY FIRST NEWSGROUP OF THE LIST!]", resume_month_value)

#MAIN
debug = True #Stampa a video
options = webdriver.FirefoxOptions()
options.add_argument("-profile")
if profile_path_set:
    options.add_argument(profile_path)
else:
    options.add_argument('/dati/Home/matteo/.mozilla/firefox/i7vm2yal.selenium')
if headless:
    options.add_argument('--headless')
file_report=open('file_report.csv', 'a')
reportwriter=csv.writer(file_report)
if single_newsgroup_scrape: #Availability check is not done because this option is meant to be used manually
    driver = webdriver.Firefox(options=options)
    print(f"\nComincio lo scraping di {single_newsgroup}")
    file_link=open(f"lista_link_{single_newsgroup}.csv","a")
    link_writer=csv.writer(file_link)
    #Scrape the from the specified setpoint:
    for month in range(resume_month_value,13,1):
        recuperati,totali = scrape_month(single_newsgroup,resume_year_value,month,link_writer,driver,debug)   
        reportwriter.writerow([single_newsgroup,recuperati,totali])  
    #Go to the next year
    starting_year=resume_year_value+1
    #Scrape it all!
    for year in range(starting_year,2025,1): #2024 è hard-coded in quanto anno finale dell'archivio Google Groups
        for month in range(1,13,1):
            recuperati,totali = scrape_month(single_newsgroup,year,month,link_writer,driver,debug)   
            reportwriter.writerow([single_newsgroup,recuperati,totali])  
else:
    with open('lista_newsgroup.csv', newline='') as csvfile:
        reader=csv.reader(csvfile)
        firstIteration=True
        for row in reader:
            if resume_month_set:
                if firstIteration:
                    starting_month=resume_month_value
                else:
                    starting_month=1
            else:
                starting_month=1
            if resume_year_set:
                if firstIteration:
                    starting_year=resume_year_value
                else:
                    starting_year=int(row[1])
            else:
                starting_year=int(row[1])

            newsgroup=row[0]
            #Apro il browser Selenium (Firefox). Questo viene fatto per ogni newsgroup in modo da 
            #introdurre un minimo di ritardo che può migliorare l'affidabilità dello script
            driver = webdriver.Firefox(options=options)
            #Apro in scrittura il file CSV del newsgroup, Struttura
            #link,titolo,pagina,mese,anno
            print(f"\nComincio lo scraping di {newsgroup}")
            file_link=open(f"lista_link_{newsgroup}.csv","a")
            link_writer=csv.writer(file_link)
            if notAvailable(driver,newsgroup):
                row=["Newsgroup not available on Google Groups. Time to scrape Narkive too :)"]
                link_writer.writerow(row)
                file_link.close()  
                driver.quit()
                continue

            for year in range(starting_year,2025,1): #2024 è hard-coded in quanto anno finale dell'archivio Google Groups
                for month in range(starting_month,13,1):
                    recuperati,totali = scrape_month(newsgroup,year,month,link_writer,driver,debug)   
                    reportwriter.writerow([newsgroup,recuperati,totali])  
            print("\nScraping di {newsgroup} completato")  
            file_link.close()  
            driver.quit()
            firstIteration=False
    file_report.close()

