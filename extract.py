import json
import re
import csv
import requests
import sys
import csv
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:100.0) Gecko/20100101 Firefox/100.0"}
def bar(total, done):
    progress = done / total
    bar_length = 50
    filled_length = int(bar_length * progress)
    remaining_length = bar_length - filled_length
    bar = '=' * filled_length + '-' * remaining_length
    percentage = progress * 100
    sys.stdout.write('\r')
    sys.stdout.write(f'Total: {total} Done: {done} [{bar}] {percentage:.2f}%')
    sys.stdout.flush()

MONTHS = {
    'Jan': '01',
    'Feb': '02',
    'Mar': '03',
    'Apr': '04',
    'May': '05',
    'Jun': '06',
    'Jul': '07',
    'Aug': '08',
    'Sep': '09',
    'Oct': '10',
    'Nov': '11',
    'Dec': '12'
}

def download_and_process_url(url):
    retries = 3
    for attempt in range(retries):
        html_content = download_with_retry(url, headers=headers)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')

            discussion_div = soup.find('div', class_='RBM0ic')

            title_div = discussion_div.find('div', class_='ThqSJd')
            title = title_div.text.strip()

            messages = []
            message_divs = discussion_div.find_all('div', class_='ptW7te')
            for message_div in message_divs:
                author = message_div.find_previous('div', class_='LgTNRd').text.strip()
                date_string = message_div.find_previous('div', class_='ELCJ4d').text.strip()

                # REGEX per recuperare le date
                match = re.search(r'(\w{3}) (\d{1,2}), (\d{4}), (\d{1,2}:\d{2}:\d{2})\s?(\w{2})', date_string)
                if match:
                    month_abbr = match.group(1)
                    month = MONTHS.get(month_abbr)
                    day = match.group(2)
                    year = match.group(3)
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
                messages.append({'author': author, 'month': month, 'day': day, 'year': year, 'time': time, 'am_pm': am_pm, 'text': text, 'date_problem': date_problem, 'original_link':url})

            conversation_data = {
                'title': title,
                'messages': messages
            }

            with open('conversations.json', 'a', encoding='utf-8') as json_file:
                json.dump(conversation_data, json_file, indent=4)
            print(f"Salvata {title} {day}/{month}/{year}")
            return True
        else:
            if attempt < retries - 1:
                wait_time = 2 ** attempt
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
    return False

def download_with_retry(url, headers=None, timeout=30, retries=3):
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        log_error(url)
        return None

def log_error(url):
    with open('errors.log.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([url])

linee = 0
scaricate = 0

with open('feed.csv', 'r', encoding='utf-8') as contalinea:
    for row in contalinea:
        linee = linee + 1
print(f"Devo scaricare {linee} conversazioni.")

with open('feed.csv', 'r', encoding='utf-8') as csvfile:
    csvreader = csv.reader(csvfile)
    urls_to_download = [row[0] for row in csvreader]

with ThreadPoolExecutor(max_workers=15) as executor:
    results = executor.map(download_and_process_url, urls_to_download)

# Count successful downloads
scaricate = sum(1 for result in results if result)

print(f"Sono state scaricate {scaricate} conversazioni.")
