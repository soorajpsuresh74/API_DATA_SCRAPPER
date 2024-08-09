import csv

import requests
import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s -%(levelname)s -%(message)s", datefmt="%Y-%m-%d %H:%M:%S",
                    handlers=[logging.StreamHandler(), logging.FileHandler("EndPointDataFetcher.log", mode="a")])

#create a logger instance
logger = logging.getLogger(__name__)


def base():
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0'
    }
    url = "https://partnerfinder.sap.com/sap/search/api/search/bm/results"
    parameters = {
        'q': '',
        'qField': 'partner',
        'pageSize': 12,
        'order': 'bestmatch'
    }
    return headers, url, parameters


def make_call():
    current_page = 0
    maximum_pages = 259
    fetch_data = []
    count = 0
    flag = True
    header, url, params = base()
    while current_page <= maximum_pages:
        params['pageNumber'] = current_page
        try:
            response = requests.get(url=url, headers=header, params=params)

            if response.status_code == 200:
                data = response.json()
                count = data.get('count')
                for results in data.get('results', []):
                    profileId = results.get('profileId', '')
                    title = results.get('title', 'N/A')
                    competencies = results.get('competencies', 'N/A')
                    hasGrowWithSap = results.get('hasGrowWithSap', 'N/A')
                    consultants = results.get('consultants', 'N/A')
                    description = results.get('description', 'N/A')

                    fetch_data.append({
                        'profileId': profileId,
                        'title': title,
                        'competencies': competencies,
                        'hasGrowWithSap': hasGrowWithSap,
                        'consultants': consultants,
                        'description': description
                    })


            else:
                status = response.status_code
                logger.log(logging.ERROR, f"Status code {status} - Failed to retrieve data for page {current_page}")
                flag = False
                break
        except ValueError as e:
            print(f"Exception occurred {e}")
        current_page += 1

    logger.log(logging.INFO, f'Success {flag} - Fetch data True')

    return fetch_data, count


data, count = make_call()
file_path = "files/Fetched_data.csv"
with open(file_path, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['profileId', 'title', 'competencies', 'hasGrowWithSap', 'consultants',
                                              'description'])
    writer.writeheader()
    writer.writerows(data)
logger.log(logging.INFO, "Successfully written to csv")
