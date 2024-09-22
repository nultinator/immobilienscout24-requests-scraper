import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_search_results(search_info, location, retries=3):
    url = f"https://www.immobilienscout24.de/Suche/de/{search_info['state']}/{search_info['city']}/wohnung-mieten"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
            soup = BeautifulSoup(response.text, "html.parser")
            
            div_cards = soup.find_all("div", class_="result-list-entry__data")
            if not div_cards:
                raise Exception("Listings failed to load!")

            for card in div_cards:
                name = card.find("div", class_="result-list-entry__address font-ellipsis").text
                href = card.find("a").get("href")
                link = ""
                prefix =  "https://www.immobilienscout24.de"
                if prefix in href:
                    continue
                else:
                    link = f"{prefix}{href}"
                attributes_card = card.select_one("div[data-is24-qa='attributes']")
                attributes = attributes_card.find_all("dl")

                price = attributes[0].text.replace("Kaltmiete", "")
                size = attributes[1].text.replace("Wohnfläche", "")
                date_available = "n/a"
                date_text = attributes[2].find("dd").text
                if "Zi" not in date_text:
                    date_available = date_text

                search_data = {
                    "name": name,
                    "price": price,
                    "size": size,
                    "date_available": date_available,
                    "url": link
                }
                print(search_data)

            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "de"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = [{"state": "bayern", "city": "muenchen"}]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:

        scrape_search_results(keyword, LOCATION, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")