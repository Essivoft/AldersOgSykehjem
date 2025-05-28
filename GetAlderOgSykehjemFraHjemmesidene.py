import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import csv
import os
import json
import yaml
import argparse
import re
import pandas as pd
import creds
import logging
import geopandas as gpd
from shapely.geometry import Point
import topojson
import numpy as np
from ckanapi import RemoteCKAN
import xml.etree.ElementTree as ET
from xml.dom.minidom import parseString

# Constants
BASE_URL = creds.base_url
CKAN_API_KEY = creds.ckan_api_key
API_PATH = "/api/nursinghome/search"
URL_JSON = "url_list.json"
CSV_FILE = "stavanger_sykehjem.csv"
JSON_FILE = "stavanger_sykehjem.json"
GeoJSON_FILE = "stavanger_sykehjem.geojson"
TopoJSON_FILE = "stavanger_sykehjem.topojson"
KML_FILE = "stavanger_sykehjem.kml"
XML_FILE = "stavanger_sykehjem.xml"
YML_FILE = "stavanger_sykehjem.yml"
XLSX_FILE = "stavanger_sykehjem.xlsx"
ERRORS_DIR = "Alders&SykehjemStavanger_Errors"
LOG_FILE = os.path.join(f"{ERRORS_DIR}\\Alders&SykehjemStavanger_Errors.log")
os.makedirs(ERRORS_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

# Set up logging
logging.basicConfig(filename=LOG_FILE, level=logging.WARNING, format='%(asctime)s:%(levelname)%:%(message)s')

def extract_urls():
    # Gets all the URLs for nursing homes
    try:
        api_url = urljoin(BASE_URL, API_PATH)
        resp = requests.get(api_url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()
        items = data.get('Items') or data

        urls = []
        for item in items:
            u = item.get('Url')
            if u:
                full = urljoin(BASE_URL, u)
                urls.append(full)
        urls = sorted(set(urls))

        with open(URL_JSON, 'w', encoding='utf-8') as f:
            json.dump(urls, f, ensure_ascii=False, indent=2)
        print(f"Extract: Lagret {len(urls)} URLer til {URL_JSON}")
        return urls
    except Exception as e:
        logging.error(f"Feil under URL-uttrekk 'def extract_urls': {e}")

def parse_location_page(url):
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'lxml')

    # Navn på institusjon
    name = soup.find('h1').get_text(strip=True) if soup.find('h1') else url

    # Init felter
    phone = email = street = postal = locality = ''

    # Kontaktinfo og adresse
    contact_dl = soup.find('dl', class_='contactinfo__list')
    if contact_dl:
        for dt, dd in zip(contact_dl.find_all('dt'), contact_dl.find_all('dd')):
            key = dt.get_text(strip=True).rstrip(':')
            if key == 'Telefon':
                a = dd.find('a', class_='phone-link')
                phone = a.get_text(strip=True) if a else ''
            elif key == 'E-post':
                a = dd.find('a', href=lambda h: h and h.startswith('mailto:'))
                email = a.get_text(strip=True) if a else ''
            elif key in ('Besøksadresse', 'Besoksadresse'):
                p = dd.find('p', class_='address')
                if p:
                    parts = p.find_all('span', class_='part')
                    if parts:
                        street = parts[0].get_text(strip=True)
                    if len(parts) > 1:
                        nums = parts[1].find_all('span')
                        if len(nums) >= 2:
                            postal, locality = nums[0].get_text(strip=True), nums[1].get_text(strip=True)

    # Virksomhetsleder
    manager = ''
    intro = soup.find('p', class_='contactinfo__introduction')
    if intro:
        text = intro.get_text(separator=' ', strip=True)
        m = re.search(r'Virksomhetsleder\s+(.+?)(?:\s+Mobil|$)', text)
        manager = m.group(1).strip() if m else ''
    if not manager:
        for p in soup.find_all('p'):
            if p.find('strong') and 'Virksomhetsleder' in p.get_text():
                manager = re.sub(r'Virksomhetsleder:?\s*', '', p.get_text(strip=True))
                break

    content_div = (
        soup.find('div', class_='block contentblock') or
        soup.find('div', class_='contentblocks') or
        soup.find('div', class_='text-content') or
        soup.find('div', class_='article-body')
    )
    comment = content_div.get_text(separator='\n\n', strip=True) if content_div else ''

    return {
        'Navn / virksomhet': name,
        'Kommune': 'Stavanger',
        'KommuneId': '1103',
        'Gateadresse': street,
        'Postnummer': postal,   # Keep from CSV if available
        'Poststed': locality,   # Keep from CSV if available
        'Latitude': '',         # Keep from CSV if available
        'Longitude': '',        # Keep from CSV if available
        'Tlf': phone,
        'epost': email,
        'Virksomhetsleder': manager,
        'URL / hjemmeside': url,
        'kommentar': comment,
    }


def scrape_from_json():
    try:
        existing = {}
        if os.path.exists(CSV_FILE):
            with open(CSV_FILE, newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f, delimiter=';'):
                    u = row.get('URL / hjemmeside')
                    existing[u] = {
                        'Latitude': row.get('Latitude', ''),
                        'Longitude': row.get('Longitude', ''),
                        'Postnummer': row.get('Postnummer', ''),
                        'Poststed': row.get('Poststed', '')
                    }

        with open(URL_JSON, 'r', encoding='utf-8') as f:
            urls = json.load(f)

        records = []
        for url in urls:
            try:
                rec = parse_location_page(url)
                # Remove "mailto:" from email field if it exists
                rec['epost'] = rec['epost'].replace('mailto:', '')
                prev = existing.get(url, {})
                for field in ['Latitude', 'Longitude', 'Postnummer', 'Poststed']:
                    if prev.get(field):
                        rec[field] = prev[field]
                records.append(rec)
                print(f"✓ Scraped {rec['Navn / virksomhet']}")
            except Exception as e:
                print(f"✗ Feilet {url}: {e}")

        fields = [
            'Navn / virksomhet','Kommune','KommuneId','Gateadresse',
            'Postnummer','Poststed','Latitude','Longitude',
            'Tlf','epost','Virksomhetsleder','URL / hjemmeside','kommentar'
        ]
        with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(records)
        print(f"CSV lagret: {CSV_FILE}")
    except Exception as e:
        logging.error(f"Feil under scraping 'def scrape_from_json': {e}")

def main():
    p = argparse.ArgumentParser(description='Two-step scraper')
    sub = p.add_subparsers(dest='cmd')
    sub.add_parser('extract')
    sub.add_parser('scrape')
    args = p.parse_args()
    if args.cmd == 'extract': extract_urls()
    elif args.cmd == 'scrape': scrape_from_json()
    else: p.print_help()

if __name__ == '__main__':
    main()

# CSV to JSON conversion
def csv_to_json(csv_file, json_file):
    data = pd.read_csv(csv_file, delimiter=';', encoding='utf-8')
    
    json_data = data.to_json(orient='records', force_ascii=False, indent=4)
    with open(json_file, 'w', encoding='utf-8') as file:
        file.write(json_data)

csv_file = f'{CSV_FILE}' 
json_file = f'{JSON_FILE}'
try:
    csv_to_json(csv_file, json_file)
except Exception as e:
    logging.error(f"Feil under CSV til JSON konvertering 'def_csv_to_json': {e}")

# JSON to YML conversion
def json_to_yml(json_file, yml_file):
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)

    with open(yml_file, 'w', encoding='utf-8') as file:
        yaml.dump(data, file, allow_unicode=True, default_flow_style=False)
json_file = f'{JSON_FILE}'
yml_file = f'{YML_FILE}'
try:
    json_to_yml(json_file, yml_file)
except Exception as e:
    logging.error(f"Feil under JSON til YML konvertering 'def_json_to_yml': {e}")
    
# CSV to GeoJSON conversion
def csv_to_geojson(input_csv_file, output_geojson_file):
    df = pd.read_csv(input_csv_file, delimiter=';', encoding='utf-8')

    # Create a geodataframe from the Pandas dataframe
    geometry = [Point(xy) for xy in zip(df.Longitude, df.Latitude)]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    gdf = gdf.set_crs('epsg:4326')

    # Convert Geodataframe to GeoJSON file
    gdf.to_file(output_geojson_file, driver="GeoJSON", encoding='utf-8')

csv_file = f'{CSV_FILE}'
geojson_file = f'{GeoJSON_FILE}'
try:
    csv_to_geojson(csv_file, geojson_file)
except Exception as e:
    logging.error(f"Feil under CSV til GeoJSON konvertering 'def_csv_to_geojson': {e}")

# CSV to XLSX conversion
def csv_to_xlsx(csv_file, xlsx_file):
    data = pd.read_csv(csv_file, delimiter=';', encoding='utf-8')
    data.to_excel(xlsx_file, index=False, sheet_name='Aldr.&Sykehjm.', engine='xlsxwriter')

csv_file = f'{CSV_FILE}'
xlsx_file = f'{XLSX_FILE}'
try:
    csv_to_xlsx(csv_file, xlsx_file)
except Exception as e:
    logging.error(f"Feil under CSV til XLSX konvertering 'def_csv_to_xlsx': {e}")

# GeoJSON to TopoJSON conversion 
def geojson_to_topojson(input_file, output_file):
    gdf = gpd.read_file(input_file, encoding='utf-8')
    #Erstatt NaN verdier
    gdf = gdf.replace({np.nan: None})

    topo = topojson.Topology(gdf)
    topo_json = json.dumps(topo.to_dict(), ensure_ascii=False, indent=4)
    with open(output_file, "w", encoding='utf-8') as f:
        f.write(topo_json)

input_file = f'{GeoJSON_FILE}'
output_file = f'{TopoJSON_FILE}'
try:
    geojson_to_topojson(input_file, output_file)
except Exception as e:
    logging.error(f"Feil under GeoJSON til TopoJSON konvertering 'def_geojson_to_topojson': {e}")

'''''''''
Method for updating a dataset in CKAN
 package_id = id dataset id in CKAN
 id = resource id in CKAN
 name = name of resource in CKAN
desc = desc on the resource in CKAN
'''''''''
ua = 'Alders&SykehjemUploadOpencom/1.0 (+https://opencom.no)'

# CSV file
def upload_csv_to_ckan():
    try:
        mysite = RemoteCKAN('https://opencom.no', apikey=creds.ckan_api_key, user_agent=ua)
        mysite.action.resource_update(
            package_id='f97f77ab-0157-44a8-8c1d-60a182612779',
            id = '740a7670-f2dc-4be8-b077-25b8c6b86cb3',
            name=f'Aldershjem og sykehjem.csv',
            #description =   ''
            format = 'csv',
            upload = open(f'{CSV_FILE}', 'rb'))
    except Exception as e:
        logging.error(f"Error under 'upload_csv_to_ckan': {str(e)}")
#====================================================================
try:
    upload_csv_to_ckan()
except Exception as e:
    logging.error(f"Error under 'upload_csv_to_ckan': {str(e)}")

# XLSX file
def upload_xlsx_to_ckan():
    try:
        mysite = RemoteCKAN('https://opencom.no', apikey=creds.ckan_api_key, user_agent=ua)
        mysite.action.resource_update(
            package_id='f97f77ab-0157-44a8-8c1d-60a182612779',
            id = '79166cce-3135-4e9b-8549-4f81b246a6b3',
            name=f'Aldershjem og sykehjem.xlsx',
            #description =   ''
            format = 'xlsx',
            upload = open(f'{XLSX_FILE}', 'rb'))
    except Exception as e:
        logging.error(f"Error under 'upload_xlsx_to_ckan': {str(e)}")
#====================================================================
try:
    upload_xlsx_to_ckan()
except Exception as e:
    logging.error(f"Error under 'upload_xlsx_to_ckan': {str(e)}")

# JSON file
def upload_json_to_ckan():
    try:
        mysite = RemoteCKAN('https://opencom.no', apikey=creds.ckan_api_key, user_agent=ua)
        mysite.action.resource_update(
            package_id='f97f77ab-0157-44a8-8c1d-60a182612779',
            id = '28c22050-4cbf-46d8-aff7-0d80c83d01df',
            name=f'Aldershjem og sykehjem.json',
            #description =   ''
            format = 'json',
            upload = open(f'{JSON_FILE}', 'rb'))
    except Exception as e:
        logging.error(f"Error under 'upload_json_to_ckan': {str(e)}")
#====================================================================
try:
    upload_json_to_ckan()
except Exception as e:
    logging.error(f"Error under 'upload_json_to_ckan': {str(e)}")

# GeoJSON file
def upload_geojson_to_ckan():
    try:
        mysite = RemoteCKAN('https://opencom.no', apikey=creds.ckan_api_key, user_agent=ua)
        mysite.action.resource_update(
            package_id='f97f77ab-0157-44a8-8c1d-60a182612779',
            id = '2de6dcaf-2ab1-492c-ad00-3289218c5013',
            name=f'Aldershjem og sykehjem.geojson',
            #description =   ''
            format = 'geojson',
            upload = open(f'{GeoJSON_FILE}', 'rb'))
    except Exception as e:
        logging.error(f"Error under 'upload_geojson_to_ckan': {str(e)}")
#====================================================================
try:
    upload_geojson_to_ckan()
except Exception as e:
    logging.error(f"Error under 'upload_geojson_to_ckan': {str(e)}")

# TopoJSON file
def upload_topojson_to_ckan():
    try:
        mysite = RemoteCKAN('https://opencom.no', apikey=creds.ckan_api_key, user_agent=ua)
        mysite.action.resource_update(
            package_id='f97f77ab-0157-44a8-8c1d-60a182612779',
            id = '0a819971-846a-4a30-9139-aee39321b338',
            name=f'Aldershjem og sykehjem.topojson',
            #description =   ''
            format = 'json',
            upload = open(f'{TopoJSON_FILE}', 'rb'))
    except Exception as e:
        logging.error(f"Error under 'upload_topojson_to_ckan': {str(e)}")
#====================================================================
try:
    upload_topojson_to_ckan()
except Exception as e:
    logging.error(f"Error under 'upload_topojson_to_ckan': {str(e)}")

# YAML file
def upload_yml_to_ckan():
    try:
        mysite = RemoteCKAN('https://opencom.no', apikey=creds.ckan_api_key, user_agent=ua)
        mysite.action.resource_update(
            package_id='f97f77ab-0157-44a8-8c1d-60a182612779',
            id = 'ba8591bf-c35e-4364-851c-633b16a5c004',
            name=f'Aldershjem og sykehjem.yml',
            #description =   ''
            format = 'txt',
            upload = open(f'{YML_FILE}', 'rb'))
    except Exception as e:
        logging.error(f"Error under 'upload_yml_to_ckan': {str(e)}")
#====================================================================
try:
    upload_yml_to_ckan()
except Exception as e:
    logging.error(f"Error under 'upload_yml_to_ckan': {str(e)}")

