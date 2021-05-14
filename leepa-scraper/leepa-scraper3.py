import csv
import requests
from bs4 import BeautifulSoup
import sys
from importlib import reload
from time import time
import asyncio
import aiohttp
from parallel import parallelize, TaskResult, Status

reload(sys)
# sys.setdefaultencoding("utf8")
import datetime

input_file = sys.argv[1]
input_data = []
results = []
with open(input_file) as csvfile:
    reader = csv.reader(
        csvfile  # , quoting=csv.QUOTE_NONNUMERIC
    )  # change contents to floats
    for row in reader:  # each row is a list
        results.append(row[0])
    input_data = results

# set variables
start_time = time()
output_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")

csv.register_dialect("myDialect1", quoting=csv.QUOTE_ALL, skipinitialspace=True)
file = open(f"leepa2_{output_timestamp} - {input_file[:10]}.csv", "a")

writer = csv.writer(file, dialect="myDialect1")
writer.writerow(
    [
        "folio_id",
        "just",
        "assessed",
        "portability_applied",
        "cap_assessed",
        "taxable",
        "cap_difference",
        "land_units",
        "units_value",
        "number_buildings",
        "bedrooms",
        "tax_roll",
        "historic_designation",
        "community",
        "panel",
        "version",
        "date",
        "evacuation_zone",
    ]
)

id_list = []
if input_data:
    id_list = input_data

session = requests.Session()

count = 0
total_count = len(id_list)

for id in id_list:
    url = "https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={}&ExemptDetails=True&TaxRollDetails=True&ElevationDetails=True#AppraisalDetails"
    r = session.get(url.format(id))
    soup = BeautifulSoup(r.text, features="html.parser")
    try:
        just = ""
        assessed = ""
        portability_applied = ""
        cap_assessed = ""
        taxable = ""
        cap_difference = ""
        if soup.find_all("table", {"class": "appraisalDetailsRight autoAlternate"})[0]:
            propertyvalues = soup.find_all(
                "table", {"class": "appraisalDetailsRight autoAlternate"}
            )[0]
            just = propertyvalues.findAll("td")[0].text.strip()
            assessed = propertyvalues.findAll("td")[1].text.strip()
            portability_applied = propertyvalues.findAll("td")[2].text.strip()
            cap_assessed = propertyvalues.findAll("td")[3].text.strip()
            taxable = propertyvalues.findAll("td")[4].text.strip()
            cap_difference = propertyvalues.findAll("td")[5].text.strip()

        land_units = ""
        units_value = ""
        number_buildings = ""
        bedrooms = ""
        tax_roll = ""
        historic_designation = ""
        if soup.find_all("table", {"class": "appraisalDetailsRight autoAlternate"})[1]:
            attribute = soup.find_all(
                "table", {"class": "appraisalDetailsRight autoAlternate"}
            )[1]
            land_units = attribute.findAll("td")[0].text.strip()
            units_value = attribute.findAll("td")[1].text.strip()
            number_buildings = attribute.findAll("td")[2].text.strip()
            bedrooms = attribute.findAll("td")[3].text.strip()
            tax_roll = attribute.findAll("td")[4].text.strip()
            historic_designation = attribute.findAll("td")[5].text.strip()

        community = ""
        panel = ""
        version = ""
        date = ""
        evacuation_zone = ""
        if soup.find_all("table", {"class": "appraisalDetailsRight autoAlternate"})[1]:
            ElevationDetails = soup.find("table", {"class": "detailsTable"})
            community = ElevationDetails.findAll("td")[0].text.strip()
            panel = ElevationDetails.findAll("td")[1].text.strip()
            version = ElevationDetails.findAll("td")[2].text.strip()
            date = ElevationDetails.findAll("td")[3].text.strip()
            evacuation_zone = ElevationDetails.findAll("td")[4].text.strip()
            print(f"{id},{panel}, total data {count}/{total_count}")
            writer.writerow(
                [
                    id,
                    just,
                    assessed,
                    portability_applied,
                    cap_assessed,
                    taxable,
                    cap_difference,
                    land_units,
                    units_value,
                    number_buildings,
                    bedrooms,
                    tax_roll,
                    historic_designation,
                    community,
                    panel,
                    version,
                    date,
                    evacuation_zone,
                ]
            )
    except:
        pass
    count += 1


MAX_WORKERS = 100
PROXY = FreeProxy(country_id=['US'], timeout=0.7, rand=True).get()
HEADERS = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'}



class leepaException(Exception):
    pass


async def get_leepa_appraiser(id):
    url = f"https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={id}&ExemptDetails=True&TaxRollDetails=True&ElevationDetails=True#AppraisalDetails"
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(20), connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        async with session.get(url, headers=HEADERS) as response:
            # async with session.get(url, headers=HEADERS) as response:
            text = await response.text()
    data = (
        text.split("window.__INITIAL_STATE__ = ", 1)[-1]
        .split("window.__SITE_CONTEXT__ = ", 1)[0]
        .strip()[:-1]
    )
    try:
        
        
        appraiser.append(date_appraiser)
        avail_ind += 1
        rate_ind += 1
        date += datetime.timedelta(days=1)
        print("OK", id, len(appraiser))
    except Exception:
        raise leepaException()
    return appraiser


async def get_appraiser(id):
    try:
        res = await get_leepa_appraiser(id)
        return TaskResult(id, res, Status.OK)
    except leepaException:
        return TaskResult(id, None, Status.IGNORE)
    except Exception:
        return TaskResult(id, None, Status.ERROR)


def on_success(listing, dates):
    with open("vrbo_appraiser.csv", "a") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writerows(dates)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    total_future = asyncio.ensure_future(
        parallelize(ids, get_appraiser, on_success, MAX_WORKERS, loop)
    )
    loop.run_until_complete(total_future)


# print(r.text)
# Addapted from https://github.com/webdeveloper001/electron-xterm-scrapy/blob/master/scrappers/pbcgov/pbcgov.py
