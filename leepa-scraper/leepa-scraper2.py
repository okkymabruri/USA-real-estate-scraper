import csv
from bs4 import BeautifulSoup
import sys
from importlib import reload
from time import time
import asyncio
import aiohttp
from parallel import parallelize, TaskResult, Status
from fp.fp import FreeProxy

reload(sys)
import datetime

class leepaException(Exception):
    pass


async def get_leepa_appraiser(id):
    # sem = asyncio.Semaphore(100)
    url = f"https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={id}&TaxRollDetails=True&ElevationDetails=True#ElevationDetails"
    # PROXY = FreeProxy(country_id=["US"], timeout=1, rand=True).get()
    PROXY = None
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(20), connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        async with session.get(url, proxy=PROXY, headers=HEADERS) as response:
            text = await response.text()
    soup = BeautifulSoup(text, features="html.parser")
    print("get soup")
    # print(PROXY)
    try:
        just = ""
        assessed = ""
        portability_applied = ""
        cap_assessed = ""
        taxable = ""
        cap_difference = ""
        print("get init")
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
        print("get stage1")
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
        print("get stage2")
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

        print("get stage3")
        appraiser = {
            "folio_id": id,
            "just": just,
            "assessed": assessed,
            "portability_applied": portability_applied,
            "cap_assessed": cap_assessed,
            "taxable": taxable,
            "cap_difference": cap_difference,
            "land_units": land_units,
            "units_value": units_value,
            "number_buildings": number_buildings,
            "bedrooms": bedrooms,
            "tax_roll": tax_roll,
            "historic_designation": historic_designation,
            "community": community,
            "panel": panel,
            "version": version,
            "date": date,
            "evacuation_zone": evacuation_zone,
        }
        print("OK", id)

    except Exception:
        raise leepaException()
    return appraiser


async def get_appraiser(id):
    print("get_appraiser(id)")
    try:
        res = await get_leepa_appraiser(id)
        print(res)
        return TaskResult(id, res, Status.OK)
    except leepaException:
        return TaskResult(id, None, Status.IGNORE)
    except Exception:
        return TaskResult(id, None, Status.ERROR)


def on_success(listing, appraiser):
    print("write")
    with open("result.csv", "a") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writerow(appraiser)


if __name__ == "__main__":
    input_file = sys.argv[1]
    input_data = []
    results = []
    with open(input_file) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:  # each row is a list
            results.append(row[0])
        input_data = results

    # set variables
    start_time = time()
    output_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")

    file = f"leepa2_{output_timestamp}.csv"
    fieldnames = [
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

    # with open(file, "w") as files:
    #     writer = csv.DictWriter(files, fieldnames=fieldnames)
    #     writer.writeheader()

    id_list = []
    if input_data:
        id_list = input_data


    MAX_WORKERS = 5
    # PROXY = FreeProxy(country_id=["US"], timeout=1, rand=True).get()
    # print(PROXY)
    HEADERS = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
    }
    with open("result.csv", "w") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
    loop = asyncio.get_event_loop()
    total_future = asyncio.ensure_future(
        parallelize(id_list, get_appraiser, on_success, MAX_WORKERS, loop)
    )
    loop.run_until_complete(total_future)


# print(r.text)
# Addapted from https://github.com/webdeveloper001/electron-xterm-scrapy/blob/master/scrappers/pbcgov/pbcgov.py
