import csv
from selenium import webdriver
from concurrent.futures import ThreadPoolExecutor, wait
import datetime
import sys
from time import sleep, time
from bs4 import BeautifulSoup
import requests


def get_driver(headless):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--start-maximized")
    # initialize driver
    driver = webdriver.Chrome(options=options)
    return driver


def write_to_file(output_list, filename):
    with open(filename, "a") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(output_list)


def write_header(filename):
    with open(filename, "a") as csvfile:
        fieldnames = [
            "STRAP ID",
            "Folio ID",
            "Owner",
            "Site Address",
            "Property Description",
            "Aerial Viewer Link",
            "Parcel Details Link",
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
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)


def connect_to_base(browser, strap_id, filename):
    base_url = f"https://www.leepa.org/Search/PropertySearch.aspx?STRAP={strap_id}"
    connection_attempts = 0
    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
            "Cookie": "_ga=GA1.2.990083083.1567017963; _gid=GA1.2.953115123.1567017963; BIGipServer~external~www.pbcgov.com_papa=rd102o00000000000000000000ffff978433b0o80; ASP.NET_SessionId=oluqmbqiyhhts22tefyfix4e; _gat_gtag_UA_117168590_1=1; _gat_gtag_UA_70407948_1=1",
        }
    )
    session = requests.Session()

    while connection_attempts < 3:
        try:
            browser.get(base_url)
            sleep(1.5)
            browser.find_element_by_xpath(
                '//*[@id="ctl00_BodyContentPlaceHolder_SubmitPropertySearch"]'
            ).click()
            sleep(1.5)
            Xpath = '//*[@id="ctl00_BodyContentPlaceHolder_PropertySearchUpdatePanel"]/div[5]/table/tbody/tr[{}]/td[{}]'

            strapid = browser.find_elements_by_xpath(Xpath.format(1, 1))[0].text
            folioid = browser.find_elements_by_xpath(Xpath.format(2, 1))[0].text
            owner = browser.find_elements_by_xpath(Xpath.format(1, 2))[0].text
            site_address = browser.find_elements_by_xpath(Xpath.format(1, 3))[0].text
            property_desc = browser.find_elements_by_xpath(Xpath.format(2, 2))[0].text

            # open parcel detail
            r = session.get(
                f"https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={folioid}&TaxRollDetails=True&ElevationDetails=True#ElevationDetails"
            )
            soup = BeautifulSoup(r.text, features="html.parser")
            just = ""
            assessed = ""
            portability_applied = ""
            cap_assessed = ""
            taxable = ""
            cap_difference = ""
            if soup.find_all("table", {"class": "appraisalDetailsRight autoAlternate"}):
                propertyvalues = soup.find_all(
                    "table", {"class": "appraisalDetailsRight autoAlternate"}
                )[0]
                just = propertyvalues.find_all("td")[0].text.strip()
                assessed = propertyvalues.find_all("td")[1].text.strip()
                portability_applied = propertyvalues.find_all("td")[2].text.strip()
                cap_assessed = propertyvalues.find_all("td")[3].text.strip()
                taxable = propertyvalues.find_all("td")[4].text.strip()
                cap_difference = propertyvalues.find_all("td")[5].text.strip()
            land_units = ""
            units_value = ""
            number_buildings = ""
            bedrooms = ""
            tax_roll = ""
            historic_designation = ""
            if soup.find_all("table", {"class": "appraisalDetailsRight autoAlternate"}):
                attribute = soup.find_all(
                    "table", {"class": "appraisalDetailsRight autoAlternate"}
                )[1]
                land_units = attribute.find_all("td")[0].text.strip()
                units_value = attribute.find_all("td")[1].text.strip()
                number_buildings = attribute.find_all("td")[2].text.strip()
                bedrooms = attribute.find_all("td")[3].text.strip()
                tax_roll = attribute.find_all("td")[4].text.strip()
                historic_designation = attribute.find_all("td")[5].text.strip()
            community = ""
            panel = ""
            version = ""
            date = ""
            evacuation_zone = ""
            if soup.find_all("table", {"class": "appraisalDetailsRight autoAlternate"}):
                ElevationDetails = soup.find("table", {"class": "detailsTable"})
                community = ElevationDetails.find_all("td")[0].text.strip()
                panel = ElevationDetails.find_all("td")[1].text.strip()
                version = ElevationDetails.find_all("td")[2].text.strip()
                date = ElevationDetails.find_all("td")[3].text.strip()
                evacuation_zone = ElevationDetails.find_all("td")[4].text.strip()
            property_info = [
                strapid,
                folioid,
                owner,
                site_address,
                property_desc,
                f"http://gissvr.leepa.org/geoview2/?FolioID={folioid}",
                f"https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={folioid}",
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
            write_to_file(property_info, filename)
            return True
        except Exception as e:
            print(e)
            connection_attempts += 1
            print(f"Error connecting to {base_url}.")
            print(f"Attempt #{connection_attempts}.")
    return False


def run_process(strap_id, filename, headless):
    # init browser

    global count, total_count
    count += 1
    print(f"get {count} ({count/total_count*100:.2f}%) data")

    browser = get_driver(headless)
    if connect_to_base(browser, strap_id, filename):
        browser.quit()
    else:
        print("Error connecting to leepa.org")
        browser.quit()


if __name__ == "__main__":
    # headless mode?
    headless = False
    if len(sys.argv) > 1:
        if sys.argv[2] == "headless":
            print("Running in headless mode")
            headless = True
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
    output_filename = f"leepa_{output_timestamp}.csv"
    futures = []

    total_count = len(input_data)

    write_header(output_filename)
    count = 1
    with ThreadPoolExecutor() as executor:
        for strap_id in input_data:
            futures.append(
                executor.submit(run_process, strap_id, output_filename, headless)
            )

    wait(futures)
    end_time = time()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time: {elapsed_time} seconds")

# addapted from https://testdriven.io/blog/building-a-concurrent-web-scraper-with-python-and-selenium/
