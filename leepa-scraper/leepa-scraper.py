import csv
from selenium import webdriver
from concurrent.futures import ThreadPoolExecutor, wait
import datetime
import sys
from time import sleep, time
from webdriver_manager.chrome import ChromeDriverManager
from pathlib import Path
import pandas as pd

# argv[2]


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
        ]
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)


def connect_to_base(browser, strap_id, filename):
    base_url = f"https://www.leepa.org/Search/PropertySearch.aspx?STRAP={strap_id}"
    connection_attempts = 0
    while connection_attempts < 3:
        try:
            browser.get(base_url)
            sleep(4)
            browser.find_element_by_xpath(
                '//*[@id="ctl00_BodyContentPlaceHolder_SubmitPropertySearch"]'
            ).click()
            sleep(2)
            Xpath = '//*[@id="ctl00_BodyContentPlaceHolder_PropertySearchUpdatePanel"]/div[5]/table/tbody/tr[{}]/td[{}]'

            strapid = browser.find_elements_by_xpath(Xpath.format(1, 1))[0].text
            folioid = browser.find_elements_by_xpath(Xpath.format(2, 1))[0].text
            owner = browser.find_elements_by_xpath(Xpath.format(1, 2))[0].text
            site_address = browser.find_elements_by_xpath(Xpath.format(1, 3))[0].text
            property_desc = browser.find_elements_by_xpath(Xpath.format(2, 2))[0].text
            # parcel_detail = browser.find_elements_by_xpath(
            #     Xpath.format(1, 4) + "/table/tbody/tr[1]/th[1]/a"
            # )[0].get_attribute("href")
            property_info = [
                strapid,
                folioid,
                owner,
                site_address,
                property_desc,
                f"http://gissvr.leepa.org/geoview2/?FolioID={folioid}",
                f"https://www.leepa.org/Display/DisplayParcel.aspx?FolioID={folioid}",
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
        if sys.argv[1] == "headless":
            print("Running in headless mode")
            headless = True

    # set variables
    start_time = time()
    output_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    output_filename = f"leepa_{output_timestamp}.csv"
    futures = []

    # scrape and crawl
    STRAP_df = pd.read_excel("Lee County FL - Auction List 2021.xls")[
        "Account No. (STRAP)"
    ]
    total_count = len(STRAP_df)

    write_header(output_filename)
    count = 1
    with ThreadPoolExecutor() as executor:
        for strap_id in STRAP_df:
            futures.append(
                executor.submit(run_process, strap_id, output_filename, headless)
            )

    wait(futures)
    end_time = time()
    elapsed_time = end_time - start_time
    print(f"Elapsed run time: {elapsed_time} seconds")

# addapted from https://testdriven.io/blog/building-a-concurrent-web-scraper-with-python-and-selenium/
