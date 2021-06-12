#!/usr/bin/env python
# coding: utf-8

import csv
import re
import datetime
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep, time

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver


def get_driver(headless):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--start-maximized")
    # initialize driver
    driver = webdriver.Chrome(options=options)
    return driver


headless = False
browser = get_driver(headless)

browser.get(
    "https://sef.mlsmatrix.com/Matrix/Public/Portal.aspx?ID=DE-115508346630&eml=b2treW1hYnJ1ckBnbWFpbC5jb20="
)

browser.find_elements_by_xpath(
    '//*[@id="wrapperTable"]/div/div[2]/div[2]/div[1]/span/a'
)[0].click()

data = pd.DataFrame(
    columns=[
        "address",
        "zipcode",
        "property_type",
        "mls",
        "beds",
        "fbaths",
        "hbaths",
        "sqFt",
    ]
)

while True:
    sleep(3)
    browser.find_elements_by_xpath(
        "/html/body/form/div[3]/div/div/div[5]/div[2]/div/div[1]/div/div/span/ul/li[2]"
    )[0].click()

    address = browser.find_elements_by_xpath(
        '//*[@id="wrapperTable"]/div/div/div[1]/div[1]/div/div[1]/span'
    )[0].text
    zipcode = browser.find_elements_by_xpath(
        '//*[@id="wrapperTable"]/div/div/div[1]/div[1]/div/div[2]/span'
    )[0].text

    # Xpath =
    property_type = browser.find_elements_by_xpath(
        '//*[@id="wrapperTable"]/div/div/div[6]/div/div/div[1]/div/div[2]/span'
    )[0].text
    mls = browser.find_elements_by_xpath(
        '//*[@id="wrapperTable"]/div/div/div[6]/div/div/div[2]/div/div[2]/span'
    )[0].text
    beds = browser.find_elements_by_xpath(
        '//*[@id="wrapperTable"]/div/div/div[6]/div/div/div[10]/div/div[2]/span'
    )[0].text
    fbaths = browser.find_elements_by_xpath(
        '//*[@id="wrapperTable"]/div/div/div[6]/div/div/div[11]/div/div[2]/span'
    )[0].text
    hbaths = browser.find_elements_by_xpath(
        '//*[@id="wrapperTable"]/div/div/div[6]/div/div/div[12]/div/div[2]/span'
    )[0].text
    sqFt = browser.find_elements_by_xpath(
        '//*[@id="wrapperTable"]/div/div/div[6]/div/div/div[13]/div/div[2]/span'
    )[0].text

    data = data.append(
        {
            "address": address,
            "zipcode": zipcode,
            "property_type": property_type,
            "mls": mls,
            "beds": beds,
            "fbaths": fbaths,
            "hbaths": hbaths,
            "sqFt": sqFt,
        },
        ignore_index=True,
    )

df = data.drop_duplicates()

df["zip"] = df["zipcode"].str.extract("(\d{5,})").astype(int)
df.to_csv("mlsmatrix.csv", index=False)


# https://www.masterbrokersforum.com/miami-mls-listing/A10426531.htm
