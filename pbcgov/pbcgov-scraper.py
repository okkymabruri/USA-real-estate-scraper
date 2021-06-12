import csv
import requests
from bs4 import BeautifulSoup
import sys
from importlib import reload
from time import time

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
file = open(f"pbcgov_{output_timestamp} - {input_file[:10]}.csv", "a")

writer = csv.writer(file, dialect="myDialect1")
writer.writerow(
    [
        "Location Address",
        "Municipality",
        "Parcel Control Number ",
        "Subdivision",
        "Official Records Book/Page",
        "Sale Date",
        "Legal Description",
        "Owner(s)",
        "Mailing Address",
        "Number of Units",
        "Total Square Feet",
        "Acres",
        "Property Use Code",
        "Zoning",
        "2020 Assessed Value",
        "2020 Exemption Amount",
        "2020 Taxable Value",
    ]
)

addresslist = []
if input_data:
    addresslist = input_data


session = requests.Session()

session.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36",
        "Cookie": "_ga=GA1.2.990083083.1567017963; _gid=GA1.2.953115123.1567017963; BIGipServer~external~www.pbcgov.com_papa=rd102o00000000000000000000ffff978433b0o80; ASP.NET_SessionId=oluqmbqiyhhts22tefyfix4e; _gat_gtag_UA_117168590_1=1; _gat_gtag_UA_70407948_1=1",
    }
)
session = requests.Session()


count = 0
total_count = len(addresslist)

for address in addresslist:
    url = (
        "https://www.pbcgov.com/papa/Asps/PropertyDetail/PropertyDetail.aspx?parcel={}"
    )
    r = session.get(url.format(address))
    soup = BeautifulSoup(r.text, features="html.parser")
    try:
        # Property Detail
        location = ""
        municipality = ""
        pcn = ""
        subdivision = ""
        records_book = ""
        sale_date = ""
        legal_desc = ""
        if soup.find("div", {"id": "propertyDetailDiv"}):
            PropertyDetail = soup.find("div", {"id": "propertyDetailDiv"})
            # Location Address
            location = PropertyDetail.find_all(
                "span", {"id": "MainContent_lblLocation"}
            )[0].text
            # Municipality
            municipality = PropertyDetail.find_all(
                "span", {"id": "MainContent_lblMunicipality"}
            )[0].text
            # Parcel Control Number
            pcn = PropertyDetail.find_all("span", {"id": "MainContent_lblPCN"})[0].text
            # Subdivision
            subdivision = PropertyDetail.find_all(
                "span", {"id": "MainContent_lblSubdiv"}
            )[0].text
            # Official Records Book/Page
            if PropertyDetail.find_all("span", {"id": "MainContent_lblBook"}):
                records_book = (
                    PropertyDetail.find_all("span", {"id": "MainContent_lblBook"})[
                        0
                    ].text
                    + "/"
                    + PropertyDetail.find_all("span", {"id": "MainContent_lblPage"})[
                        0
                    ].text
                )
            # Sale Date
            sale_date = PropertyDetail.find_all(
                "span", {"id": "MainContent_lblSaleDate"}
            )[0].text
            # Legal Description
            legal_desc = PropertyDetail.find_all(
                "span", {"id": "MainContent_lblLegalDesc"}
            )[0].text.strip()

        # ownerInformationDiv
        owner = ""
        mailing = ""
        if soup.find("div", {"id": "ownerInformationDiv"}):
            ownerInformation = soup.find("div", {"id": "ownerInformationDiv"})
            owner = (
                ownerInformation.find("table")
                .find_all("table")[0]
                .find_all("td", class_="TDValueLeft")[0]
                .text
            )
            mailing = (
                (
                    ownerInformation.find("table")
                    .find_all("table")[1]
                    .find_all("td", class_="TDValueLeft")[0]
                    .text
                    + ownerInformation.find("table")
                    .find_all("table")[1]
                    .find_all("td", class_="TDValueLeft")[2]
                    .text
                )
                .strip()
                .replace("\n\n", ", ")
            )
        # propertyInformationDiv
        number_unit = ""
        square_feet = ""
        acres = ""
        property_use_code = ""
        zoning = ""
        if soup.find("div", {"id": "propertyInformationDiv"}):
            propertyInformation = soup.find("div", {"id": "propertyInformationDiv"})
            # Number of Units
            number_unit = propertyInformation.find_all(
                "span", {"id": "MainContent_lblUnits"}
            )[0].text
            # Total Square Feet"
            square_feet = propertyInformation.find_all(
                "span", {"id": "MainContent_lblSqFt"}
            )[0].text
            # Acres
            acres = propertyInformation.find_all(
                "span", {"id": "MainContent_lblAcres"}
            )[0].text
            # Property Use Code
            property_use_code = propertyInformation.find_all(
                "span", {"id": "MainContent_lblUsecode"}
            )[0].text.strip()
            # Zoning
            zoning = propertyInformation.find_all(
                "span", {"id": "MainContent_lblZoning"}
            )[0].text.strip()
        # assessedValuesDiv
        assessed_value = ""
        exemption_amount = ""
        taxable_value = ""
        if soup.find("div", {"id": "assessedValuesDiv"}):
            assessedValues = soup.find("div", {"id": "assessedValuesDiv"})
            # 2020 Assessed Value
            assessed_value = assessedValues.find_all(
                "span", {"id": "MainContent_lblAssessedValue1"}
            )[0].text

            # 2020 Exemption Amount
            exemption_amount = assessedValues.find_all(
                "span", {"id": "MainContent_lblExemptionAmt1"}
            )[0].text
            # 2020 Taxable Value
            taxable_value = assessedValues.find_all(
                "span", {"id": "MainContent_lblTaxableValue1"}
            )[0].text

        writer.writerow(
            [
                location,
                municipality,
                pcn,
                subdivision,
                records_book,
                sale_date,
                legal_desc,
                owner,
                mailing,
                number_unit,
                square_feet,
                acres,
                property_use_code,
                zoning,
                assessed_value,
                exemption_amount,
                taxable_value,
            ]
        )
    except:
        pass
    print(f"{pcn}, total data {count}/{total_count}")
    count += 1

# print(r.text)
# Addapted from https://github.com/webdeveloper001/electron-xterm-scrapy/blob/master/scrappers/pbcgov/pbcgov.py