import logging
import requests

import pandas as pd
import numpy as np

from datetime import datetime
from bs4 import BeautifulSoup
from utils.numerical import (
    parse_float,
    parse_int,
    calc_fatality_ratio,
    calc_incidence_rate,
)
from utils.strings import normalize_keyword
from utils.requests import request_headers

from conf.constants import (
    DATA_SCH_BASE_LINK,
    COLUMN_MAPPINGS
)

class SCHStrategy(object):
    """
    SCHStrategy
    """
    def __init__(self, name=None, config=None, mongo=None):
        self.name = name if name != None else type(self).__name__
        self.config = config[0] if config != None else {}
        self.dataframe = None
        self.docs = []

    def save_dataframe(self):
        self.dataframe.to_csv("{}{}-{}.csv".format(
            self.config.get("output"), 
            datetime.now().strftime("%Y-%m-%d"),
            self.name, 
        ), index=False)
    
    def as_docs(self):
        self.docs = []
        for doc in self.dataframe.to_dict("records"):
            doc = self.geo_loc(doc)
            self.docs.append(doc)
        return self.docs
    
    def geo_loc(self, doc):
        lat = float(doc.pop('lat', 0.0))
        long = float(doc.pop('long', 0.0))
        if lat != 0.0 and long != 0.0:
            doc['loc'] = {'type': 'Point', 'coordinates': [long, lat]}
        return doc
    
    def migrate(self):
        pass
    
    def clean(self):
        pass
    
    def format(self):
        pass
    
    def scrape_document(self, url):
        logging.debug("[SCH] Scraping url {}".format(url))
        page = requests.get(url, headers=request_headers())
        return BeautifulSoup(page.content, "html.parser")
    
    def get(self):
        logging.debug("[SCH] Getting Data")
        
        size = 100
        page = 1
        pages = 1
        # get page
        soup = self.scrape_document(DATA_SCH_BASE_LINK)
        # get total length
        length = soup.find("div", attrs={"class": "summary"}).find_all("b")[-1].get_text()
        pages = int(length) / size
        
        logging.debug("[SCH] Paging {} {} {} {}".format(length, size, page, pages))
        
        # get table
        table = soup.find("table", attrs={"class": "kv-grid-table"})

        # get headers
        headers = [normalize_keyword(header.text) for header in table.find_all("th")]

        # iter rows
        rows = []
        for row in table.find_all("tr"):
            rows.append([normalize_keyword(val.text) for val in row.find_all("td")])
            
        #  loop through pages
        while page < pages:
            page += 1
            soup = self.scrape_document(DATA_SCH_BASE_LINK + "?page=" + str(page))
            # get table
            table = soup.find("table", attrs={"class": "kv-grid-table"})

            # iter rows
            for row in table.find_all("tr"):
                rows.append([normalize_keyword(val.text) for val in row.find_all("td")])

        # create the dataframe
        df = pd.DataFrame(rows, columns=headers)
        # rename columns
        df = df.rename(columns=COLUMN_MAPPINGS)
        df = df[df["school"].str.strip().astype(bool)]
        
        logging.debug("[SCH] Data Cleaned & Merged, Building...")
        logging.debug("[SCH] Shape {}".format(df.shape))
        logging.debug("[SCH] Data\n{}".format(df))
        logging.debug("[SCH] Done!")
        
        # docs = clean_docs(df.to_dict("records"))
        self.dataframe = df
        self.save_dataframe()
        return self
