import logging
import requests
import time

import pandas as pd
import numpy as np

from datetime import datetime
from bs4 import BeautifulSoup
from pymongo import ReplaceOne

from utils.numerical import (
    parse_float,
    parse_int,
    calc_fatality_ratio,
    calc_incidence_rate,
)
from utils.strings import normalize_keyword
from utils.requests import request_headers

from conf.constants import FIX_CORDS, EXCLUDE_ROWS, COLUMN_MAPPINGS, DATA_WOM_BASE_LINK


class WOMStrategy(object):
    """
    WOM Class
    """

    def __init__(self, name=None, config=None):
        self.name = name if name != None else type(self).__name__
        self.config = config[0] if config != None else {}
        self.dataframe = None
        self.collection = "global"
        self.docs = []

    def save_dataframe(self):
        self.dataframe.to_csv(
            "{}{}-{}.csv".format(
                self.config.get("output"),
                datetime.now().strftime("%Y-%m-%d"),
                self.name,
            ),
            index=False,
        )

    def as_docs(self, dataframe):
        docs = []
        for doc in dataframe.to_dict("records"):
            doc = self.geo_loc(doc)
            docs.append(doc)
        return docs

    def geo_loc(self, doc):
        lat = float(doc.pop("lat", 0.0))
        long = float(doc.pop("long", 0.0))
        if lat != 0.0 and long != 0.0:
            doc["loc"] = {"type": "Point", "coordinates": [long, lat]}
        return doc
    
    def migrate(self):
        start = time.time()
        self.docs = self.as_docs(self.dataframe)
        coll = (
            self.config.get("mongo_client")
            .get_database("covid19")
            .get_collection(self.collection)
        )
        if self.config.get("drop"):
            logging.debug("[WOM] Migrate Documents {}".format(len(self.docs)))
            deleted = coll.delete_many({"source": "worldometer"})
            logging.debug(
                "[WOM] Migration Drop Docs, {} deleted from {} in {}s".format(
                    deleted.deleted_count,
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
            result = coll.insert_many(self.docs)
            logging.debug(
                "[WOM] Migration Completed, {} inserted in {} in {}s".format(
                    len(result.inserted_ids),
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
        else:
            dates = [
                pd.to_datetime(datetime.today().strftime("%Y-%m-%d"))
            ]
            reqs = [
                ReplaceOne(
                    {
                        "date": doc["date"],
                        "uid": doc["uid"],
                        "iso3": doc["iso3"],
                        "country": doc["country"],
                        "source": doc["source"],
                    },
                    doc,
                    upsert=True,
                )
                for doc in self.docs
                if doc["date"] in dates
            ]
            logging.debug("[WOM] Migrate Documents {}".format(len(reqs)))
            result = coll.bulk_write(reqs)
            logging.debug(
                "[WOM] Migration Completed, {} inserted, {} modified in {} in {}s".format(
                    result.inserted_count,
                    result.modified_count,
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
            
    def clean(self):
        pass

    def format(self):
        pass

    def scrape_document(self, url):
        logging.debug("[WOM] Scraping url {}".format(url))
        page = requests.get(url, headers=request_headers())
        return BeautifulSoup(page.content, "html.parser")

    def get(self):
        logging.debug("[WOM] Getting Data")
        # get latest world data from worldometer
        # url: https://www.worldometers.info/coronavirus/
        soup = self.scrape_document(DATA_WOM_BASE_LINK)
        # find table
        table = soup.find("table", attrs={"id": "main_table_countries_today"})
        # get headers
        headers = [normalize_keyword(header.text) for header in table.find_all("th")]
        # iter rows
        rows = []
        for row in table.find_all("tr"):
            rows.append([normalize_keyword(val.text) for val in row.find_all("td")])
        # create the dataframe
        df = pd.DataFrame(rows[1:], columns=headers)
        # rename columns
        df = df.rename(columns=COLUMN_MAPPINGS)
        logging.debug("[WOM] Data Loaded")

        logging.debug("[WOM] Data Cleaned & Merged, Building...")

        now = datetime.now()
        df["date"] = pd.to_datetime(now.strftime("%Y-%m-%d"))
        df = df[~df["country"].isin(EXCLUDE_ROWS)]
        df = df.drop([""], axis=1).reset_index(drop=True)
        df["source"] = "worldometer"

        colunms_to_float = [
            "cases",
            "deaths",
            "recovered",
            "population",
            "tests",
            "new_cases",
            "new_deaths",
            "new_recovered",
            "active",
            "critical",
            "cases_per_1m_pop",
            "deaths_per_1m_pop",
            "test_per_1m_pop",
            "case_ratio",
            "death_ratio",
            "test_ratio",
        ]
        # convert all string numbers to floats
        # for the moment we choose float instead of integers
        for column in colunms_to_float:
            df[column] = df[column].apply(lambda x: parse_float(x))

        df[
            [
                "cases",
                "deaths",
                "recovered",
                "population",
                "tests",
                "new_cases",
                "new_deaths",
                "new_recovered",
                "active",
                "critical",
                "cases_per_1m_pop",
                "deaths_per_1m_pop",
                "test_per_1m_pop",
                "case_ratio",
                "death_ratio",
                "test_ratio",
            ]
        ] = df[
            [
                "cases",
                "deaths",
                "recovered",
                "population",
                "tests",
                "new_cases",
                "new_deaths",
                "new_recovered",
                "active",
                "critical",
                "cases_per_1m_pop",
                "deaths_per_1m_pop",
                "test_per_1m_pop",
                "case_ratio",
                "death_ratio",
                "test_ratio",
            ]
        ].fillna(
            0
        )

        # fixing data types
        df[
            [
                "cases",
                "deaths",
                "recovered",
                "population",
                "tests",
                "new_cases",
                "new_deaths",
                "new_recovered",
                "active",
                "critical",
            ]
        ] = df[
            [
                "cases",
                "deaths",
                "recovered",
                "population",
                "tests",
                "new_cases",
                "new_deaths",
                "new_recovered",
                "active",
                "critical",
            ]
        ].astype(
            "int"
        )

        df["case_fatality_ratio"] = df.apply(calc_fatality_ratio, axis=1)
        df["incidence_rate"] = df.apply(calc_incidence_rate, axis=1)

        fips = pd.read_csv(
            "./data/countries-mapping-jhu-wom.csv"
        ).to_dict("records")
        df[["population", "lat", "long", "country", "iso2", "iso3", "uid"]] = df.apply(
            lambda x: self._get_fips(x, fips), axis=1, result_type="expand"
        )
        df = df[
            [
                "date",
                "uid",
                "iso2",
                "iso3",
                "country",
                "lat",
                "long",
                "population",
                "cases",
                "deaths",
                "recovered",
                "active",
                "critical",
                "tests",
                "new_cases",
                "new_deaths",
                "new_recovered",
                "case_fatality_ratio",
                "incidence_rate",
                "source",
            ]
        ]
        
        df["last_updated_at"] = pd.to_datetime(datetime.today())

        logging.debug("[WOM] Shape {}".format(df.shape))
        logging.debug("[WOM] Data\n{}".format(df))
        logging.debug("[WOM] Done!")

        # docs = clean_docs(df.to_dict("records"))
        self.dataframe = df
        self.save_dataframe()
        return self

    def _get_fips(self, x, fips):
        for y in fips:
            if (
                y["name_en"] == x["country"]
                or y["country"] == x["country"]
                or y["wom_map"] == x["country"]
            ):
                return (
                    int(y["population"]),
                    float(y["lat"]),
                    float(y["long"]),
                    y["name_en"],
                    y["iso2"].upper(),
                    y["iso3"].upper(),
                    y["uid"],
                )

        logging.warning("[WOM] MISSING FIPS ({})".format(x["country"]))
        return x["country"], 0.0, 0.0, x["country"], "", "", 0
