import logging
import time
import shutil
import requests
from requests.exceptions import HTTPError

import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from pymongo import ReplaceOne

from conf.constants import (
    FIX_CORDS,
    COLUMN_MAPPINGS,
)


class GovGRStrategy(object):
    """
    GovGRStrategy
    """

    def __init__(self, name=None, config=None, mongo=None):
        self.name = name if name != None else type(self).__name__
        self.config = config[0] if config != None else {}
        self.dataframe = None
        self.collection = "gr_vaccines"
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
            .get_database(self.config.get("db"))
            .get_collection(self.collection)
        )
        if self.config.get("drop"):
            logging.debug("[GOVGR] Migrate Documents {}".format(len(self.docs)))
            deleted = coll.delete_many({"source": "govgr"})
            logging.debug(
                "[GOVGR] Migration Drop Docs, {} deleted from {} in {}s".format(
                    deleted.deleted_count,
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
            result = coll.insert_many(self.docs)
            logging.debug(
                "[GOVGR] Migration Completed, {} inserted in {} in {}s".format(
                    len(result.inserted_ids),
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
        else:
            dates = [pd.to_datetime(datetime.today().strftime("%Y-%m-%d")) - timedelta(days=d) for d in range(5)]
            reqs = [
                ReplaceOne(
                    {
                        "date": doc["date"],
                        "uid": doc["uid"],
                        "region": doc["region"],
                        "source": doc["source"],
                    },
                    doc,
                    upsert=True,
                )
                for doc in self.docs
                if doc["date"] in dates
            ]
            logging.debug("[GOVGR] Migrate Documents {}".format(len(reqs)))
            result = coll.bulk_write(reqs)
            logging.debug(
                "[GOVGR] Migration Completed, {} inserted, {} modified in {} in {}s".format(
                    result.inserted_count,
                    result.modified_count,
                    self.collection,
                    round(time.time() - start, 2),
                )
            )

    def get(self):
        logging.debug("[GOVGR] Getting Data")
       
        fips = pd.read_csv("./data/region-mapping-imedd.csv")
        fips = fips[fips["areaid"].notna()]
        fips = fips.rename(columns=COLUMN_MAPPINGS).to_dict("records")
        
        url = "https://data.gov.gr/api/v1/query/mdg_emvolio?date_from=2020-12-27&date_to={}".format(datetime.today().strftime("%Y-%m-%d"))
        headers = {
            "Authorization": "Token {}".format(self.config.get("govgr_token"))
        }
        logging.debug("[GOVGR] Fetching Data from {}".format(url))
        try:
            response = requests.get(url, headers = headers)
            response.raise_for_status()
        except HTTPError:
            raise
        except Exception:
            raise
        
        logging.debug("[GOVGR] Data Loaded")

        df = pd.DataFrame.from_dict(response.json(), orient = "columns")
        df = df.rename(columns = {
            "totaldistinctpersons": "total_distinct_persons",
            "totalvaccinations": "total_vaccinations",
            "daytotal": "day_total",
            "daydiff": "day_diff"
        })
        df["date"] = pd.to_datetime(df["referencedate"])
        df = df.sort_values(by="date").reset_index(drop = True)
        df["uid"] = df["areaid"].apply(lambda x: "PE{}".format(x))
            
        group = (
            df.groupby(
                ["date", "area", "areaid", "uid"]
            )[["total_distinct_persons", "total_vaccinations", "day_total", "day_diff"]]
            .sum()
            .reset_index()
        )

        # calc new values per date on cases, deaths, recovered
        temp = group.groupby(["uid", "date"])[["total_distinct_persons", "total_vaccinations"]]
        temp = temp.sum().diff().reset_index()
        
        mask = temp["uid"] != temp["uid"].shift(1)
        temp.loc[mask, "total_distinct_persons"] = np.nan
        temp.loc[mask, "total_vaccinations"] = np.nan
        
        # renaming columns
        temp.columns = [
            "uid",
            "date",
            "new_total_distinct_persons",
            "new_total_vaccinations"
        ]
        
        # merging new values
        group = pd.merge(group, temp, on=["uid", "date"])
        # filling na with 0
        group = group.fillna(0)
        
        group[
            [
                "new_total_distinct_persons",
                "new_total_vaccinations"
            ]
        ] = group[
            [
                "new_total_distinct_persons",
                "new_total_vaccinations"
            ]
        ].astype(
            "int"
        )
        
        df = group
        
        df[
            ["geo_unit", "state", "region", "population", "lat", "long"]
        ] = df.apply(
            lambda x: self._get_fips(x, fips), axis=1, result_type="expand"
        )
        
        df["last_updated_at"] = pd.to_datetime(datetime.today())
        df["source"] = "govgr"
        
        # df = df.drop(columns=["referencedate"])
        df = df.sort_values(by="date").reset_index(drop = True)
        
        logging.debug("[GOVGR] Shape {}".format(df.shape))
        logging.debug("[GOVGR] Data\n{}".format(df))
        logging.debug("[GOVGR] Done!")
        
        self.dataframe = df
        self.save_dataframe()
        return self
    
    def get_last_occur_ncd(self, x, df):
        y = x["date"] - timedelta(days=1)
        y = y.strftime('%Y-%m-%d')
        d = df.loc[
            (df["date"] == y) & (df["uid"] == x["uid"])
        ]
        if len(d) == 0:
            return np.nan, np.nan
        new_cases = x["cases"] - d.iloc[0]["cases"]
        new_deaths = x["deaths"] - d.iloc[0]["deaths"] 
        return new_cases if new_cases > 0 else np.nan, new_deaths if new_deaths > 0 else np.nan

    def _get_fips(self, x, fips):
        for y in fips:
            if (
                y["areaid"] == x["areaid"]
            ):
                return (
                    y["geo_unit"],
                    y["state"],
                    y["region"],
                    y["population"],
                    y["lat"],
                    y["long"]
                )

        logging.warning("[GOVGR] MISSING FIPS ({})".format(x["county"]))
        return "", "", "", "", 0, 0.0, 0.0