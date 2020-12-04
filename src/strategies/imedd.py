import logging
import time
import shutil

from git import Repo
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from utils.numerical import calc_fatality_ratio, calc_incidence_rate
from pymongo import ReplaceOne

from conf.constants import (
    DATA_IMEDD_BASE_PATH,
    FIX_CORDS,
    EXCLUDE_ROWS,
    REPO_IMEDD_URL,
    COLUMN_MAPPINGS,
)


class IMEDDStrategy(object):
    """
    IMEDDStrategy
    """

    def __init__(self, name=None, config=None, mongo=None):
        self.name = name if name != None else type(self).__name__
        self.config = config[0] if config != None else {}
        self.dataframe = None
        self.collection = "greece"
        self.docs = []

    def clone(self, url, path):
        logging.debug("[IMEDD] Clone Repo {} on {}".format(url, path))
        shutil.rmtree(path, ignore_errors=True)
        Repo.clone_from(url, path)
        shutil.rmtree(path + "/.git")
        logging.debug("[IMEDD] Repo {} Cloned on {}".format(url, path))

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
            logging.debug("[IMEDD] Migrate Documents {}".format(len(self.docs)))
            deleted = coll.delete_many({"source": "imedd"})
            logging.debug(
                "[IMEDD] Migration Drop Docs, {} deleted from {} in {}s".format(
                    deleted.deleted_count,
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
            result = coll.insert_many(self.docs)
            logging.debug(
                "[IMEDD] Migration Completed, {} inserted in {} in {}s".format(
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
            logging.debug("[IMEDD] Migrate Documents {}".format(len(reqs)))
            result = coll.bulk_write(reqs)
            logging.debug(
                "[IMEDD] Migration Completed, {} inserted, {} modified in {} in {}s".format(
                    result.inserted_count,
                    result.modified_count,
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
            
    def enrich_global(self):
        logging.debug("[IMEDD] Enrich Global")
        start = time.time()
        timeline = self.get_timeline();
        docs = self.as_docs(timeline)
        coll = (
            self.config.get("mongo_client")
            .get_database(self.config.get("db"))
            .get_collection("global")
        )
        if self.config.get("drop"):
            deleted = coll.delete_many(
                {
                    "iso3": "GRC",
                    "date": { 
                        "$in": timeline["date"].tolist()
                    }
                }
            )
            logging.debug(
                "[IMEDD] Migration Drop Docs, {} deleted from {} in {}s".format(
                    deleted.deleted_count,
                    "global",
                    round(time.time() - start, 2),
                )
            )
            
            result = coll.insert_many(docs)
            logging.debug(
                "[IMEDD] Migration Completed, {} inserted in {} in {}s".format(
                    len(result.inserted_ids),
                    "global",
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
                        "country": doc["country"],
                        "iso3": doc["iso3"],
                        "source": { "$in": ["imedd", "jhu"]}
                    },
                    doc,
                    upsert=True,
                )
                for doc in docs
                if doc["date"] in dates
            ]
            logging.debug("[IMEDD] Migrate Documents {}".format(len(reqs)))
            result = coll.bulk_write(reqs)
            logging.debug(
                "[IMEDD] Migration Completed, {} inserted, {} modified in {} in {}s".format(
                    result.inserted_count,
                    result.modified_count,
                    "global",
                    round(time.time() - start, 2),
                )
            )

    def clean(self):
        pass

    def format(self):
        pass
    
    def get_timeline(self):
        logging.debug("[IMEDD] Getting Timeline Data")
        
        fips = pd.read_csv("./data/countries-mapping-jhu-wom.csv")
        fips = fips.rename(columns=COLUMN_MAPPINGS)
        
        greece_fips = fips.loc[(fips["country"] == "Greece")].to_dict("records")
        df = pd.read_csv(
            self.config.get("tmp")
            + DATA_IMEDD_BASE_PATH
            + "greeceTimeline.csv"
        )
        
        # now_df = now_df[now_df['Country/Region'].notnull()]
        
        df = df.rename(columns={"Date": "date", "Status": "status", "Province/State": "state", "Country/Region": "county"})
        dates = df.columns[3:]
        
         # pivot table using melt
        df = df.melt(
            id_vars=[
                "status"
            ],
            value_vars=dates,
            var_name="date",
            value_name="value",
        )
        
        df["date"] = pd.to_datetime(df["date"])
        df = df.pivot_table("value", ["date"], "status")
        df = df.reset_index()
        df[
            ["population", "lat", "long", "country", "iso2", "iso3", "uid"]
        ] = [
            greece_fips[0]["population"], 
            greece_fips[0]["lat"], 
            greece_fips[0]["long"], 
            greece_fips[0]["country"], 
            greece_fips[0]["iso2"], 
            greece_fips[0]["iso3"],
            greece_fips[0]["uid"]
        ]
        df = df.rename(columns={
            "cases": "new_cases", "deaths": "new_deaths", 
            "hospitalized": "new_hospitalized",
            "total cases": "cases",
            "intubated": "critical",
            "estimated_new_total_tests": "new_tests"
        }) 
        
        df["deaths"] = df.new_deaths.cumsum()
        df["deaths"] = df["deaths"].fillna(method='pad')
        df["tests"] = df.new_tests.cumsum()
        df["critical"] = df["critical"].fillna(0)
        df["recovered"] = df["recovered"].fillna(method='pad')
        
        group = (
            df.groupby(
               ["date", "population", "lat", "long", "country", "iso2", "iso3", "uid"]
            )[["recovered"]] # , "recovered", "active"
            .sum()
            .reset_index()
        )

        # # calc new values per date on cases, deaths, recovered
        temp = group.groupby(["uid", "date"])[["recovered"]] # , "recovered"
        temp = temp.sum().diff().reset_index()

        mask = temp["uid"] != temp["uid"].shift(1)
        temp.loc[mask, "recovered"] = np.nan
        # renaming columns
        temp.columns = [
            "uid",
            "date",
            "new_recovered",
        ]
        
        # merging new values
        df = pd.merge(df, temp, on=["uid", "date"])
        # filling na with 0
        df = df.fillna(0)
                
        # df["new_recovered"] = df.recovered.cumsum()
        df["active"] = df["cases"] - df["deaths"] - df["recovered"]

        # fixing data types
        df[
            [
                "population",
                "cases",
                "deaths",
                "recovered",
                "active",
                "new_cases",
                "new_deaths",
                "new_recovered",
                "new_hospitalized", 
                "intensive_care", 
                "critical", 
                "cumulative_rtpcr_tests_raw",
                "estimated_new_rtpcr_tests", 
                "cumulative_rapid_tests_raw",
                "esitmated_new_rapid_tests",
                "tests",
                "new_tests"
            ]
        ] = df[
            [
                "population",
                "cases",
                "deaths",
                "recovered",
                "active",
                "new_cases",
                "new_deaths",
                "new_recovered",
                "new_hospitalized", 
                "intensive_care", 
                "critical", 
                "cumulative_rtpcr_tests_raw",
                "estimated_new_rtpcr_tests", 
                "cumulative_rapid_tests_raw",
                "esitmated_new_rapid_tests",
                "tests",
                "new_tests"
            ]
        ].astype(
            "int"
        )

        # df = group
        df["case_fatality_ratio"] = df.apply(calc_fatality_ratio, axis=1)
        df["incidence_rate"] = df.apply(calc_incidence_rate, axis=1)
        df["source"] = "imedd"
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
                "new_cases",
                "new_deaths",
                "new_recovered",
                "new_hospitalized", 
                "intensive_care", 
                "critical", 
                "incidence_rate",
                "case_fatality_ratio",
                "cumulative_rtpcr_tests_raw",
                "estimated_new_rtpcr_tests", 
                "cumulative_rapid_tests_raw",
                "esitmated_new_rapid_tests",
                "tests",
                "new_tests",
                "source",
            ]
        ]
        
        df["iso2"] = df["iso2"].str.upper() 
        df["iso3"] = df["iso3"].str.upper() 
        df["last_updated_at"] = pd.to_datetime(datetime.today())
        df.to_csv(
            "{}{}-{}-timeline.csv".format(
                self.config.get("output"),
                datetime.now().strftime("%Y-%m-%d"),
                self.name,
            ),
            index=False,
        )
        
        return df
        

    def get(self):
        logging.debug("[IMEDD] Getting Data")
        if self.config.get("clone"):
            self.clone(REPO_IMEDD_URL, self.config.get("tmp") + "imedd")
       
        fips = pd.read_csv("./data/region-mapping-imedd.csv")
        fips = fips.rename(columns=COLUMN_MAPPINGS).to_dict("records")
        
        now = pd.to_datetime(datetime.today().strftime("%m/%d/%Y"))
        yesterday = datetime.today() - timedelta(days=1)
        yesterday = yesterday.strftime('%Y-%m-%d')

        confirmed_df = pd.read_csv(
            self.config.get("tmp")
            + DATA_IMEDD_BASE_PATH
            + "greece_cases_v2.csv"
        )
        deaths_df = pd.read_csv(
            self.config.get("tmp")
            + DATA_IMEDD_BASE_PATH
            + "greece_deaths_v2.csv"
        )
        
        now_df = pd.read_csv(
            self.config.get("tmp")
            + DATA_IMEDD_BASE_PATH
            + "greece_latest.csv"
        )
        now_df = now_df[now_df.county_normalized.notnull()]

        logging.debug("[IMEDD] Data Loaded")
        
        # do the fips stuff here
        confirmed_df[
            ["uid", "geo_unit", "state", "region", "population", "lat", "long"]
        ] = confirmed_df.apply(
            lambda x: self._get_fips(x, fips), axis=1, result_type="expand"
        )
        deaths_df[
            ["uid", "geo_unit", "state", "region", "population", "lat", "long"]
        ] = deaths_df.apply(
            lambda x: self._get_fips(x, fips), axis=1, result_type="expand"
        )
        now_df[
            ["uid", "geo_unit", "state", "region", "population", "lat", "long"]
        ] = now_df.apply(
            lambda x: self._get_fips(x, fips), axis=1, result_type="expand"
        )
        
        # drop values with no fipss
        confirmed_df = confirmed_df[confirmed_df["uid"].str.strip().astype(bool)]
        confirmed_df = confirmed_df.drop(["Γεωγραφικό Διαμέρισμα", "Περιφέρεια", "county_normalized", "county", "pop_11"], axis = 1)

        deaths_df = deaths_df[deaths_df["uid"].str.strip().astype(bool)]
        deaths_df = deaths_df.drop(["Γεωγραφικό Διαμέρισμα", "Περιφέρεια", "county_normalized", "county", "pop_11"], axis = 1)
        
        now_df = now_df[now_df["uid"].str.strip().astype(bool)]
        now_df = now_df.drop(["Γεωγραφικό Διαμέρισμα", "Περιφέρεια", "county_normalized", "county", "pop_11", "county_en", "Πρωτεύουσα"], axis = 1)
        now_df["date"] = now
       
        confirmed_df = confirmed_df[["uid", "geo_unit", "state", "region", "population", "lat", "long"] + confirmed_df.columns[:-7].tolist()]
        deaths_df = deaths_df[["uid", "geo_unit", "state", "region", "population", "lat", "long"] + deaths_df.columns[:-7].tolist()]
        
        dates = confirmed_df.columns[7:]
        if len(confirmed_df.columns[7:]) < len(deaths_df.columns[7:]):
            confirmed_df[deaths_df.columns.tolist()[-1]] = confirmed_df[confirmed_df.columns[-1]]
        elif len(confirmed_df.columns[7:]) > len(deaths_df.columns[7:]):
            deaths_df[confirmed_df.columns.tolist()[-1]] = deaths_df[deaths_df.columns[-1]]
            
        # pivot table using melt
        confirmed_df = confirmed_df.melt(
            id_vars=[
                "uid", "geo_unit", "state", "region", "population", "lat", "long"
            ],
            value_vars=dates,
            var_name="date",
            value_name="cases",
        )
        
        # pivot table using melt
        deaths_df = deaths_df.melt(
            id_vars=[
               "uid", "geo_unit", "state", "region", "population", "lat", "long"
            ],
            value_vars=dates,
            var_name="date",
            value_name="deaths",
        )
                        
        # merge data from deaths and confirmed to df
        df = confirmed_df.merge(
            right=deaths_df,
            how="left",
            on=["date", "uid", "geo_unit", "state", "region", "population", "lat", "long"],
        )
        
        # df = df.append(now_df, ignore_index = True)
        logging.debug("[IMEDD] Data Cleaned & Merged, Building...")
        
        df["date"] = pd.to_datetime(df["date"])
        
        # calc new values per date on cases, deaths, recovered
        temp = df.groupby(["uid", "date"])[["cases", "deaths"]]
        temp = temp.sum().diff().reset_index()
        
        mask = temp["uid"] != temp["uid"].shift(1)
        temp.loc[mask, "cases"] = np.nan
        temp.loc[mask, "deaths"] = np.nan
        # renaming columns
        temp.columns = [
            "uid",
            "date",
            "new_cases",
            "new_deaths",
        ]        
        # merging new values
        df = pd.merge(df, temp, on=["uid", "date"])
        # df = group
        # df[["new_cases", "new_deaths"]] = df.apply(lambda x: self.get_last_occur_ncd(x, df), axis=1, result_type="expand")
        if len(df.loc[df["date"] == now]) == 0:
            # print(now_df)
            now_df[["cases", "deaths"]] = now_df.apply(lambda x: self.get_last_occur_cd(x, df), axis=1, result_type="expand")
            df = df.append(now_df, ignore_index = True)
        
        # filling na with 0
        df = df.fillna(0)

        # # fixing data types
        df[
            [
                "population",
                "cases",
                "deaths",
                "new_cases",
                "new_deaths",
            ]
        ] = df[
            [
                "population",
                "cases",
                "deaths",
                "new_cases",
                "new_deaths",
            ]
        ].astype(
            "int"
        )
        df["case_fatality_ratio"] = df.apply(calc_fatality_ratio, axis=1)
        df["incidence_rate"] = df.apply(calc_incidence_rate, axis=1)
        df["source"] = "imedd"
        df = df[
            [
                "date",
                "uid",
                "geo_unit", 
                "state", 
                "region", 
                "population",
                "cases",
                "deaths",
                "new_cases",
                "new_deaths",
                "case_fatality_ratio",
                "incidence_rate",
                "source", "lat", "long"
            ]
        ]
        
        df["last_updated_at"] = pd.to_datetime(datetime.today())

        logging.debug("[IMEDD] Shape {}".format(df.shape))
        logging.debug("[IMEDD] Data\n{}".format(df))
        logging.debug("[IMEDD] Done!")

        # docs = clean_docs(df.to_dict("records"))
        self.dataframe = df
        self.save_dataframe()
        return self

    def _fix_misc(self, cases, deaths, recovered):
        pass
    
    def get_last_occur_cd(self, x, df):
        y = x["date"] - timedelta(days=1)
        y = y.strftime('%Y-%m-%d')
        d = df.loc[
            (df["date"] == y) & (df["uid"] == x["uid"])
        ]
        if len(d) == 0:
            return np.nan, np.nan
        return d.iloc[0]["cases"] ,d.iloc[0]["deaths"]
    
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
                y["region_el"] == x["county"]
                or y["map_value"] == x["county"]
            ):
                return (
                    y["uid"],
                    y["geo_unit"],
                    y["state"],
                    y["region"],
                    y["population"],
                    y["lat"],
                    y["long"]
                )

        logging.warning("[IMEDD] MISSING FIPS ({})".format(x["county"]))
        return "", "", "", "", 0, 0.0, 0.0