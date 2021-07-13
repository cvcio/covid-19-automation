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
    DATA_JHU_BASE_PATH,
    FIX_CORDS,
    EXCLUDE_ROWS,
    REPO_JHU_URL,
    COLUMN_MAPPINGS,
)


class JHUStrategy(object):
    """
    JHUStrategy
    """

    def __init__(self, name=None, config=None, mongo=None):
        self.name = name if name != None else type(self).__name__
        self.config = config[0] if config != None else {}
        self.dataframe = None
        self.collection = "global"
        self.docs = []

    def clone(self, url, path):
        logging.debug("[JHU] Clone Repo {} on {}".format(url, path))
        shutil.rmtree(path, ignore_errors=True)
        Repo.clone_from(url, path)
        shutil.rmtree(path + "/.git")
        logging.debug("[JHU] Repo {} Cloned on {}".format(url, path))

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
            logging.debug("[JHU] Migrate Documents {}".format(len(self.docs)))
            deleted = coll.delete_many({"source": "jhu"})
            logging.debug(
                "[JHU] Migration Drop Docs, {} deleted from {} in {}s".format(
                    deleted.deleted_count,
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
            result = coll.insert_many(self.docs)
            logging.debug(
                "[JHU] Migration Completed, {} inserted in {} in {}s".format(
                    len(result.inserted_ids),
                    self.collection,
                    round(time.time() - start, 2),
                )
            )
        else:
            dates = [pd.to_datetime(datetime.today().strftime("%Y-%m-%d")) - timedelta(days=d) for d in range(5) if d > 0]
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
            logging.debug("[JHU] Migrate Documents {}".format(len(reqs)))
            result = coll.bulk_write(reqs)
            logging.debug(
                "[JHU] Migration Completed, {} inserted, {} modified in {} in {}s".format(
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

    def get(self):
        logging.debug("[JHU] Getting Data")
        if self.config.get("clone"):
            self.clone(REPO_JHU_URL, self.config.get("tmp") + "jhu")

        fips = pd.read_csv("./data/countries-mapping-jhu-wom.csv")
        fips = fips.rename(columns=COLUMN_MAPPINGS).to_dict("records")

        confirmed_df = pd.read_csv(
            self.config.get("tmp")
            + DATA_JHU_BASE_PATH
            + "time_series_covid19_confirmed_global.csv"
        )
        deaths_df = pd.read_csv(
            self.config.get("tmp")
            + DATA_JHU_BASE_PATH
            + "time_series_covid19_deaths_global.csv"
        )
        recovered_df = pd.read_csv(
            self.config.get("tmp")
            + DATA_JHU_BASE_PATH
            + "time_series_covid19_recovered_global.csv"
        )

        logging.debug("[JHU] Data Loaded")

        # remove unnecessary data such as the 3 cruise ships
        confirmed_df, deaths_df, recovered_df = self._remove_ships(
            confirmed_df, deaths_df, recovered_df
        )

        self._fix_misc(confirmed_df, deaths_df, recovered_df)

        # decolonize countries on table
        self._decolonization(confirmed_df, "Denmark")
        self._decolonization(confirmed_df, "France")
        self._decolonization(confirmed_df, "Netherlands")
        self._decolonization(confirmed_df, "United Kingdom")
        self._decolonization(deaths_df, "Denmark")
        self._decolonization(deaths_df, "France")
        self._decolonization(deaths_df, "Netherlands")
        self._decolonization(deaths_df, "United Kingdom")
        self._decolonization(recovered_df, "Denmark")
        self._decolonization(recovered_df, "France")
        self._decolonization(recovered_df, "Netherlands")
        self._decolonization(recovered_df, "United Kingdom")
        
        # merge states to countries
        confirmed_df = self._merge_states(confirmed_df, "Canada", "Confirmed")
        deaths_df = self._merge_states(deaths_df, "Canada", "Deaths")
        recovered_df = self._merge_states(recovered_df, "Canada", "Recovered")
        
        confirmed_df = self._merge_states(confirmed_df, "Australia", "Confirmed")
        deaths_df = self._merge_states(deaths_df, "Australia", "Deaths")
        recovered_df = self._merge_states(recovered_df, "Australia", "Recovered")
        
        confirmed_df = self._merge_states(confirmed_df, "China", "Confirmed")
        deaths_df = self._merge_states(deaths_df, "China", "Deaths")
        recovered_df = self._merge_states(recovered_df, "China", "Recovered")

        # do the fips stuff here
        confirmed_df[
            ["population", "lat", "long", "country", "iso2", "iso3", "uid"]
        ] = confirmed_df.apply(
            lambda x: self._get_fips(x, fips), axis=1, result_type="expand"
        )
        deaths_df[
            ["population", "lat", "long", "country", "iso2", "iso3", "uid"]
        ] = deaths_df.apply(
            lambda x: self._get_fips(x, fips), axis=1, result_type="expand"
        )
        recovered_df[
            ["population", "lat", "long", "country", "iso2", "iso3", "uid"]
        ] = recovered_df.apply(
            lambda x: self._get_fips(x, fips), axis=1, result_type="expand"
        )
        
        # recovered_df = recovered_df[recovered_df['Country/Region']!='Canada']
        
        dates = confirmed_df.columns[4:-7]
        # pivot table using melt
        confirmed_df = confirmed_df.melt(
            id_vars=[
                "Province/State",
                "Country/Region",
                "Lat",
                "Long",
                "population",
                "lat",
                "long",
                "country",
                "iso2",
                "iso3",
                "uid",
            ],
            value_vars=dates,
            var_name="Date",
            value_name="Confirmed",
        )

        # pivot table using melt
        deaths_df = deaths_df.melt(
            id_vars=[
                "Province/State",
                "Country/Region",
                "Lat",
                "Long",
                "population",
                "lat",
                "long",
                "country",
                "iso2",
                "iso3",
                "uid",
            ],
            value_vars=dates,
            var_name="Date",
            value_name="Deaths",
        )

        # pivot table using melt
        recovered_df = recovered_df.melt(
            id_vars=[
                "Province/State",
                "Country/Region",
                "Lat",
                "Long",
                "population",
                "lat",
                "long",
                "country",
                "iso2",
                "iso3",
                "uid",
            ],
            value_vars=dates,
            var_name="Date",
            value_name="Recovered",
        )
        
        confirmed_df.drop(
            [
                "Province/State",
                "Country/Region",
                "Lat",
                "Long",
            ],
            axis=1,
            inplace=True,
        )
        deaths_df.drop(
            [
                "Province/State",
                "Country/Region",
                "Lat",
                "Long",
            ],
            axis=1,
            inplace=True,
        )
        recovered_df.drop(
            [
                "Province/State",
                "Country/Region",
                "Lat",
                "Long",
            ],
            axis=1,
            inplace=True,
        )

        # merge data from deaths and confirmed to df
        df = confirmed_df.merge(
            right=deaths_df,
            how="left",
            on=["Date", "population", "lat", "long", "country", "iso2", "iso3", "uid"],
        )

        # merge data from recovered to df
        df = df.merge(
            right=recovered_df,
            how="left",
            on=["Date", "population", "lat", "long", "country", "iso2", "iso3", "uid"],
        )

        df = df.rename(columns=COLUMN_MAPPINGS)

        logging.debug("[JHU] Data Cleaned & Merged, Building...")
        df["date"] = pd.to_datetime(df["date"], format='%m/%d/%y')
        
        # df = df.groupby(["date", "population", "lat", "long", "country", "iso2", "iso3", "uid"])["cases", "deaths", "recovered"].sum().reset_index()
        df["recovered"] = df["recovered"].fillna(0)
        # Active: Active cases = total cases - total recovered - total deaths.
        df["active"] = df["cases"] - df["deaths"] - df["recovered"]
        
        group = (
            df.groupby(
                ["date", "population", "lat", "long", "country", "iso2", "iso3", "uid"]
            )[["cases", "deaths", "recovered", "active"]]
            .sum()
            .reset_index()
        )

        # calc new values per date on cases, deaths, recovered
        temp = group.groupby(["country", "date"])[["cases", "deaths", "recovered"]]
        temp = temp.sum().diff().reset_index()
        
        mask = temp["country"] != temp["country"].shift(1)
        temp.loc[mask, "cases"] = np.nan
        temp.loc[mask, "deaths"] = np.nan
        temp.loc[mask, "recovered"] = np.nan
        
        # renaming columns
        temp.columns = [
            "country",
            "date",
            "new_cases",
            "new_deaths",
            "new_recovered",
        ]
        
        # merging new values
        group = pd.merge(group, temp, on=["country", "date"])
        # filling na with 0
        group = group.fillna(0)

        # fixing data types
        group[
            [
                "population",
                "cases",
                "deaths",
                "recovered",
                "active",
                "new_cases",
                "new_deaths",
                "new_recovered",
            ]
        ] = group[
            [
                "population",
                "cases",
                "deaths",
                "recovered",
                "active",
                "new_cases",
                "new_deaths",
                "new_recovered",
            ]
        ].astype(
            "int"
        )

        df = group
        df["case_fatality_ratio"] = df.apply(calc_fatality_ratio, axis=1)
        df["incidence_rate"] = df.apply(calc_incidence_rate, axis=1)
        df["source"] = "jhu"
        df["last_updated_at"] = pd.to_datetime(datetime.today())
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
                "case_fatality_ratio",
                "incidence_rate",
                "source",
                "last_updated_at"
            ]
        ]

        logging.debug("[JHU] Shape {}".format(df.shape))
        logging.debug("[JHU] Data\n{}".format(df))
        logging.debug("[JHU] Done!")

        # docs = clean_docs(df.to_dict("records"))
        self.dataframe = df
        self.save_dataframe()
        return self

    def _remove_ships(self, cases, deaths, recovered):
        ships_rows = (
            cases["Province/State"].str.contains("Grand Princess")
            | cases["Province/State"].str.contains("Diamond Princess")
            | cases["Province/State"].str.contains("MS Zaandam")
            | cases["Country/Region"].str.contains("Grand Princess")
            | cases["Country/Region"].str.contains("Diamond Princess")
            | cases["Country/Region"].str.contains("MS Zaandam")
            | cases["Country/Region"].str.contains("Summer Olympics 2020")
        )
        cases = cases[~(ships_rows)]
        ships_rows = (
            deaths["Province/State"].str.contains("Grand Princess")
            | deaths["Province/State"].str.contains("Diamond Princess")
            | deaths["Province/State"].str.contains("MS Zaandam")
            | deaths["Country/Region"].str.contains("Grand Princess")
            | deaths["Country/Region"].str.contains("Diamond Princess")
            | deaths["Country/Region"].str.contains("MS Zaandam")
            | deaths["Country/Region"].str.contains("Summer Olympics 2020")
        )
        deaths = deaths[~(ships_rows)]
        ships_rows = (
            recovered["Province/State"].str.contains("Grand Princess")
            | recovered["Province/State"].str.contains("Diamond Princess")
            | recovered["Province/State"].str.contains("MS Zaandam")
            | recovered["Country/Region"].str.contains("Grand Princess")
            | recovered["Country/Region"].str.contains("Diamond Princess")
            | recovered["Country/Region"].str.contains("MS Zaandam")
            | recovered["Country/Region"].str.contains("Summer Olympics 2020")
        )
        recovered = recovered[~(ships_rows)]
        return cases, deaths, recovered

    def _fix_misc(self, cases, deaths, recovered):
        cases.loc[
            (cases["Province/State"] == "Hong Kong"), "Country/Region"
        ] = "Hong Kong"
        deaths.loc[
            (deaths["Province/State"] == "Hong Kong"), "Country/Region"
        ] = "Hong Kong"
        recovered.loc[
            (recovered["Province/State"] == "Hong Kong"), "Country/Region"
        ] = "Hong Kong"

        cases.loc[(cases["Province/State"] == "Macau"), "Country/Region"] = "Macau"
        deaths.loc[(deaths["Province/State"] == "Macau"), "Country/Region"] = "Macau"
        recovered.loc[
            (recovered["Province/State"] == "Macau"), "Country/Region"
        ] = "Macau"

    def _get_fips(self, x, fips):
        for y in fips:
            if (
                y["name_en"] == x["Country/Region"]
                or y["country"] == x["Country/Region"]
                or y["wom_map"] == x["Country/Region"]
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

        logging.warning("[JHU] MISSING FIPS ({})".format(x["Country/Region"]))
        return x["Country/Region"], 0.0, 0.0, x["Country/Region"], "", "", 0

    def _merge_states(self, df, country, key):
        df.loc[(df["Country/Region"] == country), "Province/State"] = ""
        df.loc[(df["Country/Region"] == country), "Lat"] = FIX_CORDS.get(country, 0.0)[
            "Lat"
        ]
        df.loc[(df["Country/Region"] == country), "Long"] = FIX_CORDS.get(country, 0.0)[
            "Long"
        ]

        temp = df[(df["Country/Region"] == country)]
        df.drop(temp.index, inplace = True)
        temp = temp.groupby(["Province/State", "Country/Region", "Lat", "Long"]).sum().reset_index()
        df = df.append(temp, ignore_index = True)
        return df
        
    def _decolonization(self, df, country):
        for index, row in df.iterrows():
            if (row["Country/Region"] == country) & pd.notnull(row["Province/State"]):
                df.loc[index, "Country/Region"] = row["Province/State"]
                df.loc[index, "Province/State"] = ""
