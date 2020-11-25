"""
Set Constants
"""

ALLOWED_SOURCES = ["jhu", "worldometer", "imedd", "who", "sch"]
COLUMN_MAPPINGS = {
    "Country,Other": "country",
    "TotalCases": "cases",
    "NewCases": "new_cases",
    "TotalDeaths": "deaths",
    "NewDeaths": "new_deaths",
    "TotalRecovered": "recovered",
    "NewRecovered": "new_recovered",
    "ActiveCases": "active",
    "Serious,Critical": "critical",
    "Tot\xa0Cases/1M pop": "cases_per_1m_pop",
    "Deaths/1M pop": "deaths_per_1m_pop",
    "TotalTests": "tests",
    "Tests/\n1M pop": "test_per_1m_pop",
    "Population": "population",
    "Continent": "continent",
    "1 Caseevery X ppl": "case_ratio",
    "1 Deathevery X ppl": "death_ratio",
    "1 Testevery X ppl": "test_ratio",
    "Σχολείο/Δομή": "school",
    "Περιοχή": "region",
    "Διεύθυνση": "address",
    "Αναστολή έως και": "dueTo",
    "Παρατηρήσεις:": "notes",
    "Country/Region": "country",
    "Date": "date",
    "Confirmed": "cases",
    "Deaths": "deaths",
    "Recovered": "recovered",
    "Active": "active",
    "New Cases": "new_cases",
    "New Deaths": "new_deaths",
    "New Recovered": "new_recovered",
    "Case-Fatality Ratio": "case_fatality_ratio",
    "Admin2": "admin_2",
    "Country_Region": "country",
    "Lat": "lat",
    "Long_": "long",
    "Long": "long",
    "Population": "population",
    "UID": "uid",
}
EXCLUDE_ROWS = [
    "",
    "North America",
    "Asia",
    "South America",
    "Europe",
    "Africa",
    "Oceania",
    "World",
    "Total:",
    "MS Zaandam",
    "Diamond Princess",
    "Wallis and Futuna"
]

OUTPUT = "data/"
TMP = "tmp/"

DATA_JHU_BASE_PATH = "jhu/csse_covid_19_data/csse_covid_19_time_series/"
DATA_IMEDD_BASE_PATH = "imedd/COVID-19/"
DATA_WOM_BASE_LINK = "https://www.worldometers.info/coronavirus/"
DATA_SCH_BASE_LINK = "https://www.sch.gr/anastoli/web/index.php"
REPO_JHU_URL = "https://github.com/CSSEGISandData/COVID-19.git"
REPO_IMEDD_URL = "https://github.com/iMEdD-Lab/open-data.git"

FIX_CORDS = {
    "Canada": {"Lat": 56.1304, "Long": -106.3468},
    "China": {"Lat": 35.8617, "Long": 104.1954},
    "Australia": {"Lat": -25.2744, "Long": 133.7751},
}
