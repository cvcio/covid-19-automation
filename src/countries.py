import pandas as pd
"""
en = pd.read_csv("/home/andefined/python/cvcio/covid-19-automation/data/world_countries-master/data/en/world.csv")
gr = pd.read_csv("/home/andefined/python/cvcio/covid-19-automation/data/world_countries-master/data/el/world.csv")
wom = pd.read_csv("/home/andefined/python/cvcio/covid-19-automation/data/wom-countries.csv").to_dict("records")

df = en.merge(gr, how="outer", on="id")
df = df.rename(columns={"name_x": "name_en", "name_y": "name_el", "alpha2_x": "iso2", "alpha3_x": "iso3", "id": "uid"})
df = df[[
    "name_en",
    "name_el",
    "iso2",
    "iso3",
    "uid"
]]

fips = pd.read_csv("/home/andefined/python/cvcio/covid-19-automation/tmp/jhu/csse_covid_19_data/UID_ISO_FIPS_LookUp_Table.csv").to_dict("records")

def get_from_fips(x):
    for y in fips:
        if y["UID"] == x["uid"]:
            country_name = y["Country_Region"]
            if y["Country_Region"] != y["Combined_Key"]:
                country_name = y["Province_State"]
            return int(y["Population"]), float(y["Lat"]), float(y["Long_"]), country_name # y["Province_State"] if not pd.isna(y["FIPS"]) else y["Country_Region"]
    
    print("MISSING FIPS", x["name_en"])
    return int(0), 0.0, 0.0, ""

def get_from_wom(x):
    for y in wom:
        if y["0"] == x["country"] or y["0"] == x["name_en"] or y["0"] == x["country"]:
            return y["0"]
    
    print("MISSING WOM", x["name_en"])
    return ""
    
df[["population","lat", "long", "country"]] = df.apply(lambda x: get_from_fips(x), axis=1, result_type='expand')
df["wom_map"] = df.apply(lambda x: get_from_wom(x), axis=1, result_type='expand')

df.to_csv("/home/andefined/python/cvcio/covid-19-automation/data/countries-mapping.csv", index = False)
"""
jhu = pd.read_csv("/home/andefined/python/cvcio/covid-19-automation/data/2020-11-05-jhu.csv")
worldometer = pd.read_csv("/home/andefined/python/cvcio/covid-19-automation/data/2020-11-05-worldometer.csv")


print(len(jhu.country.unique()))
print(len(worldometer.country.unique()))

l1 = list(set(worldometer.country.unique()) - set(jhu.country.unique()))
l2 = list(set(jhu.country.unique()) - set(worldometer.country.unique()))

print(l1)
print(l2)
