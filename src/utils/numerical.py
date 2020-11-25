import numpy as np

def parse_float(s):
    if s is None or len(s) == 0 or s == "N/A":
        return np.NaN
    s = s.replace(",", "")
    return float(s)


def parse_int(s):
    if s is None or len(s) == 0 or s == "N/A":
        return np.NaN
    s = s.replace(",", "")
    return int(s)


# Case-Fatality Ratio (%): Case-Fatality Ratio (%) = Number recorded deaths / Number cases.
def calc_fatality_ratio(x):
    return 0 if x["cases"] == 0 else round(float((x["deaths"] / x["cases"]) * 100), 4)


# Incidence_Rate: Incidence Rate = cases per 100,000 persons.
def calc_incidence_rate(x):
    return (
        0
        if x["population"] == 0
        else round(float((x["cases"] * 100000) / x["population"]), 4)
    )
