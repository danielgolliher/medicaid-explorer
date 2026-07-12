"""Classify every HCPCS/CPT code in the dataset into a service sector.

Sectors are inferred from official code families (HCPCS Level II letter
prefixes, CPT numeric ranges, CDT D-codes) — an approximation, labeled as
such in the UI. Output: data/sectors.json + docs/data/sectors.json
mapping code -> sector name (unmapped codes are omitted; the UI buckets
them as "Other / unclassified").
"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))

HOSPICE_T = {f"T20{n}" for n in range(42, 47)}                 # T2042-T2046
HOSPICE_Q = {f"Q50{n:02d}" for n in range(3, 11)}              # Q5003-Q5010
HOME_T = {"T1019", "T1020", "T1021", "T1022", "T1000", "T1001",
          "T1002", "T1003", "T1004", "T1005", "T1030", "T1031"}
NEMT_T = {"T2001", "T2002", "T2003", "T2004", "T2005", "T2049"}
HOME_G = {f"G0{n}" for n in range(151, 163)} | {"G0299", "G0300"}


def classify(code):
    c = code.strip().upper()
    if re.fullmatch(r"D\d{4}", c):
        return "Dental"
    if c in HOSPICE_T or c in HOSPICE_Q:
        return "Hospice"
    if c in HOME_T or c in HOME_G:
        return "Home health & personal care"
    if c in NEMT_T or re.fullmatch(r"A0\d{3}", c) or c == "S0215":
        return "Transportation (NEMT & ambulance)"
    if re.fullmatch(r"A[4-9]\d{3}", c):
        return "Medical supplies"
    if re.fullmatch(r"[EKL]\d{4}", c):
        return "DME, prosthetics & orthotics"
    if re.fullmatch(r"J\d{4}", c):
        return "Drugs (provider-administered)"
    if re.fullmatch(r"H\d{4}", c):
        return "Behavioral health & community services"
    if re.fullmatch(r"V\d{4}", c):
        return "Vision & hearing"
    if re.fullmatch(r"S5\d{3}", c) or re.fullmatch(r"S91\d{2}", c):
        return "Home health & personal care"
    if re.fullmatch(r"[ST]\d{4}", c):
        return "State plan & waiver services"
    if re.fullmatch(r"G\d{4}", c):
        return "Physician & clinical services"
    if re.fullmatch(r"\d{5}", c):
        n = int(c)
        if 100 <= n <= 1999:
            return "Physician & clinical services"      # anesthesia
        if 10004 <= n <= 69990:
            return "Physician & clinical services"      # surgery/procedures
        if 70010 <= n <= 89398:
            return "Lab & imaging"
        if 90785 <= n <= 90899:
            return "Behavioral health & community services"
        if 90281 <= n <= 99499:
            return "Physician & clinical services"
        if 99500 <= n <= 99602:
            return "Home health & personal care"
        if 99605 <= n <= 99607:
            return "Physician & clinical services"
    return None


if __name__ == "__main__":
    with open(os.path.join(HERE, "data", "stats.json")) as f:
        codes = json.load(f).get("codes")
    if not codes:
        import duckdb
        codes = [r[0] for r in duckdb.connect().execute(
            "SELECT DISTINCT hcpcs FROM 'data/spending.parquet' "
            "WHERE hcpcs IS NOT NULL").fetchall()]
    mapping = {}
    for c in codes:
        s = classify(c)
        if s:
            mapping[c] = s
    for out in ("data/sectors.json", os.path.join("docs", "data", "sectors.json")):
        with open(os.path.join(HERE, out), "w") as f:
            json.dump(mapping, f, separators=(",", ":"))
    from collections import Counter
    counts = Counter(mapping.values())
    print(f"classified {len(mapping)}/{len(codes)} codes")
    for s, n in counts.most_common():
        print(f"  {n:5d}  {s}")
