import re

def name_cleaner(name):
    pat = r"S/O|S O|SO|SON OF|S-O|W/O|WO|W O|DO|D/O|D O|D-O|MR|MRS|KYC|REMOVEDDATA|NOT-AVAIL|UPDATE|NULL"
    name = name.astype(str).str.replace(pat, "", flags=re.IGNORECASE)
    name = name.astype(str).str.replace(r"'", " ").str.strip()
    name = name.astype(str).str.extract(r"([A-Za-z ]+)")
    return name
