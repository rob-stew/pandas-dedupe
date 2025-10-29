from unidecode import unidecode
import pandas as pd
import numpy as np
from ast import literal_eval

from dedupe import variables as V
try:
    from dedupe_variable_name import Name as VName
except Exception:
    VName = None
try:
    from dedupe_variable_address import Address as VAddress
except Exception:
    VAddress = None
try:
    from dedupe_variable_datetime import DateTime as VDateTime
except Exception:
    VDateTime = None
try:
    from dedupe_variable_number import Number as VNumber
except Exception:
    VNumber = None

_TYPE_MAP = {
    "String": V.String,
    "ShortString": V.ShortString,
    "Text": V.Text,
    "Exact": V.Exact,
    "Set": V.Set,
    "LatLong": V.LatLong,
    "Price": V.Price,
    "Categorical": V.Categorical,   # requires categories=[...]
    "Exists": V.Exists,             # requires field
    # plugin variables (optional installs)
    "Name": VName,
    "Address": VAddress,
    "DateTime": VDateTime,
    "Number": VNumber,
}


def trim(x):
    x = x.split()
    x = ' '.join(x)
    return x   


def clean_punctuation(df):
    for i in df.columns:
        df[i] = df[i].astype(str) 
    df = df.applymap(lambda x: x.lower())
    for i in df.columns:
        df[i] = df[i].str.replace('[^\w\s\.\-\(\)\,\:\/\\\\]','')
    df = df.applymap(lambda x: trim(x))
    df = df.applymap(lambda x: unidecode(x))
    for i in df.columns:
        df[i] = df[i].replace({'nan': None, 'none': None, 'nat': None})
    return df

def select_fields(fields, field_properties):
    """
    Legacy adapter:
      - 'col'                       -> String(col)
      - (col, Type)                 -> Type(col)
      - (col, Type, 'has missing')  -> Type(col, has_missing=True)
      - (col, 'String', 'crf')      -> String(col, crf=True)
    """
    vars_ = []
    for item in field_properties:
        # simplest form: just a column name -> String
        if isinstance(item, str):
            vars_.append(V.String(item))
            continue

        if len(item) == 2:
            col, typ = item
            cls = _TYPE_MAP.get(typ)
            if cls is None:
                raise ValueError(f"Unknown variable type: {typ}. "
                                 "Install plugin packages for Name/Address/DateTime/Number if needed.")
            if typ == "Categorical":
                raise ValueError("Categorical requires categories=[...] in 3.x")
            if typ == "Exists":
                vars_.append(cls(col))  # Exists needs a field in 3.x
            else:
                vars_.append(cls(col))
            continue

        if len(item) == 3:
            col, typ, flag = item
            cls = _TYPE_MAP.get(typ)
            if cls is None:
                raise ValueError(f"Unknown variable type: {typ}")
            if flag == "has missing":
                vars_.append(cls(col, has_missing=True))
            elif flag == "crf" and typ in ("String", "ShortString"):
                vars_.append(cls(col, crf=True))
            else:
                raise ValueError(f"Unsupported field property: {flag}")
            continue

        raise ValueError(f"Unrecognized field spec: {item}")

    # mutate the list in-place to preserve your call sites
    fields[:] = vars_
                
    
def latlong_datatype(x):
    if x is None:
        return None
    else:
        try:
            x = literal_eval(x)
            k,v = x
            k = float(k)
            v = float(v)
            return k, v
        except:
            raise Exception("Make sure that LatLong columns are tuples arranged like ('lat', 'lon')")
            
            
def specify_type(df, field_properties):
    for i in field_properties:
        if i[1] == 'LatLong':
            df[i[0]] = df[i[0]].apply(lambda x: latlong_datatype(x))
        elif i[1] == 'Price':
            try:
                df[i[0]] = df[i[0]].str.replace(",","")
                df[i[0]] = df[i[0]].replace({None: np.nan})
                df[i[0]] = df[i[0]].astype(float)
                df[i[0]] = df[i[0]].replace({np.nan: None})
            except:
                raise Exception('Make sure that Price columns can be converted to float.')
 
