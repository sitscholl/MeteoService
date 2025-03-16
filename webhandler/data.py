import pandas as pd
import numpy as np
import re


def open_sbr_export(path):

    tbl = pd.read_csv(path, sep = ';', decimal=',').dropna(how = 'all', axis = 1)
    tbl['datetime'] = pd.to_datetime(tbl["wet_data"] + " " + tbl["wet_ora"], format = '%Y-%m-%d %S:%H:%M')

    tbl.rename(columns = lambda x: re.sub('^wet_', '', x), inplace = True)
    tbl.drop(['data', 'ora', 'status', 't_2m_min', 't_2m_max', 'luftfeucht_min', 'luftfeucht_max', 'v_wind_max'], axis = 1, inplace = True)
    tbl = tbl[['datetime', 'wst_codice', *[i for i in np.sort(tbl.columns) if i not in  ['datetime', 'wst_codice']]]]

    scale_cols = ['niederschl', *[col for col in tbl if any([col.startswith(i) for i in ['bt_', 't_', 'tf_', 'tt_']])]]
    tbl[scale_cols] = tbl[scale_cols] / 10

    return(tbl)
