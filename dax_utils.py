from __future__ import print_function, division, absolute_import
import json
import requests
import pandas as pd


def imgserv_json_to_df(json_input):
    with open(json_input) as json_file:
        json_data = json.load(json_file)

    df = pd.DataFrame.from_dict(json_data['result']['table']['data'])
    df.columns = [e['name'] 
        for e in json_data['result']['table']['metadata']['elements']]
    return(df)


def query_tap_json(tap_endpoint, adql_query):
    """
    Query a TAP service (designated by its tap_endpoint)
    with a given ADQL query

    Query is performed synchronously

    Return a Pandas dataframe
    """
    r = requests.post(tap_endpoint + '/sync', data={'query': adql_query})
                                                   #'format': 'votable'})

    mydict = json.loads(r.content)
    if 'result' not in mydict:
        print(r.content)
        return(None)
    df = pd.DataFrame.from_dict(mydict['result']['table']['data'])
    df.columns = [e['name'] for e in mydict['result']['table']['metadata']['elements']]
    df.apply(lambda x: pd.to_numeric(x, errors='ignore'))

    return df


def get_image_table(ra, dec, filter_name, table_name='Science_Ccd_Exposure'):
    if filter_name is not None:
        df = query_tap_json('http://lsst-qserv-dax01:5000/db/v0/tap',
                """select * from {} where 
                   filterName = '{}' and
                   scisql_s2PtInCPoly({}, {}, 
                   corner1Ra, corner1Decl, corner2Ra, corner2Decl, 
                   corner3Ra, corner3Decl, corner4Ra, corner4Decl)=1
                """.format(table_name, filter_name, ra, dec))
    else:
        df = query_tap_json('http://lsst-qserv-dax01:5000/db/v0/tap',
                """select * from {} where 
                   scisql_s2PtInCPoly({}, {}, 
                   corner1Ra, corner1Decl, corner2Ra, corner2Decl, 
                   corner3Ra, corner3Decl, corner4Ra, corner4Decl)=1
                """.format(table_name, ra, dec))
    return(df)


