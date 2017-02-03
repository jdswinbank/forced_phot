#!/usr/bin/env python

from __future__ import print_function, division, absolute_import

import os
import json
import tempfile
import shutil
import pandas as pd
import numpy as np
from astropy.table import Table, Column, vstack, join
import astropy.units as u
from astropy.time import Time
import glob

def imgserv_json_to_df(json_input):
    with open(json_input) as json_file:
        json_data = json.load(json_file)

    df = pd.DataFrame.from_dict(json_data['result']['table']['data'])
    df.columns = [e['name'] 
        for e in json_data['result']['table']['metadata']['elements']]
    return(df)

def parse_phot_table(table_path):
    tab = Table.read(table_path)
    tab['run'] = tab.meta['RUN']
    tab['camcol'] = tab.meta['CAMCOL']
    tab['field'] = tab.meta['FIELD']
    tab['filterName'] = tab.meta['FILTER']
    tab['psfMag'] = -2.5*np.log10(tab['base_PsfFlux_flux']/tab.meta['FLUXM0'])
    tab['psfMagErr'] = -2.5*np.log10(tab['base_PsfFlux_fluxSigma']
                                     /tab.meta['FLUXM0SG'])
    tab['psfMag'].unit = u.mag
    tab['psfMagErr'].unit = u.mag
    del tab.meta['RUN']
    del tab.meta['CAMCOL']
    del tab.meta['FIELD']
    del tab.meta['FILTER']
    del tab.meta['FLUXM0']
    del tab.meta['FLUXM0SG']
    return(tab)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="""
            Run forced photometry on specified images and output a time-series table
        """)
    parser.add_argument('input_repo', help='input repository directory path for images')
    parser.add_argument('output_repo', help='dummy repository directory for schema')
    parser.add_argument('json_input', help='json file as returned by imgserv')
    parser.add_argument('coord_str', help='coordinates string as name,ra,dec')
    parser.add_argument('filter_name', help='name of filter to subset input table [ugriz]')
    parser.add_argument('output_table', help='output table path in FITS format')

    args = parser.parse_args()
    input_repo = args.input_repo
    output_repo = args.output_repo
    json_input = args.json_input
    coord_str = args.coord_str
    filter_name = args.filter_name
    output_table = args.output_table

    # Parse the json into Pandas and subset by filter
    df = imgserv_json_to_df(json_input)
    df_f = df[df.filterName == unicode(filter_name)]

    # Form string of images to work on
    idstr = (' '.join(['--id run={} camcol={} filter={} field={}'
        .format(row['run'],row['camcol'],row['filterName'],row['field']) 
        for index, row in df_f.iterrows()]))

    # Make a temporary directory to work in
    dirpath = tempfile.mkdtemp()

    # Form the coordinates file
    coords_path = os.path.join(dirpath, 'coords.csv')
    with open(coords_path, 'w') as cfh:
        cfh.write('Name,RA,Dec\n')
        cfh.write('%s\n'%coord_str)

    # Run the forced photometry
    exec_str = (('python forcedPhotExternalCatalog.py {} ' +
            '--coord_file {} --dataset calexp --output {} --out_root {} ').format(
            input_repo, coords_path, output_repo, os.path.join(dirpath, 'photometry')) + idstr)
    os.system(exec_str)

    # Concatenate the input tables
    tnames = glob.glob(os.path.join(dirpath, 'photometry_*.fits'))
    tbl_list = [parse_phot_table(name) for name in tnames]
    alltabs = vstack(tbl_list)

    # merge
    intab = Table.from_pandas(df_f)
    outtab = join(alltabs, intab, keys=['run','camcol','field','filterName'],
                  join_type='left')
    t = Time(outtab['expMidpt'], format='isot', scale='utc')
    outtab['mjd'] = t.mjd
    outtab.sort('mjd')
    outtab['ra'] = outtab['coord_ra'].to('deg')
    outtab['dec'] = outtab['coord_dec'].to('deg')
    mycols = ['mjd','psfMag','psfMagErr','ra','dec',
           'expMidpt','run','field','camcol','filterName',
           'objectId','base_RaDecCentroid_x',
           'base_RaDecCentroid_y','base_PsfFlux_flux','base_PsfFlux_fluxSigma']
    outtab.keep_columns(mycols)
    newtab = outtab[mycols]

    # Write output
    #outtab.write(output_table, format='fits', overwrite=True)
    newtab.write(output_table, format='ipac')

    # Clean up
    shutil.rmtree(dirpath)

if __name__ == '__main__':
    main()


