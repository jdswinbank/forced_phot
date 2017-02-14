from __future__ import print_function, division, absolute_import
import os
import functools32
import tempfile
from lsst.daf.persistence import Butler
import lsst.afw.table as afwTable
from lsst.afw.geom import Angle, degrees
import numpy as np
from astropy.table import Table
import astropy.units as u
from forcedPhotExternalCatalog import ForcedPhotExternalCatalogTask


if os.path.exists('/home/shupe/work/forcephot/output'):
    out_butler = Butler('/home/shupe/work/forcephot/output')
elif os.path.exists('/hydra/workarea/forcephot/output'):
    out_butler = Butler('/hydra/workarea/forcephot/output')


def make_refcat(ra, dec):
    schema = out_butler.get('src_schema', immediate=True).schema
    mapper = afwTable.SchemaMapper(schema)
    mapper.addMinimalSchema(schema)
    newSchema = mapper.getOutputSchema()
    src_cat = afwTable.SourceCatalog(newSchema)
    for row in zip(ra,dec):
        record = src_cat.addNew()
        record.set('coord_ra', Angle(row[0]*degrees))
        record.set('coord_dec', Angle(row[1]*degrees))
    return(src_cat)


def conv_afwtable_astropy(afwtable):
    with tempfile.NamedTemporaryFile() as tf:
        afwtable.writeFits(tf.name)
        tf.flush()
        tf.seek(0)
        atab = Table.read(tf.name, hdu=1)
    return(atab)


def parse_phot_table(afwTable, convert=True):
    if convert:
        tab = conv_afwtable_astropy(afwTable)
    else:
        tab = afwTable
    tab['run'] = tab.meta['RUN']
    tab['camcol'] = tab.meta['CAMCOL']
    tab['field'] = tab.meta['FIELD']
    tab['filterName'] = tab.meta['FILTER']
    tab['psfMag'] = -2.5*np.log10(tab['base_PsfFlux_flux']/tab.meta['FLUXM0'])
    tab['psfMagErr'] = -2.5*np.log10(1.- + (tab['base_PsfFlux_fluxSigma']
                                     /tab['base_PsfFlux_flux']))
    tab['psfMag'].unit = u.mag
    tab['psfMagErr'].unit = u.mag
    del tab.meta['RUN']
    del tab.meta['CAMCOL']
    del tab.meta['FIELD']
    del tab.meta['FILTER']
    del tab.meta['FLUXM0']
    del tab.meta['FLUXM0SG']
    return(tab)


def do_phot(dataId, refCat):
    """ Perform forced photometry on dataId from repo_str at positions in refCat
    """
    in_butler = get_in_butler()
    exposure = in_butler.get('calexp', dataId=dataId)
    expWcs = exposure.getWcs()

    ftask = ForcedPhotExternalCatalogTask(out_butler)
    measCat = ftask.measurement.generateMeasCat(exposure, refCat, expWcs)

    ftask.measurement.attachTransformedFootprints(measCat, refCat, exposure, expWcs)
    ftask.measurement.run(measCat, exposure, refCat, expWcs)

    # Get magnitude information so it can be added to catalog metadata
    calib = exposure.getCalib()
    fluxMag0, fluxMag0Err = calib.getFluxMag0()

    meta = measCat.getTable().getMetadata()
    for (key, val) in dataId.iteritems():
        meta.add(key.upper(), val)
    meta.add('FLUXM0', fluxMag0)
    meta.add('FLUXM0SG', fluxMag0Err)
    measCat.getTable().setMetadata(meta)
    return(measCat)


@functools32.lru_cache()
def get_in_butler(repo_str='/datasets/gapon/data/DC_2013/calexps'):
    return(Butler(repo_str))

