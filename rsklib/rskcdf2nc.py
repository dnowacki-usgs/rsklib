#!/usr/bin/env python

from __future__ import division, print_function

import xarray as xr
import sys
sys.path.insert(0, '/Users/dnowacki/Documents/rsklib')
import rsklib
sys.path.insert(0, '/Users/dnowacki/Documents/aqdlib')
import aqdlib

def cdf_to_nc(metadata, atmpres=None, offset=0):
    """
    Load raw .cdf file, trim, apply QAQC, and save to .nc
    """

    cdf_filename = metadata['filename'] + '-raw.cdf'

    ds = xr.open_dataset(cdf_filename, autoclose=True)

    # trim data via one of two methods
    ds = aqdlib.clip_ds(ds, metadata)

    if atmpres is not None:
        print("Atmospherically correcting data")

        met = xr.open_dataset(atmpres, autoclose=True)
        # need to save attrs before the subtraction, otherwise they are lost
        # ds['P_1ac'] = ds['P_1'].copy(deep=True)
        attrs = ds['P_1'].attrs
        ds['P_1ac'] = ds['P_1'] - met['atmpres'] - met['atmpres'].offset
        print('Correcting using offset of %f' % met['atmpres'].offset)
        ds['P_1ac'].attrs = attrs

    # assign min/max:
    for k in ['P_1', 'P_1ac']:
        if k in ds:
            ds[k].attrs.update(minimum=ds[k].min().values, maximum=ds[k].max().values)

            # TODO: published dwave data are not in time, lon, lat, sample format...
            # shouldn't they be?
            # reshape and add lon and lat dimensions

    print("Writing metadata to Dataset")
    ds = rsklib.write_metadata(ds, metadata)

    print(ds)

    # Write to .nc file
    print("Writing cleaned/trimmed data to .nc file")
    write_nc(ds, metadata)

    return ds

def write_nc(ds, metadata):
    """Write cleaned and trimmed Dataset to .nc file"""

    nc_filename = metadata['filename'] + 'b-cal.nc'

    ds.to_netcdf(nc_filename)

def main():
    import sys
    sys.path.insert(0, '/Users/dnowacki/Documents/rsklib')
    import rsklib
    import argparse
    import yaml

    parser = argparse.ArgumentParser(description='Convert raw RBR d|wave .cdf format to processed .nc files')
    parser.add_argument('cdfname', help='raw .CDF filename')
    parser.add_argument('gatts', help='path to global attributes file (gatts formatted)')
    parser.add_argument('config', help='path to ancillary config file (YAML formatted)')
    parser.add_argument('--atmpres', help='path to cdf file containing atmopsheric pressure data')

    args = parser.parse_args()

    # initialize metadata from the globalatts file
    metadata = rsklib.read_globalatts(args.gatts)

    # Add additional metadata from metadata config file
    config = yaml.safe_load(open(args.config))

    for k in config:
        metadata[k] = config[k]

    if args.atmpres:
        # press_ac = aqdlib.load_press_ac('press_ac.cdf', ['p_1ac'])
        ds = rsklib.cdf_to_nc(metadata, atmpres=args.atmpres)
    else:
        ds = rsklib.cdf_to_nc(metadata)

if __name__ == '__main__':
    main()
