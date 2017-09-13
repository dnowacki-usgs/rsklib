# %% Processing routines for Ruskin RSK files
from __future__ import division, print_function

import sqlite3
import numpy as np
import xarray as xr
import pandas as pd



def rsk_to_cdf(metadata):
    """
    Main function to load data from RSK file and save to raw .CDF
    """

    print("Loading from sqlite; this may take a while for large datasets")
    RAW, metadata = rsk_to_xr(metadata)

    print("Writing to raw netCDF")
    xr_to_cdf(RAW, metadata)

    print("Done")

    return RAW, metadata

def init_connection(rskfile):
    """Initialize an sqlite3 connection and return a cursor"""

    conn = sqlite3.connect(rskfile)
    return conn.cursor()

def rsk_to_xr(metadata):
    """
    Load data from RSK file and generate an xarray Dataset
    """

    rskfile = metadata['basefile'] + '.rsk'
    c = init_connection(rskfile)

    c.execute("SELECT tstamp, channel01 FROM burstdata")
    data = c.fetchall()
    print("Done fetching data")
    d = np.asarray(data)
    # Get samples per burst
    samplingcount = c.execute("select samplingcount from schedules").fetchall()[0][0]

    metadata['samples_per_burst'] = samplingcount
    samplingperiod = c.execute("select samplingperiod from schedules").fetchall()[0][0]
    metadata['sample_interval'] = samplingperiod / 1000
    repetitionperiod = c.execute("select repetitionperiod from schedules").fetchall()[0][0]
    metadata['burst_interval'] = repetitionperiod / 1000
    metadata['burst_length'] = metadata['samples_per_burst'] * metadata['sample_interval']
    metadata['serial_number'] = c.execute("select serialID from instruments").fetchall()[0][0]

    a = {}
    a['unixtime'] = d[:,0].copy()
    a['pres'] = d[:,1].copy()
    # sort by time (not sorted for some reason)
    sort = np.argsort(a['unixtime'])
    a['unixtime'] = a['unixtime'][sort]
    a['pres'] = a['pres'][sort]

    # get indices that end at the end of the final burst
    datlength = a['unixtime'].shape[0] - a['unixtime'].shape[0] % samplingcount

    # reshape
    for k in a:
        a[k] = a[k][:datlength].reshape((int(datlength/samplingcount), samplingcount))

    times = pd.to_datetime(a['unixtime'][:,0], unit='ms')
    samples = np.arange(samplingcount)
    # jd = times.to_julian_date().values + 0.5
    # time = np.floor(jd)
    # time2 = (jd - time)*86400000

    dwave = {}

    dwave['P_1'] = xr.DataArray(a['pres'], coords=[times, samples],
        dims=('time', 'sample'), name='Pressure',
        attrs={'long_name': 'Pressure', '_FillValue': 1e35, 'units': 'dbar', 'epic_code': 1,
        'height_depth_units': 'm', 'initial_instrument_height': metadata['initial_instrument_height'],
        'serial_number': metadata['serial_number']})

    dwave['time'] = xr.DataArray(times, dims=('time'), name='time')
    # dwave['time2'] = xr.DataArray(time2.astype(int), dims=('time'), name='time2',
    #     attrs={'units': 'msec since 0:00 GMT', 'type': 'EVEN', 'epic_code': 624 })

    dwave['sample'] = xr.DataArray(samples, dims=('sample'), name='sample')
    dwave['lat'] = xr.DataArray([metadata['latitude']], dims=('lat'), name='lat',
        attrs={'units': 'degrees_north', 'long_name': 'Latitude', 'epic_code': 500})
    dwave['lon'] = xr.DataArray([metadata['longitude']], dims=('lon'), name='lon',
        attrs={'units': 'degrees_east', 'long_name': 'Longitude', 'epic_code': 502})
    dwave['depth'] = xr.DataArray([metadata['WATER_DEPTH']], dims=('depth'), name='depth',
        attrs={'units': 'm', 'long_name': 'mean water depth', 'epic_code': 3})

    # Create Dataset from dictionary of DataArrays
    RAW = xr.Dataset(dwave)

    # need to add the time attrs after DataArrays have been combined into Dataset
    RAW['time'].attrs.update({'standard_name': 'time', 'axis': 'T'})

    # need to assign this after it has become a Dataset, I think, to prevent errors. xarray wants to generate time by default, or something
    # RAW['time'] = xr.DataArray(time.astype(int), dims=('time'), name='time',
    #     attrs={'units': 'True Julian Day', 'type': 'EVEN', 'epic_code': 624})

    RAW = write_metadata(RAW, metadata)

    return RAW, metadata

def xr_to_cdf(RAW, metadata):
    """Write raw xarray Dataset to .cdf"""

    cdf_filename = metadata['filename'] + '-raw.cdf'

    RAW.to_netcdf(cdf_filename)

def cdf_to_nc(metadata, atmpres=None, offset=0):
    """
    Load raw .cdf file, trim, apply QAQC, and save to .nc
    """

    cdf_filename = metadata['filename'] + '-raw.cdf'

    print('printing ds')

    ds = xr.open_dataset(cdf_filename, autoclose=True)

    print(xr.decode_cf(ds))

    # trim data via one of two methods
    if 'good_ens' in metadata:
        # we have good ensemble indices in the metadata
        print('Using good_ens')
        S = metadata['good_ens'][0]
        E = metadata['good_ens'][1]

        print('first burst in full file:', ds['time'].min().values)
        print('last burst in full file:', ds['time'].max().values)

        ds = ds.isel(time=slice(S,E))

        print('first burst in trimmed file:', ds['time'].min().values)
        print('last burst in trimmed file:', ds['time'].max().values)

    else:
        # we clip by the times in/out of water as specified in the metadata
        print('Using Deployment_date and Recovery_date')

        print('first burst in full file:', ds['time'].min().values)
        print('last burst in full file:', ds['time'].max().values)

        ds = ds.sel(time=slice(metadata['Deployment_date'], metadata['Recovery_date']))

        print('first burst in trimmed file:', ds['time'].min().values)
        print('last burst in trimmed file:', ds['time'].max().values)



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

    print("Writing metadata to Dataset")
    ds = write_metadata(ds, metadata)

    # Write to .nc file
    print("Writing cleaned/trimmed data to .nc file")
    write_nc(ds, metadata)

    return ds

def write_nc(ds, metadata):
    """Write cleaned and trimmed Dataset to .nc file"""

    nc_filename = metadata['filename'] + '.nc'

    ds.to_netcdf(nc_filename)

def write_metadata(ds, metadata):
    """Write metadata to Dataset"""

    for k in metadata:
        ds.attrs.update({k: metadata[k]})

    return ds

        # # TODO: add the following??
        # # {'positive','down';
        # #                'long_name', 'Depth';
        # #                'axis','z';
        # #                'units', 'm';
        # #                'epic_code', 3};
        #
        # Pressid = rg.createVariable('Pressure', 'f', ('time','sample',), fill_value=False, zlib=True)
        # Pressid.units = 'dbar'
        # Pressid.long_name = 'Pressure (dbar)'
        # Pressid.generic_name = 'press'
        # Pressid.note = 'raw pressure from instrument, not corrected for changes in atmospheric pressure'
        # Pressid.epic_code = 1
        # Pressid.height_depth_units = 'm'
