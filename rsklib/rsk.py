# %% Processing routines for Ruskin RSK files
from __future__ import division, print_function

import sqlite3
import numpy as np
import xarray as xr
import pandas as pd

def init_connection(rskfile):
    conn = sqlite3.connect(rskfile)
    return conn.cursor()

def rsk_to_cdf(metadata):

    print("Loading from sqlite; this may take a while for large datasets")
    RAW, metadata = rsk_to_xr(metadata)

    print("Writing to raw netCDF")
    xr_to_cdf(RAW, metadata)

    print("Done")

    return RAW, metadata

def rsk_to_xr(metadata):
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
    jd = times.to_julian_date().values + 0.5
    time = np.floor(jd)
    time2 = (jd - time)*86400000

    dwave = {}
    dwave['cf_time'] = xr.DataArray(times, dims=('time'), name='cf_time', attrs={'standard_name': 'time', 'axis': 'T'})
    dwave['time2'] = xr.DataArray(time2.astype(int), dims=('time'), name='time2',
        attrs={'units': 'msec since 0:00 GMT', 'type': 'EVEN', 'epic_code': 624 })

    dwave['P_1'] = xr.DataArray(a['pres'], # coords=[times, samples],
        dims=('time', 'sample'), name='Pressure',
        attrs={'long_name': 'Pressure', '_FillValue': 1e35, 'units': 'dbar', 'epic_code': 1,
        'height_depth_units': 'm', 'initial_instrument_height': metadata['initial_instrument_height'],
        'serial_number': metadata['serial_number']})

    dwave['sample'] = xr.DataArray(samples, dims=('sample'), name='sample')
    dwave['lat'] = xr.DataArray([metadata['latitude']], dims=('lat'), name='lat',
        attrs={'units': 'degrees_north', 'long_name': 'Latitude', 'epic_code': 500})
    dwave['lon'] = xr.DataArray([metadata['longitude']], dims=('lon'), name='lon',
        attrs={'units': 'degrees_east', 'long_name': 'Longitude', 'epic_code': 502})
    dwave['depth'] = xr.DataArray([metadata['WATER_DEPTH']], dims=('depth'), name='depth',
        attrs={'units': 'm', 'long_name': 'mean water depth', 'epic_code': 3})

    # Assign min/max
    RAW = xr.Dataset(dwave)
    # need to assign this after it has become a Dataset, I think, to prevent errors. xarray wants to generate time by default, or something
    RAW['time'] = xr.DataArray(time.astype(int), dims=('time'), name='time',
        attrs={'units': 'True Julian Day', 'type': 'EVEN', 'epic_code': 624})

    return RAW, metadata

def xr_to_cdf(RAW, metadata):
    """Write xarray to CDF format"""

    cdf_filename = metadata['filename'] + '-raw.cdf'

    RAW.to_netcdf(cdf_filename)

def cdf_to_nc(metadata, offset=0):

    cdf_filename = metadata['filename'] + '-raw.cdf'

    print('printing ds')

    ds = xr.open_dataset(cdf_filename, autoclose=True)

    # print('Saving attrs')
    # attrs = {}
    # for k in ds:
    #     attrs[k] = ds[k].attrs

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

    print("Subtracting atmospheric offset")
    # need to save attrs
    attrs = ds['P_1'].attrs
    ds['P_1'] = ds['P_1'] - offset
    ds['P_1'].attrs = attrs
    ds['P_1'].assign_attrs(minimum=ds['P_1'].min().values, maximum=ds['P_1'].max().values)

    print("Writing metadata to Dataset")
    ds = write_metadata(ds, metadata)

    nc_filename = metadata['filename'] + '.nc'
    print("Writing to " + nc_filename)
    ds.to_netcdf(nc_filename)

    return ds

def write_metadata(ds, metadata):

    for k in metadata:
        ds.attrs.update({k: metadata[k]})

    return ds

def compute_time(RAW):
    """Compute Julian date and then time and time2 for use in NetCDF file"""

    shape = np.shape(RAW['datetime'])

    RAW['jd'] = pd.to_datetime(np.ravel(RAW['datetime'])).to_julian_date().values + 0.5
    RAW['jd'] = np.reshape(RAW['jd'], shape)

    RAW['time'] = np.floor(RAW['jd'])
    # TODO: Hopefully this is correct... roundoff errors on big numbers...
    RAW['time2'] = (RAW['jd'] - np.floor(RAW['jd']))*86400000

    return RAW





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
