#!/usr/bin/env python

from __future__ import division, print_function

import xarray as xr
import sys
sys.path.append('/Users/dnowacki/Documents/python')
import matlabtools

def nc_to_diwasp(metadata, atmpres=None):

    ds = xr.open_dataset(metadata['filename'] + 'b-cal.nc', autoclose=True)

    mat = matlabtools.loadmat(metadata['filename'] + 's-a.mat')

    mat = mat['dw']

    ds = ds.drop(['P_1', 'P_1ac'])

    for k in ['wp_peak', 'wh_4061', 'wp_4060']:
        ds[k] = xr.DataArray(mat[k], dims='time')

    ds['frequency'] = xr.DataArray(mat['frequency'], dims=('frequency'))

    ds['pspec'] = xr.DataArray(mat['pspec'].T, dims=('time', 'frequency'))

    ds = trim_min_wp(ds, metadata)

    ds = trim_min_wh(ds, metadata)

    ds = trim_wp_ratio(ds, metadata)

    # Add attrs
    ds = ds_add_attrs(ds, metadata)

    ds = rsklib.write_metadata(ds, metadata)

    # Write to netCDF
    ds.to_netcdf(metadata['filename'] + 's-a.nc', unlimited_dims='time')

    return ds


def trim_min_wp(ds, metadata):
    """
    QA/QC
    Trim wave data based on maximum wave period as specified in metadata
    """

    if 'maximum_wp' in metadata:
        print('Trimming using maximum period of %f seconds'
            % metadata['maximum_wp'])
        ds = ds.where((ds['wp_peak'] < metadata['maximum_wp']) &
            (ds['wp_4060'] < metadata['maximum_wp']))

        for var in ['wp_peak', 'wp_4060', 'wh_4061', 'pspec']:
            notetxt = 'Values filled where wp_peak, wp_4060 >= %f' % metadata['maximum_wp'] + '. '

            if 'note' in ds[var].attrs:
                ds[var].attrs['note'] = notetxt + ds[var].attrs['note']
            else:
                ds[var].attrs.update({'note': notetxt})

    return ds


def trim_min_wh(ds, metadata):
    """
    QA/QC
    Trim wave data based on minimum wave height as specified in metadata
    """

    if 'minimum_wh' in metadata:
        print('Trimming using minimum wave height of %f m'
            % metadata['minimum_wh'])
        ds = ds.where(ds['wh_4061'] > metadata['minimum_wh'])
        print('done trimming waves')

        for var in ['wp_peak', 'wp_4060', 'wh_4061', 'pspec']:
            notetxt = 'Values filled where wh_4061 <= %f' % metadata['minimum_wh'] + '. '

            if 'note' in ds[var].attrs:
                ds[var].attrs['note'] = notetxt + ds[var].attrs['note']
            else:
                ds[var].attrs.update({'note': notetxt})

    return ds


def trim_wp_ratio(ds, metadata):
    """
    QA/QC
    Trim wave data based on maximum ratio of wp_peak to wp_4060
    """

    if 'wp_ratio' in metadata:
        print('Trimming using maximum ratio of wp_peak to wp_4060 of %f'
            % metadata['wp_ratio'])
        ds = ds.where(ds['wp_peak']/ds['wp_4060'] < metadata['wp_ratio'])

        for var in ['wp_peak', 'wp_4060', 'wh_4061', 'pspec']:
            notetxt = 'Values filled where wp_peak:wp_4060 >= %f' % metadata['wp_ratio'] + '. '

            if 'note' in ds[var].attrs:
                ds[var].attrs['note'] = notetxt + ds[var].attrs['note']
            else:
                ds[var].attrs.update({'note': notetxt})

    return ds


def ds_add_attrs(ds, metadata):
    """
    Add EPIC and other attributes to variables
    """

    def add_attributes(var, metadata, INFO):
            var.attrs.update({'serial_number': INFO['serial_number'],
                'initial_instrument_height': metadata['initial_instrument_height'],
                # 'nominal_instrument_depth': metadata['nominal_instrument_depth'], # FIXME
                'height_depth_units': 'm',
                'sensor_type': INFO['INST_TYPE'],
                '_FillValue': 1e35})

    ds['wp_peak'].attrs.update({'long_name': 'Dominant (peak) wave period',
        'units': 's',
        'epic_code': 4063})

    ds['wp_4060'].attrs.update({'long_name': 'Average wave period',
        'units': 's',
        'epic_code': 4060})

    ds['wh_4061'].attrs.update({'long_name': 'Significant wave height',
        'units': 'm',
        'epic_code': 4061})

    ds['pspec'].attrs.update({'long_name': 'Pressure derived non-directional wave energy spectrum',
        'units': 'm^2/Hz',
        'note': 'Use caution: all spectra are provisional'})

    ds['frequency'].attrs.update({'long_name': 'Frequency',
        'units': 'Hz'})

    for var in ['wp_peak', 'wh_4061', 'wp_4060', 'pspec']:
        add_attributes(ds[var], metadata, ds.attrs)
        ds[var].attrs.update({'minimum': ds[var].min().values, 'maximum': ds[var].max().values})

    return ds

def main():
    import sys
    sys.path.insert(0, '/Users/dnowacki/Documents/rsklib')
    import rsklib
    import argparse
    import yaml

    parser = argparse.ArgumentParser(description='Convert processed .nc files using DIWASP')
    parser.add_argument('gatts', help='path to global attributes file (gatts formatted)')
    parser.add_argument('config', help='path to ancillary config file (YAML formatted)')

    args = parser.parse_args()

    # initialize metadata from the globalatts file
    metadata = rsklib.read_globalatts(args.gatts)

    # Add additional metadata from metadata config file
    config = yaml.safe_load(open(args.config))

    for k in config:
        metadata[k] = config[k]

    ds = rsklib.nc_to_diwasp(metadata)

if __name__ == '__main__':
    main()
