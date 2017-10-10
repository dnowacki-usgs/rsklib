# rsklib - Process RBR d|wave data in Python

This module contains code to process RBR, Ltd. d|wave data as downloaded from the instrument in binary .rsk format, consistent with the procedures of the Sediment Transport Group at the USGS Woods Hole Coastal and Marine Science Center.

Processing consists of three main steps:

1. Convert from .rsk binary to a raw netCDF file with `.cdf` extension
2. Convert the raw `.cdf` data into an EPIC-compliant netCDF file with `.nc` extension, optionally including atmospheric correction of the pressure data
3. Run DIWASP (within MATLAB) to produce wave statistics, and incorporate these statistics into an EPIC-compliant netCDF file with `.nc` extension

## Raw binary to raw netCDF (.cdf)

This step will generally be completed by using the import statement `from aqdlib import rskrsk2cdf` and calling `rskrsk2cdf.hdr_to_cdf()`, or by running `rskrsk2cdf.py` from the command line.

## Raw netCDF (.cdf) to EPIC-compliant and processed netCDF (.nc)

This step will generally be completed by using the import statement `from aqdlib import rskcdf2nc` and calling `rskcdf2nc.cdf_to_nc()`, or by running `rskcdf2nc.py` from the command line. When calling `cdf_to_nc()`, the user may provide the path to a netCDF file consisting of atmospheric pressure, which will be used to atmospherically correct the pressure data. This path can also be passed as a command-line argument to `rskcdf2nc.py`.

## DIWASP processing and creation of EPIC-compliand wave statistics netCDF (.nc)

This step will generally be completed by using the import statement `from aqdlib import rsknc2diwasp` and calling `rsknc2diwasp.nc_to_diwasp()`, or by running `rsknc2diwasp.py` from the command line. Note that DIWASP is a MATLAB package and must be run from MATLAB before using this module. A sample MATLAB run file for DIWASP is included in the `scripts` directory.