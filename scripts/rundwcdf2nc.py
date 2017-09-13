#!/usr/bin/env python

from __future__ import division, print_function

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
