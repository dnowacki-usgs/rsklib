#!/usr/bin/env python

from __future__ import division, print_function

import sys
sys.path.insert(0, '/Users/dnowacki/Documents/rsklib')
import rsklib
import argparse
import yaml

parser = argparse.ArgumentParser(description='Convert raw RBR d|wave files (.rsk) to raw .cdf format. Run this script from the directory containing d|wave files')
parser.add_argument('gatts', help='path to global attributes file (gatts formatted)')
parser.add_argument('config', help='path to ancillary config file (YAML formatted)')

args = parser.parse_args()

# initialize metadata from the globalatts file
metadata = rsklib.read_globalatts(args.gatts)

# Add additional metadata from metadata config file
config = yaml.safe_load(open(args.config))

for k in config:
    metadata[k] = config[k]

RAW, metadata = rsklib.rsk_to_cdf(metadata)
