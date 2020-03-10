#!/usr/bin/env python3

import urllib.request
import shutil, yaml
import os, sys, platform
import pkg_resources
from pkg_resources import DistributionNotFound, VersionConflict



# Check the dependency requirements

dependencies = [
    'pandas>=0.24.0',
    'mysql-connector-python>=8.0.0',
    'PyYAML>=5.0.0'
]

try:
    pkg_resources.require(dependencies)
except DistributionNotFound as ex:
    sys.exit("Error: The '%s' distribution was not found and is required by DataHub. Please install it by:\n\n >>> pip install %s\n" % (str(ex.args[0]), str(ex.args[0])))



# Update online setting file

url_root = "http://risklab.chicagobooth.edu/RLD/"
url = url_root + 'settings.yaml'

setting_file_path = 'APPDATA' if platform.system() == 'Windows' else 'HOME'
setting_file = os.environ[setting_file_path] + '/.RLD_settings.yaml'

try:
    with urllib.request.urlopen(url) as response, open(setting_file, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)
except:
    sys.exit("Error: failed to download the settings for RLD.")

try:
    with open(setting_file, 'r') as f:
        settings = yaml.safe_load(f)
except:
    sys.exit("Error: failed to read and parse the settings for RLD.")

__version__ = "0.0.2"
from packaging import version

if version.parse(__version__) < version.parse(settings['required-ver']):
    sys.exit("Error: the version of DataHub is not sufficient. Please update it via Git:\n\n $ git pull\n")


# Load modules

from .handle import *



def list_handles(verbose = True):
    if verbose:
        print("\nList of available handles:\n\n"
            + '\n'.join([ " * " + h for h in settings['handles']]) + '\n')
        
    return list(settings['handles'])



def list_requests(handle, verbose = True):
    
    if handle not in settings['handles']:
        sys.exit("Error: unknown handle [%s]." % (handle))
    
    requests = settings['handles'][handle]['requests']
    
    if verbose:
        
        maxlen = max([len(r) for r in requests])
        fmt = " * %-" + str(maxlen) +"s  %s"
        
        print(("\nList of available requests in [%s]:\n\n" % (handle))
            + '\n'.join([(fmt % (r, requests[r]['func'])) for r in requests]) + '\n')
    
    return list(requests)



def show_request(handle, request, show='both'):
    
    try:
        r = settings['handles'][handle]['requests'][request]
    except:
        sys.exit("Error: unknown request [%s.%s]." % (handle, request))
    
    info = "\nRequest: %s.%s\n" % (handle, request)
    info += r['func'] + "\n"
    
    if show=='both' or show=='args':
        info += "\nArguments:\n\n"
        maxlen = max([len(arg) for arg in r['args']])
        fmt = " %-" + str(maxlen) +"s  %s\n %" + str(maxlen) + "s  Default: %s\n"
        info += "\n".join([(fmt % (arg,r['args'][arg][1]," ",str(r['args'][arg][0]))) for arg in r['args']])
    
    if 'fields' in r['args'] and (show=='both' or show=='fields'):
        info += "\nFields:\n\n"
        url = url_root + r['source'] + '.txt'

        try:
            with urllib.request.urlopen(url) as response:
                cols = [row.split("\t") for row in response.read().decode('ascii').strip().split("\n")]
                fmt = " * %-" + str(max([len(c[0]) for c in cols])) + "s  %-5s %s"
                info += "\n".join([(fmt % tuple(col[:3])) for col in cols]) + "\n"
        except:
            info += " ** Failed to query fields information online.\n"
    
    info += "\n- " + r['link'] + "\n"
    print(info)
    return


