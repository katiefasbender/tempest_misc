#!/usr/bin/env/python 

# This file contains a function written by David Nidever called getdata()
# which qill download exposures from the NOIRLab Astro Data Archive for use 
# in the NSC measurements analysis process.  

#------------------------------------------------------------
# Imports 
#------------------------------------------------------------

import os
import time
import numpy as np
import pandas as pd
import requests
import urllib.request
from argparse import ArgumentParser
from dlnpyutils import utils as dln

#------------------------------------------------------------
# Functions
#------------------------------------------------------------

def getdata(rawname,fluxfile,maskfile,wtfile,outdir='.'):
    """ Download exposures from Astro Data Archive """
    natroot = 'https://astroarchive.noirlab.edu'
    adsurl = f'{natroot}/api/adv_search'
    # https://astroarchive.noirlab.edu/api/adv_search

    t0 = time.time()

    fluxbase = os.path.basename(fluxfile)
    maskbase = os.path.basename(maskfile)
    wtbase = os.path.basename(wtfile)    

    print('Downloading data for exposure RAWNAME = ',rawname)

    # Do the query
    jj = {"outfields" : ["original_filename", "archive_filename", "md5sum", "url"],
          "search" : [
                ["original_filename",rawname, 'icontains'],
           ]}
    res =  pd.DataFrame(requests.post(f'{adsurl}/fasearch/?limit=100',json=jj).json()[1:])  
    # Get the base names and compare to our filenames
    base = [os.path.basename(f) for f in res.archive_filename]
    fluxind, = np.where(np.array(base)==fluxbase)
    maskind, = np.where(np.array(base)==maskbase)
    wtind, = np.where(np.array(base)==wtbase)
    # Download the files
    # 'https://astroarchive.noirlab.edu/api/retrieve/26e44fdb72ae8c6123511bead4caa97a/'
    print('Downloading fluxfile =',fluxfile,' url =',res.url[fluxind[0]])
    urllib.request.urlretrieve(res.url[fluxind[0]],outdir+'/'+fluxbase)   # save to fluxfile
    print('Downloading maskfile =',maskfile,' url =',res.url[maskind[0]])
    urllib.request.urlretrieve(res.url[maskind[0]],outdir+'/'+maskbase)   # save to maskfile
    print('Downloading wtfile =',wtfile,' url =',res.url[wtind[0]])
    urllib.request.urlretrieve(res.url[wtind[0]],outdir+'/'+wtbase)   # save to wtfile

    print('dt = ',time.time()-t0,' sec')

#------------------------------------------------------------
# Main Code
#------------------------------------------------------------
if __name__ == "__main__":


    # Initiate arguments
    parser=ArgumentParser(description='Download flux, weight, and mask files for rawname')
    parser.add_argument('--rawname',type=str,nargs=1,help="exposure rawname")
    parser.add_argument('--fluxfile',type=str,nargs=1,help="exposure flux file")
    parser.add_argument('--wtfile',type=str,nargs=1,help="exposure weight file")
    parser.add_argument('--maskfile',type=str,nargs=1,help="exposure mask file")
    parser.add_argument('--outdir',type=str,nargs=1,default=".",help="directory to which files will be downloaded")
    args = parser.parse_args()

    # Load arguments
    rawname=dln.first_el(args.rawname) # dln.first_el() returns the first element 
    fluxfile=dln.first_el(args.fluxfile)
    wtfile=dln.first_el(args.wtfile)
    maskfile=dln.first_el(args.maskfile)
    outdir=dln.first_el(args.outdir)

    getdata(rawname,fluxfile,wtfile,maskfile,outdir)

