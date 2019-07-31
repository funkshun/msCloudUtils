import string
import glob
import sqlite3
from collections import defaultdict
from msSharedUtil import (dirLabels, MSBWTdirs, uniformLengths)
from msCloudUtils.msbwtCloudUtils import *
from MUSCython import MultiStringBWTCython as MSBWT
from datetime import datetime as dt

LEGACY = True

def loadMeta(metaFile):
    MSBWTavailable = defaultdict(list)
    cloudUrls = {}
    uniformLengths = {}
    with open(metaFile, 'rf') as f:
        header = next(f)
        for line in f:
            group, msbwtName, url, readLength = line.strip().split(',')
            MSBWTavailable[group].append(msbwtName)
            cloudUrls[msbwtName] = url
            uniformLengths[msbwtName] = int(readLength)
    return MSBWTavailable, cloudUrls, uniformLengths

if LEGACY:
    remoteMSBWT, remoteUrls, remoteLengths = loadMeta('msbwt.meta')

def loadBWT(name, forceLocal=False):
    logIt("Loading %s...\n" % name)

    if not forceLocal:
        try:
            logIt("Trying remote source...\n")
            remoteSource = findRemote(name)
            return CloudBwt(name, remoteSource)
        except Exception as e:
            logIt(" Failed\n" + e.message)
            pass
    try:
        localSource = findLocal(name)
        return MSBWT.loadBWT(localSource)
    except Exception as e:
        return None


def findLocal(name):

    for di in MSBWTdirs:
        avail = sorted(glob.glob('%s/*/' % di))
        for poss in avail:
            if poss.strip().split('/')[-2] == name:
                return poss
    else:
        raise ValueError("Failed to find a local source for {}".format(name))

def findRemote(name):

    try:
        return remoteUrls[name]
    except Exception as e:
        raise ValueError("No remote url available for {}".format(name))

def logIt(string):
    logfile = 'msbwtCloud.log'
    
    with open(logfile, 'a') as f:
        dtstring = dt.strftime(dt.now(), "%Y/%m/%d %H:%M:%S")
        logString = "[" + dtstring + "] " + string
        f.write(string)
