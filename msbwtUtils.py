import string
import glob
import sqlite3
from collections import defaultdict
from msSharedUtil import (dirLabels, MSBWTdirs, uniformLengths)
from msbwtCloudUtils import *
from MUSCython import MultiStringBWTCython as MSBWT

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

    if not forceLocal:
        try:
            remoteSource = findRemote(name)
            return CloudBWT(name, remoteSource)
        except Exception as e:
            print(e)

    try:
        localSource = findLocal(name)
        return MSBWT.loadBWT(localSource)
    except Exception as e:
        print(e)
        return None


def findLocal(name):

    for di in MSBWTdirs:
        avail = sorted(glob.glob(%s/*/))
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


