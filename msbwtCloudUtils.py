import requests
from MUSCython import MultiStringBWTCython as msBWT

def checkHost(host):
    if host[-1] != '/':
        host += '/'
    r = requests.get(host + 'checkAlive')

    if r.status_code == 200:
        j = r.json()
        return j['name']
    else:
        return None

class CloudBwt():

    def __init__(self, name, host):
        self.name = name
        self.host = host

    def countOccurrencesOfSeq(self, seq, givenRange=None):
        para = {
            "args": [seq]
        }
        if givenRange is not None:
            para["givenRange"] = givenRange
        res = requests.get(self.host + "/"+ self.name + "/" + "countOccurrencesOfSeq", params=para)
        return res.json()['result']

    def findIndicesOfStr(self, seq, givenRange=None):
        para = {
            "args": [seq]
        }
        if givenRange is not None:
            para["givenRange"] = givenRange
        res = requests.get(self.host + "/"+ self.name + "/" + "findIndicesOfStr", params=para)
        return res.json()['result']

    def getBWTRange(self, start, end):
        para = {
            "args": [start, end]
        }
        res = requests.get(self.host + "/"+ self.name + "/" + "getBWTRange", params=para)
        return res.json()['result']

    def getCharAtIndex(self, index):
        para = {
            "args": [index]
        }
        res = requests.get(self.host + "/"+ self.name + "/" + "getCharAtIndex", params=para)
        return res.json()['result']

    def getFullFMAtIndex(self, index):
        para = {
            "args": [index]
        }
        res = requests.get(self.host + "/"+ self.name + "/" + "getFullFMAtIndex", params=para)
        return res.json()['result']

    def getOccurrenceOfCharAtIndex(self, sym, index):
        para = {
            "args": [sym, index]
        }
        res = requests.get(self.host + "/"+ self.name + "/" + "getOccurrenceOfCharAtIndex", params=para)
        return res.json()['result']

    def getSequenceDollarID(self, strIndex, returnOffset=False):
        para = {
            "args": [strIndex],
            "returnOffset": returnOffset
        }
        res = requests.get(self.host + "/"+ self.name + "/" + "getSequenceDollarID", params = para)
        return res.json()['result']

    def getTotalSize(self):
        res = requests.get(self.host + "/"+ self.name + "/" + "getTotalSize")
        return res.json()['result']

    def recoverString(self, strIndex, withIndex=False):
        para = {
            "args": [strIndex],
            "withIndex": False
        }
        res = requests.get(self.host + "/"+ self.name + "/" + "recoverString", params=para)
        return res.json()['result']
