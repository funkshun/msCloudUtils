import cgAdmin
import glob
import os

dirLabels = ['CEGS 3x3 diallele',
             'CC Founders',
             'WashU HR Mice',
             'Wild Testis RNA-seq (PRN \'14)',
             'Maternal Nutrition Experiment',
             'Seisure Study',
             'Miscellaneous datasets',
             'Unclassified Samples',
             'CEGS2',
             'wild mice',
             'CC Genome',
             'Sister strains',
             'Hao dataset',
             'Poe Pool',
             'C57BL6 pedigree',
             'CC027 RNA Seq']
MSBWTdirs = ['/csbiodata/CEGS3x3BWT',
             '/csbiodata/BWTs/sangerMsbwt', #'/playpen/sangerMsbwt',
             '/csbiodata/PompWU-nobackup/msbwts',
             '/csbiodata/BWTs/wild_testis_RNAseq', #'/playpen/wild_testis_RNAseq',
             '/csbiodata/BWTs/matnut_full', # '/csbiohome/holtjma/auxiliaryBWTSpace/matnut_full',
             '/csbiodataxv/SeizureStudy',
             '/csbiodata/BWTs/unclassified',# '/playpen',
             '/csbiodata/BWTs/temporaryWebAccess', # '/csbiohome/holtjma/auxiliaryBWTSpace/temporaryWebAccess',
             '/csbiodata/BWTs/cegs2/msbwt',# '/csbiohome/holtjma/auxiliaryBWTSpace/cegs2/msbwt',
             '/csbiodata/BWTs/minibwt', # '/csbiohome/holtjma/auxiliaryBWTSpace/minibwt',
             '/csbiodata/perlegen/CC_bwt',
             '/csbiodata/perlegen/sisterStrains',
             '/csbiodata/RNAseq-nobackup/Hao',
             '/csbiodata/RNAseq-nobackup/PoePool',
             '/csbiodataxw/C57BL6',
             '/csbiodata/perlegen/CC027_RNASeq']
uniformLengths = [101, 101, 101, 77, 101, 50, 101, 0, 101, 101, 151, 151, 151, 76, 150, 151, 151]
minPermission = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

dirLabelsRNA = ['CEGS 3x3 diallele',
             'Wild Testis RNA-seq (PRN \'14)',
             'Maternal Nutrition Experiment',
             ]
MSBWTdirsRNA = ['/csbiodata/CEGS3x3BWT',
             '/csbiodata/BWTs/wild_testis_RNAseq', #'/playpen/wild_testis_RNAseq',
             '/csbiodata/BWTs/matnut_full', # '/csbiohome/holtjma/auxiliaryBWTSpace/matnut_full',
             ]
uniformLengthsRNA = [101, 77, 101]
minPermissionRNA = [0, 0, 0]


def buildRadioSelect(form, panel, includeFormStart):
    user, permissionLevel, date, time, elapsed = cgAdmin.getCompgenCookie(form)
    citationDict = {}
    citOrder = []
    citCount = 1
    tableOrdering = ["Species", "Strain", "Data Type", "Sequence Method", "Number of Reads", "Size of BWT", "Publication"]
    
    panel.h3("Select Dataset:")
    if includeFormStart:
        panel.form(action="", method="POST", enctype="multipart/form-data")
    panel.ul(id='mainList')
    for j, msdir in enumerate(MSBWTdirs):
        if permissionLevel < minPermission[j]:
            continue
        
        available = sorted(glob.glob("%s/*/*msbwt.npy" % msdir))
        panel.li()
        #panel.input(type="checkbox", name="", value="")
        panel.add(dirLabels[j])
        panel.ul()
        panel.table(border='1')
        panel.tr()
        panel.th("")
        panel.th("Dataset")
        for l in tableOrdering:
            panel.th(l)
        panel.tr.close()
        for i, dataset in enumerate(available):
            end = dataset.rfind('/')
            start = dataset.rfind('/', 0, end-1) + 1
            shorten = dataset[start:end]
            metadata = loadMetadata(dataset[0:end])
            
            #panel.li()
            panel.tr()
            panel.td()
            if i == 0 and j == 0:
                panel.input(type="radio", name="dataset", value=str(j)+'-'+shorten, checked="Y")
            else:
                panel.input(type="radio", name="dataset", value=str(j)+'-'+shorten)
            panel.td.close()
            panel.td()
            panel.add(metadata.get("Name", shorten))
            panel.td.close()
            for l in tableOrdering:
                if l == "Publication":
                    citation = metadata.get(l, "Not available")
                    if citation == "Not available":
                        panel.td("Not available", style="text-align:right;")
                    else:
                        if citationDict.has_key(citation):
                            citLink, citID = citationDict[citation]
                        else:
                            citLink = 'citation'+str(citCount)
                            citationDict[citation] = (citLink, citCount)
                            citOrder.append(citation)
                            citID = citCount
                            citCount += 1
                        panel.td()
                        panel.a('Pub '+str(citID), href='#'+citLink)
                        panel.td.close()
                else:
                    panel.td(metadata.get(l, "Not available"), style="text-align:right;")
            panel.tr.close()
        #panel.li.close()
        #panel.br()
        panel.table.close()
        panel.br()
        panel.ul.close()
        panel.li.close()
    panel.ul.close()#this one closes the mainList
    
    return citOrder, citationDict

def buildCheckboxSelect(form, panel, includeFormStart=True):
    user, permissionLevel, date, time, elapsed = cgAdmin.getCompgenCookie(form)
    citationDict = {}
    citOrder = []
    citCount = 1
    tableOrdering = ["Species", "Strain", "Data Type", "Sequence Method", "Number of Reads", "Size of BWT", "Publication"]
    
    panel.h3("Select Dataset:")
    if includeFormStart:
        panel.form(action="", method="POST", enctype="multipart/form-data")
    panel.ul(id='mainList')
    for j, msdir in enumerate(MSBWTdirs):
        if permissionLevel < minPermission[j]:
            continue
        
        available = sorted(glob.glob("%s/*/*msbwt.npy" % msdir))
        panel.li()
        panel.input(type="checkbox", name="", value="")
        panel.add(dirLabels[j])
        panel.ul()
        panel.table(border='1')
        panel.tr()
        panel.th("")
        panel.th("Dataset")
        for l in tableOrdering:
            panel.th(l)
        panel.tr.close()
        for i, dataset in enumerate(available):
            end = dataset.rfind('/')
            start = dataset.rfind('/', 0, end-1) + 1
            shorten = dataset[start:end]
            metadata = loadMetadata(dataset[0:end])
            
            #panel.li()
            panel.tr()
            panel.td()
            panel.input(type="checkbox", name="dataset", value=str(j)+'-'+shorten)
            panel.input(type="hidden", id=str(j)+'-'+shorten, value=metadata.get("Name", shorten))
            panel.td.close()
            panel.td()
            panel.add(metadata.get("Name", shorten))
            panel.td.close()
            for l in tableOrdering:
                if l == "Publication":
                    citation = metadata.get(l, "Not available")
                    if citation == "Not available":
                        panel.td("Not available", style="text-align:right;")
                    else:
                        if citationDict.has_key(citation):
                            citLink, citID = citationDict[citation]
                        else:
                            citLink = 'citation'+str(citCount)
                            citationDict[citation] = (citLink, citCount)
                            citOrder.append(citation)
                            citID = citCount
                            citCount += 1
                        panel.td()
                        panel.a('Pub '+str(citID), href='#'+citLink)
                        panel.td.close()
                else:
                    panel.td(metadata.get(l, "Not available"), style="text-align:right;")
            panel.tr.close()
        #panel.li.close()
        #panel.br()
        panel.table.close()
        panel.br()
        panel.ul.close()
        panel.li.close()
    
    panel.ul.close()#this one closes the mainList
    return citOrder, citationDict

def buildRNACheckboxSelect(form, panel, includeFormStart=True):
    user, permissionLevel, date, time, elapsed = cgAdmin.getCompgenCookie(form)
    citationDict = {}
    citOrder = []
    citCount = 1
    tableOrdering = ["Species", "Strain", "Data Type", "Sequence Method", "Number of Reads", "Publication"]
    
    panel.h3("Select Dataset:")
    if includeFormStart:
        panel.form(action="", method="POST", enctype="multipart/form-data")
    panel.ul(id='mainList')
    for j, msdir in enumerate(MSBWTdirsRNA):
        if permissionLevel < minPermissionRNA[j]:
            continue
        
        available = sorted(glob.glob("%s/*/*msbwt.npy" % msdir))
        panel.li()
        panel.input(type="checkbox", name="", value="")
        panel.add(dirLabelsRNA[j])
        panel.ul()
        panel.table(border='1')
        panel.tr()
        panel.th("")
        panel.th("Dataset")
        for l in tableOrdering:
            panel.th(l)
        panel.tr.close()
        for i, dataset in enumerate(available):
            end = dataset.rfind('/')
            start = dataset.rfind('/', 0, end-1) + 1
            shorten = dataset[start:end]
            metadata = loadMetadata(dataset[0:end])
            
            #panel.li()
            panel.tr()
            panel.td()
            panel.input(type="checkbox", name="dataset", value=str(j)+'-'+shorten)
            panel.input(type="hidden", id=str(j)+'-'+shorten, value=metadata.get("Name", shorten))
            panel.td.close()
            panel.td()
            panel.add(metadata.get("Name", shorten))
            panel.td.close()
            for l in tableOrdering:
                if l == "Publication":
                    citation = metadata.get(l, "Not available")
                    if citation == "Not available":
                        panel.td("Not available", style="text-align:right;")
                    else:
                        if citationDict.has_key(citation):
                            citLink, citID = citationDict[citation]
                        else:
                            citLink = 'citation'+str(citCount)
                            citationDict[citation] = (citLink, citCount)
                            citOrder.append(citation)
                            citID = citCount
                            citCount += 1
                        panel.td()
                        panel.a('Pub '+str(citID), href='#'+citLink)
                        panel.td.close()
                else:
                    panel.td(metadata.get(l, "Not available"), style="text-align:right;")
            panel.tr.close()
        #panel.li.close()
        #panel.br()
        panel.table.close()
        panel.br()
        panel.ul.close()
        panel.li.close()
    
    panel.ul.close()#this one closes the mainList
    return citOrder, citationDict
    
def loadMetadata(msbwtDir):
    '''
        Return a dictionary of values
        '''
    csvFN = msbwtDir + '/metadata.csv'
    ret = {}
    if os.path.exists(csvFN):
        fp = open(csvFN, 'r')
        for line in fp:
            if line == '\n':
                continue
            nv = line.strip('\n').split(',')
            ret[nv[0]] = ','.join(nv[1:])
        fp.close()
    return ret
