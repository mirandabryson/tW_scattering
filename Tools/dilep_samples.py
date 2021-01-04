import os
import re
import glob
from Tools.config_helpers import *
cfg = loadConfig()

version = cfg['meta']['version']
tag = version.replace('.','p')
#tag = '0p1p5'
tag = 'v0.2.4'

# DILEP samples for DNN
data_path_dilep_2016 = os.path.join("/hadoop/cms/store/user/dspitzba/WH_hadronic/", tag)
data_path_dilep_2017 = os.path.join("/hadoop/cms/store/user/dspitzba/WH_hadronic/", tag)
data_path_dilep_2018 = os.path.join("/hadoop/cms/store/user/mbryson/WH_hadronic/", tag)


groups_dilep_2016 = {
    'TTW':           ['/TTWJets.*Summer16NanoAODv7[-_]'],
    'TTZ':           ['/TTZToLLNuNu[-_].*Summer16NanoAODv7[-_]'],
    'TTJets':        ['/TTJets_DiLept_TuneCUETP8M1.*Summer16NanoAODv7[-_]'],
    'DY_HT':         ['/DYJetsToLL_M-50_HT[-_].*Summer16NanoAODv7[-_]'],
    'DY_Tune':       ['/DYJetsToLL_M-50_TuneCUETP8M1.*Summer16NanoAODv7[-_]'], 
    'ZNuNu':         ['/ZJetsToNuNu.*Summer16NanoAODv7[-_]'],
    'DoubleMuon':    ['/DoubleMuon_Run2016[BCDEFGH]'],
    'DoubleEG':      ['/DoubleEG_Run2016[BCDEFGH]'],
    'MuonEG':        ['/MuonEG_Run2016[BCDEFGH]'],
}

groups_dilep_2017 = {
    'TTW':           ['/ttWJet.*Fall17NanoAODv7[-_]'],
    'TTZ':           ['/TTZToLLNuNu[-_].*Fall17NanoAODv7[-_]'],
    'TTJets':        ['/TTJets_DiLept_TuneCP5.*Fall17NanoAODv7[-_]'],
    'DY_HT':         ['/DYJetsToLL_M-50_HT[-_].*Fall17NanoAODv7[-_]'],
    'DY_Tune':       ['/DYJetsToLL_M-50_TuneCP5.*Fall17NanoAODv7[-_]'],
    'ZNuNu':         ['/ZJetsToNuNu.*Fall17NanoAODv7[-_]'],
    'DoubleMuon':    ['/DoubleMuon_Run2017[BCDEF]'],
    'DoubleEG':      ['/DoubleEG_Run2017[BCDEF]'],
    'MuonEG':        ['/MuonEG_Run2017[BCDEF]'],
}


groups_dilep_2018 = {
    'TTW':           ['/ttWJet.*Autumn18NanoAODv7[-_]'],
    'TTZ':           ['/TTZToLLNuNu[-_].*Autumn18NanoAODv7[-_]'],
    'TTJets':        ['/TTJets_DiLept_TuneCP5.*Autumn18NanoAODv7[-_]'],
    'DY_HT':         ['/DYJetsToLL_M-50_HT[-_].*Autumn18NanoAODv7[-_]'],
    'DY_Tune':       ['/DYJetsToLL_M-50_TuneCP5.*Autumn18NanoAODv7[-_]'],
    'ZNuNu':         ['/ZJetsToNuNu.*Autumn18NanoAODv7[-_]'],
    'DoubleMuon':    ['/DoubleMuon_Run2018[ABCD]'],
    'DoubleEG':      ['/EGamma_Run2018[ABCD]'],
    'MuonEG':        ['/MuonEG_Run2018[ABCD]'],
}


samples_dilep_2016 = glob.glob(data_path_dilep_2016 + '/*')
fileset_dilep_2016 = { group: [] for group in groups_dilep_2016.keys() }

samples_dilep_2017 = glob.glob(data_path_dilep_2017 + '/*')
fileset_dilep_2017 = { group: [] for group in groups_dilep_2017.keys() }

samples_dilep_2018 = glob.glob(data_path_dilep_2018 + '/*')
fileset_dilep_2018 = { group: [] for group in groups_dilep_2018.keys() }

for sample in samples_dilep_2016:
    for group in groups_dilep_2016.keys():
        for process in groups_dilep_2016[group]:
            if re.search(process, sample):
                fileset_dilep_2016[group] += glob.glob(sample+'/*.root')

fileset_dilep_2016_small = { sample: fileset_dilep_2016[sample][:2] for sample in fileset_dilep_2016.keys() }

for sample in samples_dilep_2017:
    for group in groups_dilep_2017.keys():
        for process in groups_dilep_2017[group]:
            if re.search(process, sample):
                fileset_dilep_2017[group] += glob.glob(sample+'/*.root')

fileset_dilep_2017_small = { sample: fileset_dilep_2017[sample][:2] for sample in fileset_dilep_2017.keys() }

for sample in samples_dilep_2018:
    for group in groups_dilep_2018.keys():
        for process in groups_dilep_2018[group]:
            if re.search(process, sample):
                fileset_dilep_2018[group] += glob.glob(sample+'/*.root')

fileset_dilep_2018_small = { sample: fileset_dilep_2018[sample][:2] for sample in fileset_dilep_2018.keys() }
