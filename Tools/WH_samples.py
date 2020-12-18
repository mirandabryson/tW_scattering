import os
import re
import glob
from Tools.config_helpers import *
cfg = loadConfig()

version = cfg['meta']['version']
tag = version.replace('.','p')
#tag = '0p1p5'
tag = 'v0.2.4'

#data_path = os.path.join(cfg['meta']['localSkim'], tag)
data_path_2016 = os.path.join("/hadoop/cms/store/user/dspitzba/WH_hadronic/", tag)
data_path_2017 = os.path.join("/hadoop/cms/store/user/dspitzba/WH_hadronic/", tag)
data_path_2018 = os.path.join("/hadoop/cms/store/user/mbryson/WH_hadronic/", tag)

# WH All Had samples for DNN
groups_WH_2016 = {
    'signal':        ['/SMS-TChiWH.*Summer16NanoAODv7[-_]'],
    'TTW':           ['/TTWJets.*Summer16NanoAODv7[-_]','/TT[W][W][-_].*Summer16NanoAODv7[-_]'],
    'TTZ':           ['/TTZToLLNuNu[-_].*Summer16NanoAODv7[-_]','/TT[Z][Z][-_].*Summer16NanoAODv7[-_]'],
    'TTJets':        [ # default set
            'TTJets_SingleLeptFromT(|bar)_TuneCUETP8M1[-_].*Summer16NanoAODv7[-_]',
            '/TTJets_DiLept_TuneCUETP8M1.*Summer16NanoAODv7[-_]'
            ],
    'TTJets_ext':    [ # use those with stitch only!!
            'TTJets_SingleLeptFromT(|bar)_TuneCUETP8M1[-_].*Summer16NanoAODv7[-_]',
            'TTJets_SingleLeptFromT(|bar)_genMET-150_TuneCUETP8M1[-_].*Summer16NanoAODv7[-_]',
            '/TTJets_DiLept_TuneCUETP8M1.*Summer16NanoAODv7[-_]',
            '/TTJets_DiLept_genMET-150_TuneCUETP8M1.*Summer16NanoAODv7[-_]',
            ],
    'QCD':           ['/QCD_HT.*Summer16NanoAODv7[-_]'],
    'WJets':         ['/W[1-4]JetsToLNu_TuneCUETP8M1[-_].*Summer16NanoAODv7[-_]'],
    'WJets_ext':     [ # use those with stitch only!!
            '/W[1-4]JetsToLNu_TuneCUETP8M1[-_].*Summer16NanoAODv7[-_]', 
            '/W[1-4]JetsToLNu_NuPt-200_TuneCUETP8M1[-_].*Summer16NanoAODv7[-_]'
            ],
    'ZNuNu':         ['/ZJetsToNuNu.*Summer16NanoAODv7[-_]'],
    'DY':            ['/DYJetsToLL_M-50_HT[-_].*Summer16NanoAODv7[-_]'], # only use HT>100. needs to be applied on analysis level, too
    'ST':            ['/ST[-_].*Summer16NanoAODv7[-_]'],
    'WW':            ['/WW_.*Summer16NanoAODv7[-_]'],
    'VV':            ['/ZZTo.*Summer16NanoAODv7[-_]', '/WZ_.*Summer16NanoAODv7[-_]'],
    'MET':           ['/MET_Run2016[BCDEFGH]'],
    'DoubleMuon':    ['/DoubleMuon_Run2016[BCDEFGH]'],
    'DoubleEG':      ['/DoubleEG_Run2016[BCDEFGH]'],
    'MuonEG':        ['/MuonEG_Run2016[BCDEFGH]'],
}

groups_WH_2017 = {
    'signal':        ['/SMS-TChiWH.*Fall17NanoAODv7[-_]'],
    'TTW':           ['/ttWJet.*Fall17NanoAODv7[-_]','/TT[W][W][-_].*Fall17NanoAODv7[-_]'],
    'TTZ':           ['/TTZToLLNuNu[-_].*Fall17NanoAODv7[-_]','/TT[Z][Z][-_].*Fall17NanoAODv7[-_]'],
    'TTJets':        [ # default set
            'TTJets_SingleLeptFromT(|bar)_TuneCP5[-_].*Fall17NanoAODv7[-_]',
            '/TTJets_DiLept_TuneCP5.*Fall17NanoAODv7[-_]'
            ],
    'TTJets_ext':    [ # use those with stitch only!!
            'TTJets_SingleLeptFromT(|bar)_TuneCP5[-_].*Fall17NanoAODv7[-_]',
            'TTJets_SingleLeptFromT(|bar)_genMET-150_TuneCP5[-_].*Fall17NanoAODv7[-_]',
            '/TTJets_DiLept_TuneCP5.*Fall17NanoAODv7[-_]',
            '/TTJets_DiLept_genMET-150_TuneCP5.*Fall17NanoAODv7[-_]',
            ],
    'QCD':           ['/QCD_HT.*Fall17NanoAODv7[-_]'],
    'WJets':         ['/W[1-4]JetsToLNu_TuneCP5[-_].*Fall17NanoAODv7[-_]'],
    'WJets_ext':     [ # use those with stitch only!!
            '/W[1-4]JetsToLNu_TuneCP5[-_].*Fall17NanoAODv7[-_]', 
            '/W[1-4]JetsToLNu_NuPt-200_TuneCP5[-_].*Fall17NanoAODv7[-_]'
            ],
    'ZNuNu':         ['/ZJetsToNuNu.*Fall17NanoAODv7[-_]'],
    'DY':            ['/DYJetsToLL_M-50_HT[-_].*Fall17NanoAODv7[-_]'], # only use HT>100. needs to be applied on analysis level, too
    'ST':            ['/ST[-_].*Fall17NanoAODv7[-_]'],
    'WW':            ['/WW_.*Fall17NanoAODv7[-_]'],
    'VV':            ['/ZZTo.*Fall17NanoAODv7[-_]', '/WZ_.*Fall17NanoAODv7[-_]'],
    'MET':           ['/MET_Run2017[BCDEF]'],
    'DoubleMuon':    ['/DoubleMuon_Run2017[BCDEF]'],
    'DoubleEG':      ['/DoubleEG_Run2017[BCDEF]'],
    'MuonEG':        ['/MuonEG_Run2017[BCDEF]'],
}


groups_WH_2018 = {
    'signal':        ['/SMS-TChiWH.*Autumn18NanoAODv7[-_]'],
    'TTW':           ['/ttWJet.*Autumn18NanoAODv7[-_]','/TT[W][W][-_].*Autumn18NanoAODv7[-_]'],
    'TTZ':           ['/TTZToLLNuNu[-_].*Autumn18NanoAODv7[-_]','/TT[Z][Z][-_].*Autumn18NanoAODv7[-_]'],
    'TTJets':        [ # default set
            'TTJets_SingleLeptFromT(|bar)_TuneCP5[-_].*Autumn18NanoAODv7[-_]',
            '/TTJets_DiLept_TuneCP5.*Autumn18NanoAODv7[-_]'
            ],
    'TTJets_ext':    [ # use those with stitch only!!
            'TTJets_SingleLeptFromT(|bar)_TuneCP5[-_].*Autumn18NanoAODv7[-_]',
            'TTJets_SingleLeptFromT(|bar)_genMET-80_TuneCP5[-_].*Autumn18NanoAODv7[-_]',
            '/TTJets_DiLept_TuneCP5.*Autumn18NanoAODv7[-_]',
            '/TTJets_DiLept_genMET-80_TuneCP5.*Autumn18NanoAODv7[-_]',
            ],
    'QCD':           ['/QCD_HT.*Autumn18NanoAODv7[-_]'],
    'WJets':         ['/W[1-4]JetsToLNu_TuneCP5[-_].*Autumn18NanoAODv7[-_]'],
    'WJets_ext':     [ # use those with stitch only!!
            '/W[1-4]JetsToLNu_TuneCP5[-_].*Autumn18NanoAODv7[-_]', 
            '/W[1-4]JetsToLNu_NuPt-200_TuneCP5[-_].*Autumn18NanoAODv7[-_]'
            ],
    'ZNuNu':         ['/ZJetsToNuNu.*Autumn18NanoAODv7[-_]'],
    'DY':            ['/DYJetsToLL_M-50_HT[-_].*Autumn18NanoAODv7[-_]'], # only use HT>100. needs to be applied on analysis level, too
    'ST':            ['/ST[-_].*Autumn18NanoAODv7[-_]'],
    'WW':            ['/WW_.*Autumn18NanoAODv7[-_]'],
    'VV':            ['/ZZTo.*Autumn18NanoAODv7[-_]', '/WZ_.*Autumn18NanoAODv7[-_]'],
    'MET':           ['/MET_Run2018[ABCD]'],
    'DoubleMuon':    ['/DoubleMuon_Run2018[ABCD]'],
    'DoubleEG':      ['/EGamma_Run2018[ABCD]'],
    'MuonEG':        ['/MuonEG_Run2018[ABCD]'],
}


samples_2016 = glob.glob(data_path_2016 + '/*')
fileset_2016 = { group: [] for group in groups_WH_2016.keys() }

samples_2017 = glob.glob(data_path_2017 + '/*')
fileset_2017 = { group: [] for group in groups_WH_2017.keys() }

samples_2018 = glob.glob(data_path_2018 + '/*')
fileset_2018 = { group: [] for group in groups_WH_2018.keys() }

for sample in samples_2016:
    for group in groups_WH_2016.keys():
        for process in groups_WH_2016[group]:
            if re.search(process, sample):
                fileset_2016[group] += glob.glob(sample+'/*.root')

fileset_2016_small = { sample: fileset_2016[sample][:2] for sample in fileset_2016.keys() }

for sample in samples_2017:
    for group in groups_WH_2017.keys():
        for process in groups_WH_2017[group]:
            if re.search(process, sample):
                fileset_2017[group] += glob.glob(sample+'/*.root')

fileset_2017_small = { sample: fileset_2017[sample][:2] for sample in fileset_2017.keys() }

for sample in samples_2018:
    for group in groups_WH_2018.keys():
        for process in groups_WH_2018[group]:
            if re.search(process, sample):
                fileset_2018[group] += glob.glob(sample+'/*.root')

fileset_2018_small = { sample: fileset_2018[sample][:2] for sample in fileset_2018.keys() }


