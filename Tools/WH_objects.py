from coffea.processor.accumulator import AccumulatorABC
from coffea.analysis_objects import JaggedCandidateArray
import numpy as np
## happily borrowed from https://github.com/bu-cms/bucoffea/blob/master/bucoffea/helpers/helpers.py

def mask_or(df, masks):
    """Returns the OR of the masks in the list
    :param df: Data frame
    :type df: LazyDataFrame
    :param masks: Mask names as saved in the df
    :type masks: List
    :return: OR of all masks for each event
    :rtype: array
    """
    # Start with array of False
    decision = np.ones(df.size)==0

    # Flip to true if any is passed
    for t in masks:
        try:
            decision = decision | df[t]
        except KeyError:
            continue
    return decision

def mask_and(df, masks):
    """Returns the AND of the masks in the list
    :param df: Data frame
    :type df: LazyDataFrame
    :param masks: Mask names as saved in the df
    :type masks: List
    :return: OR of all masks for each event
    :rtype: array
    """
    # Start with array of False
    decision = np.ones(df.size)==1

    # Flip to true if any is passed
    for t in masks:
        try:
            decision = decision & df[t]
        except KeyError:
            continue
    return decision


def getFilters(df, year=2018, dataset='None'):
    #filters, recommendations in https://twiki.cern.ch/twiki/bin/view/CMS/MissingETOptionalFiltersRun2
    if year == 2018:
        filters_MC = [\
            "Flag_goodVertices",
            "Flag_globalSuperTightHalo2016Filter",
            "Flag_HBHENoiseFilter",
            "Flag_HBHENoiseIsoFilter",
            "Flag_EcalDeadCellTriggerPrimitiveFilter",
            "Flag_BadPFMuonFilter",
            "Flag_ecalBadCalibFilterV2"
        ]
        
        filters_data = filters_MC + ["Flag_eeBadScFilter"]
        
    elif year == 2017:
        filters_MC = [\
            "Flag_goodVertices",
            "Flag_globalSuperTightHalo2016Filter",
            "Flag_HBHENoiseFilter",
            "Flag_HBHENoiseIsoFilter",
            "Flag_EcalDeadCellTriggerPrimitiveFilter",
            "Flag_BadPFMuonFilter",
            "Flag_ecalBadCalibFilterV2"
        ]
        
        filters_data = filters_MC + ["Flag_eeBadScFilter"]

    elif year == 2016:
        filters_MC = [\
            "Flag_goodVertices",
            "Flag_globalSuperTightHalo2016Filter",
            "Flag_HBHENoiseFilter",
            "Flag_HBHENoiseIsoFilter",
            "Flag_EcalDeadCellTriggerPrimitiveFilter",
            "Flag_BadPFMuonFilter",
        ]
        
        filters_data = filters_MC + ["Flag_eeBadScFilter"]
        
    if dataset.lower().count('data') or dataset.lower().count('run201'):
        return mask_and(df, filters_data)
    else:
        return mask_and(df, filters_MC)
        
def getTriggers(df, year=2018, dataset='None'):
    # these are the MET triggers from the MT2 analysis
    
    if year == 2018:
        triggers = [\
            "HLT_PFMET120_PFMHT120",
            "HLT_PFMET120_PFMHT120_PFHT60",
            "HLT_PFMETNoMu120_PFMHTNoMu120",
            "HLT_PFMETNoMu120_PFMHTNoMu120_PFHT60",
        ]
        
    elif year == 2017:
        triggers = [\
            "HLT_PFMET120_PFMHT120",
            "HLT_PFMET120_PFMHT120_PFHT60",
            "HLT_PFMETNoMu120_PFMHTNoMu120",
            "HLT_PFMETNoMu120_PFMHTNoMu120_PFHT60",
        ]
        
    elif year == 2016:
        triggers = [\
            "HLT_PFMET120_PFMHT120_IDTight",
            "HLT_PFMETNoMu120_PFMHTNoMu120_IDTight",
        ]
        
    if dataset.lower().count('data') or dataset.lower().count('run201'):
        return mask_or(df, triggers)
    else:
        return (df['run']>0)

    
def getMuons(df, WP='veto'):
    muon = JaggedCandidateArray.candidatesfromcounts(
            df['nMuon'],
            pt = df['Muon_pt'].content,
            eta = df['Muon_eta'].content,
            phi = df['Muon_phi'].content,
            mass = df['Muon_mass'].content,
            miniPFRelIso_all=df['Muon_miniPFRelIso_all'].content,
            looseId =df['Muon_looseId'].content,
            mediumId =df['Muon_mediumId'].content
            )
    if WP=='veto':
        return muon[(muon.pt > 10) & (abs(muon.eta) < 2.4) & (muon.looseId) & (muon.miniPFRelIso_all < 0.2)]
    elif WP=='medium':
        return muon[(muon.pt > 25) & (abs(muon.eta) < 2.4) & (muon.mediumId) & (muon.miniPFRelIso_all < 0.2)]


def getElectrons(df, WP='veto'):
    electron = JaggedCandidateArray.candidatesfromcounts(
            df['nElectron'],
            pt = df['Electron_pt'].content,
            eta = df['Electron_eta'].content,
            phi = df['Electron_phi'].content,
            mass = df['Electron_mass'].content,
            miniPFRelIso_all=df['Electron_miniPFRelIso_all'].content,
            cutBased=df['Electron_cutBased'].content
            )
    if WP=='veto':
        return electron[(electron.pt>10) & (abs(electron.eta) < 2.4) & (electron.miniPFRelIso_all < 0.1) &  (electron.cutBased >= 1)]
    elif WP=='medium':
        return electron[(electron.pt>25) & (abs(electron.eta) < 2.4) & (electron.miniPFRelIso_all < 0.1) &  (electron.cutBased >= 3)]

def getFatJets(df):
    fatjet = JaggedCandidateArray.candidatesfromcounts(
            df['nFatJet'],
            pt = df['FatJet_pt'].content,
            eta = df['FatJet_eta'].content,
            phi = df['FatJet_phi'].content,
            mass = df['FatJet_mass'].content,
            msoftdrop = df["FatJet_msoftdrop"].content,  
            deepTagMD_HbbvsQCD = df['FatJet_deepTagMD_HbbvsQCD'].content, 
            deepTagMD_WvsQCD = df['FatJet_deepTagMD_WvsQCD'].content, 
            deepTag_WvsQCD = df['FatJet_deepTag_WvsQCD'].content
            
        )
    return fatjet[(fatjet.pt>200) & (abs(fatjet.eta)<2.4)]

def getJets(df):
    jet = JaggedCandidateArray.candidatesfromcounts(
            df['nJet'],
            pt = df['Jet_pt'].content,
            eta = df['Jet_eta'].content,
            phi = df['Jet_phi'].content,
            mass = df['Jet_mass'].content,
            jetId = df['Jet_jetId'].content, # https://twiki.cern.ch/twiki/bin/view/CMS/JetID
            btagDeepB = df['Jet_btagDeepB'].content, # https://twiki.cern.ch/twiki/bin/viewauth/CMS/BtagRecommendation102X
        )
    return jet[(jet.pt>30) & (abs(jet.eta)<2.4) & (jet.jetId>1)]
