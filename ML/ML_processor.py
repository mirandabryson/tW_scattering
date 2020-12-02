import os
import time
import glob
import re
import pandas as pd
from functools import reduce
from klepto.archives import dir_archive

import numpy as np
from tqdm.auto import tqdm
import coffea.processor as processor
from coffea.processor.accumulator import AccumulatorABC
from coffea.analysis_objects import JaggedCandidateArray
from coffea.btag_tools import BTagScaleFactor
from coffea import hist
import pandas as pd
import uproot_methods
import uproot
import awkward
import copy

from memory_profiler import profile

from Tools.helpers import loadConfig, getCutFlowTable, mergeArray, mt
#from Tools.objects import Collections
#from Tools.cutflow import Cutflow

class WHhadProcessor(processor.ProcessorABC):
    def __init__(self):
        
        #Great, now let's define some bins for our histograms.
        
        dataset_axis         = hist.Cat("dataset", "Primary dataset")
        pt_axis              = hist.Bin("pt", r"$p_{T}$ (GeV)", 500, 0, 2000)
        multiplicity_axis    = hist.Bin("multiplicity", r"N", 30, -0.5, 29.5)
        phi_axis             = hist.Bin("phi", r"$\Delta \phi$", 80, 0, 8)
        mass_axis            = hist.Bin("mass", r"mass (GeV)", 500, 0, 2000)
        r_axis               = hist.Bin("r", r"$\Delta R$", 80, 0, 4)

        #In order to create proper histograms, we always need to include a dataset axis!
        #For different types of histograms with different scales, I create axis to fit 
        #those dimensions!
        
        #Now, let's move to actually telling our processor what histograms we want to make.
        #Let's start out simple. 
        
        self._accumulator = processor.dict_accumulator({
            "met":                          processor.column_accumulator(np.zeros(shape=(0,))),
            "ht":                           processor.column_accumulator(np.zeros(shape=(0,))),
            "lead_jet_pt":                  processor.column_accumulator(np.zeros(shape=(0,))),
            "sublead_jet_pt":               processor.column_accumulator(np.zeros(shape=(0,))),
            "njets":                        processor.column_accumulator(np.zeros(shape=(0,))),
            "bjets":                        processor.column_accumulator(np.zeros(shape=(0,))),
            "nWs":                          processor.column_accumulator(np.zeros(shape=(0,))),
            "nHs":                          processor.column_accumulator(np.zeros(shape=(0,))),
            "nFatJets":                     processor.column_accumulator(np.zeros(shape=(0,))),
            "met_significance":             processor.column_accumulator(np.zeros(shape=(0,))),
            "min_dphi_met_j4":              processor.column_accumulator(np.zeros(shape=(0,))),
            #"dR_fj1_fj2":                   processor.column_accumulator(np.zeros(shape=(0,))),
            "signal":                       processor.column_accumulator(np.zeros(shape=(0,))),

        })

    #Make sure to plug in the dataset axis and the properly binned axis you created above.
    #Cool. Now let's define some properties of the processor.
    
    @property
    
    #First is this guy. He does important things so always include him. 
    def accumulator(self):
        return self._accumulator

    def process(self, df):
        """
        Processing function. This is where the actual analysis happens.
        """
        output = self.accumulator.identity()
        dataset = df["dataset"]
        cfg = loadConfig()
        
        ## MET -> can switch to puppi MET
        met_pt  = df["MET_pt"]
        met_phi = df["MET_phi"]
        
        ## Muons
        muon = JaggedCandidateArray.candidatesfromcounts(
            df['nMuon'],
            pt = df['Muon_pt'].content,
            eta = df['Muon_eta'].content,
            phi = df['Muon_phi'].content,
            mass = df['Muon_mass'].content,
            miniPFRelIso_all=df['Muon_miniPFRelIso_all'].content,
            looseId =df['Muon_looseId'].content
            )
        muon = muon[(muon.pt > 10) & (abs(muon.eta) < 2.4) & (muon.looseId) & (muon.miniPFRelIso_all < 0.2)]
        #muon = Collections(df, "Muon", "tightTTH").get() # this needs a fix for DASK
        
        electrons = JaggedCandidateArray.candidatesfromcounts(
            df['nElectron'],
            pt=df['Electron_pt'].content, 
            eta=df['Electron_eta'].content, 
            phi=df['Electron_phi'].content,
            mass=df['Electron_mass'].content,
            pdgid=df['Electron_pdgId'].content,
            mini_iso=df['Electron_miniPFRelIso_all'].content
        )
        
        ## Electrons
        electron = JaggedCandidateArray.candidatesfromcounts(
            df['nElectron'],
            pt = df['Electron_pt'].content,
            eta = df['Electron_eta'].content,
            phi = df['Electron_phi'].content,
            mass = df['Electron_mass'].content,
            miniPFRelIso_all=df['Electron_miniPFRelIso_all'].content,
            cutBased=df['Electron_cutBased'].content
            )
        electron = electron[(electron.pt>10) & (abs(electron.eta) < 2.4) & (electron.miniPFRelIso_all < 0.1) &  (electron.cutBased >= 1)]
        #electron = Collections(df, "Electron", "tightTTH").get() # this needs a fix for DASK
        
        ## FatJets
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
        
        leadingFatJets = fatjet[:,:2]
        difatjet = leadingFatJets.choose(2)
        dphiDiFatJet = np.arccos(np.cos(difatjet.i0.phi-difatjet.i1.phi))
        
        nfatjets = fatjet.counts

        htag = fatjet[((fatjet.pt > 200) & (fatjet.deepTagMD_HbbvsQCD > 0.8365))]
        htag_hard = fatjet[((fatjet.pt > 300) & (fatjet.deepTagMD_HbbvsQCD > 0.8365))]
        
        lead_htag = htag[htag.pt.argmax()]
        
        wtag = fatjet[((fatjet.pt > 200) & (fatjet.deepTagMD_HbbvsQCD < 0.8365) & (fatjet.deepTag_WvsQCD > 0.918))]
        wtag_hard = fatjet[((fatjet.pt > 300) & (fatjet.deepTagMD_HbbvsQCD < 0.8365) & (fatjet.deepTag_WvsQCD > 0.918))]
        
        lead_wtag = wtag[wtag.pt.argmax()]
        
        wh = lead_htag.cross(lead_wtag)
        #wh_deltaPhi = np.arccos(wh.i0.phi - wh.i1.phi)
        wh_deltaR = wh.i0.p4.delta_r(wh.i1.p4)
        
        ## Jets
        jet = JaggedCandidateArray.candidatesfromcounts(
            df['nJet'],
            pt = df['Jet_pt'].content,
            eta = df['Jet_eta'].content,
            phi = df['Jet_phi'].content,
            mass = df['Jet_mass'].content,
            jetId = df['Jet_jetId'].content, # https://twiki.cern.ch/twiki/bin/view/CMS/JetID
            #puId = df['Jet_puId'].content, # https://twiki.cern.ch/twiki/bin/viewauth/CMS/PileupJetID
            btagDeepB = df['Jet_btagDeepB'].content, # https://twiki.cern.ch/twiki/bin/viewauth/CMS/BtagRecommendation102X
            #deepJet = df['Jet_'].content # not there yet?
        )
        
        jet       = jet[(jet.pt>30) & (abs(jet.eta)<2.4) & (jet.jetId>0)]
        jet       = jet[(jet.pt>30) & (jet.jetId>1) & (abs(jet.eta)<2.4)]
        jet       = jet[~jet.match(muon, deltaRCut=0.4)] # remove jets that overlap with muons
        jet       = jet[~jet.match(electron, deltaRCut=0.4)] # remove jets that overlap with electrons
        jet       = jet[jet.pt.argsort(ascending=False)] # sort the jets
        btag      = jet[(jet.btagDeepB>0.4184)]
        light     = jet[(jet.btagDeepB<0.4184)]
        
        njets     = jet.counts
        nbjets    = btag.counts

        ## Get the leading b-jets
        high_score_btag = jet[jet.btagDeepB.argsort(ascending=False)][:,:2]
        
        leadingJets = jet[:,:2]
        dijet = leadingJets.choose(2)
        dphiDiJet = np.arccos(np.cos(dijet.i0.phi-dijet.i1.phi))
        
        leading_jet = leadingJets[leadingJets.pt.argmax()]
        subleading_jet = leadingJets[leadingJets.pt.argmin()]
        leading_b      = btag[btag.pt.argmax()]

        bb = high_score_btag.choose(2)
        bb_deltaPhi = np.arccos(np.cos(bb.i0.phi-bb.i1.phi))
        bb_deltaR = bb.i0.p4.delta_r(bb.i1.p4)
        
        mtb = mt(btag.pt, btag.phi, met_pt, met_phi)
        
        ## other variables
        ht = jet.pt.sum()
        #met_sig = met_pt/np.sqrt(ht)

        min_dphiJetMet4 = np.arccos(np.cos(jet[:,:4].phi-met_phi)).min()
        #goodjcut = ((jets.pt>30) & (abs(jets.eta)<2.4) & (jets.jetid>0))
        #goodjets = jets[goodjcut]
        #abs_min_dphi_met_leadjs4 = abs(np.arccos(np.cos(goodjets[:,:4].phi-metphi)).min())
        #print(min_dphiJetMet4.shape)
        #print(self.means['min_dphi_met_j4'].shape)

        ht_ps = (ht > 0)
        met_ps = (met_pt>250)
        njet_ps = (njets >= 2)
        bjet_ps = (nbjets >= 1)
        fatjet_sel = (nfatjets >=1)


        e_sel = (electron.counts == 0)
        m_sel = (muon.counts == 0)
        #it_sel = (veto_it.counts == 0)
        #t_sel = (veto_t.counts == 0)
        l_sel = e_sel & m_sel# & it_sel & t_sel
        
        h_sel =(htag.counts > 0) 
        wmc_sel = (wtag.counts > 0) 

        
        sel = ht_ps & met_ps & njet_ps & bjet_ps & l_sel & fatjet_sel & h_sel #& wmc_sel

        met_sig = met_pt[sel]/np.sqrt(ht[sel])

        
        nEventsBaseline = len(df['weight'][sel])
        signal_label = np.ones(nEventsBaseline) if (df['dataset']=='WH') else np.zeros(nEventsBaseline)   
    
        #Let's make sure we weight our events properly.
        wght = df['weight'][sel] * 137        

        output['met']               += processor.column_accumulator(met_pt[sel].flatten())
        output['ht']                += processor.column_accumulator(ht[sel].flatten())
        output['lead_jet_pt']       += processor.column_accumulator(leading_jet[sel].pt.flatten())
        output['sublead_jet_pt']    += processor.column_accumulator(subleading_jet[sel].pt.flatten())
        output['njets']             += processor.column_accumulator(njets[sel].flatten())
        output['bjets']             += processor.column_accumulator(nbjets[sel].flatten())
        output['nWs']               += processor.column_accumulator(wtag[sel].counts.flatten())
        output['nHs']               += processor.column_accumulator(htag[sel].counts.flatten())
        output['nFatJets']          += processor.column_accumulator(fatjet[sel].counts.flatten())
        output['met_significance']  += processor.column_accumulator(met_sig.flatten())
        output['min_dphi_met_j4']   += processor.column_accumulator(min_dphiJetMet4[sel].flatten())
        #output['dR_fj1_fj2']        += processor.column_accumulator(dR_fj1_fj2[sel].flatten())
        output['signal']            += processor.column_accumulator(signal_label)


        return output

    #Remember this bad boy and we're done with this block of code!
    
    def postprocess(self, accumulator):
        return accumulator


# create empty df
df_out = {
    'met':    [],
    'ht':   [],
    'lead_jet_pt':   [],
    'sublead_jet_pt':   [],
    'njets':    [],
    'bjets':   [],
    'nWs':   [],
    'nHs':   [],
    'nFatJets':   [],
    'met_significance':   [],
    'min_dphi_met_j4':    [],
    #'dR_fj1_fj2':    [],
    #'weight':   [],
    'signal':   [],
}
#df_out = {'spectator_pt': []}


small = False
overwrite = True

# load the config and the cache
cfg = loadConfig()

from processor.samples import fileset, fileset_small#, fileset_2l, fileset_SS

if small:
    fileset = {'WH': fileset['WH'][:2]}#, 'ttbar':fileset['ttbar'][:2]} # {'tW_scattering': fileset_small['tW_scattering']}
    workers = 4
else:
    fileset = {'WH': fileset['WH'],

               #'QCD': fileset['QCD'],
               #'DY': fileset['DY'],

               'WJets': fileset['WJets'],
               'ttbar': fileset['ttbar'],
               'ST': fileset['ST'],
               'TTW': fileset['TTW'],
               'WW': fileset['WW'],

               'ZJets': fileset['ZJets'],
               'TTZ': fileset['TTZ'],
               'ZZ/WZ': fileset['ZZ/WZ'],
               }
    workers = 8

if overwrite:

    # create .h5 file
    df = pd.DataFrame(df_out)
    df.to_hdf( 'data/data_X.h5', key='df', format='table', mode='w')

    output = processor.run_uproot_job(fileset,
                                      treename='Events',
                                      processor_instance=WHhadProcessor(),
                                      executor=processor.futures_executor,
                                      executor_args={'workers': workers, 'function_args': {'flatten': False}},
                                      chunksize=500000,
                                     )
    df_out = pd.DataFrame({
            'met':              output['met'].value.flatten(),
            'ht':               output['ht'].value.flatten(),
            'lead_jet_pt':      output['lead_jet_pt'].value.flatten(),
            'sublead_jet_pt':   output['sublead_jet_pt'].value.flatten(),
            'njets':            output['njets'].value.flatten(),
            'bjets':            output['bjets'].value.flatten(),
            'nWs':              output['nWs'].value.flatten(),
            'nHs':              output['nHs'].value.flatten(),
            'nFatJets':         output['nFatJets'].value.flatten(),
            'met_significance': output['met_significance'].value.flatten(),
            'min_dphi_met_j4':  output['min_dphi_met_j4'].value.flatten(),
            #'dR_fj1_fj2':       output['dR_fj1_fj2'].value.flatten(),
            'signal':           output['signal'].value.flatten(),
        })

    df_out.to_hdf('data/data_X.h5', key='df', format='table', mode='a', append=True)

    #check = output['passedEvents']['all'] == len(pd.read_hdf('data/data_X.h5'))
    #print ("Analyzed events:", output['totalEvents']['all'])
    #print ("Check passed:", check)

test = pd.read_hdf('data/data_X.h5')

#print ("Got %s events for signal (WH)."%len(test[test['signal']==1]))
#print ("Got %s events for background (ttbar/ttW/ttZ)."%len(test[test['signal']==0]))

#len(test[test['signal']==1])
