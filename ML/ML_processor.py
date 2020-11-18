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

from Tools.helpers import loadConfig, getCutFlowTable, mergeArray
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
        '''self._accumulator = processor.dict_accumulator({
            'ttbar':            processor.defaultdict_accumulator(int),
            'TTW/TTZ':          processor.defaultdict_accumulator(int),
            'WH':               processor.defaultdict_accumulator(int),
            #'totalEvents':      processor.defaultdict_accumulator(int),
            #'passedEvents':     processor.defaultdict_accumulator(int),
        })'''
        
        self._accumulator = processor.dict_accumulator({
            "met":                          processor.column_accumulator(np.zeros(shape=(0,))),
            "ht":                           processor.column_accumulator(np.zeros(shape=(0,))),
            "lead_jet_pt":                  processor.column_accumulator(np.zeros(shape=(0,))),
            "sublead_jet_pt":               processor.column_accumulator(np.zeros(shape=(0,))),
            "lead_jet_eta":                 processor.column_accumulator(np.zeros(shape=(0,))),
            "sublead_jet_eta":              processor.column_accumulator(np.zeros(shape=(0,))),
            "njets":                        processor.column_accumulator(np.zeros(shape=(0,))),
            "bjets":                        processor.column_accumulator(np.zeros(shape=(0,))),
            "min_dphi_met_j1":              processor.column_accumulator(np.zeros(shape=(0,))),
            "min_dphi_met_j2":              processor.column_accumulator(np.zeros(shape=(0,))),
            "min_dphi_met_j3":              processor.column_accumulator(np.zeros(shape=(0,))),
            "min_dphi_met_j4":              processor.column_accumulator(np.zeros(shape=(0,))),
            #"dphi_j1_j2":                   hist.Hist
            #"dphi_fj1_fj2":                 hist.Hist
            #"dR_fj1_fj2":                   hist.Hist("Counts", dataset_axis, r_axis),
            #"m_FatJet_softdrop":            hist.Hist("Counts", dataset_axis, mass_axis),
            "signal":                       processor.column_accumulator(np.zeros(shape=(0,))),

        })

    #Make sure to plug in the dataset axis and the properly binned axis you created above.
    #Cool. Now let's define some properties of the processor.
    
    @property
    
    #First is this guy. He does important things so always include him. 
    def accumulator(self):
        return self._accumulator

    #Now comes the fun part. Here's where we tell our processor exactly what to do with the data.
    def process(self, df):
     
        #Make sure to declare your output, which stores everything you put into the histograms.
        output = self.accumulator.identity()
        
        #Load your data for the dataset axis.
        dataset = df['dataset']

        #Let's define some variables from our dataset, starting with MET.
        metphi = df["MET_phi"]
        metpt = df["MET_pt"]
        #Here, I'm simply calling those nanoaod branches from the samples
        #and storing them under easy to access variable names. 
        
      
        
        #Let's define some 4 vector objects. For these I can call the branches whatever 
        #I want. Just make sure to include the .content at the end. Also, by making these
        #objects, we can call the branches in a pretty easy way. Shown below.
        
        #Leptons
        electrons = JaggedCandidateArray.candidatesfromcounts(
            df['nElectron'],
            pt=df['Electron_pt'].content, 
            eta=df['Electron_eta'].content, 
            phi=df['Electron_phi'].content,
            mass=df['Electron_mass'].content,
            pdgid=df['Electron_pdgId'].content,
            mini_iso=df['Electron_miniPFRelIso_all'].content
        )
        

        muons = JaggedCandidateArray.candidatesfromcounts(
            df['nMuon'],
            pt=df['Muon_pt'].content, 
            eta=df['Muon_eta'].content, 
            phi=df['Muon_phi'].content,
            mass=df['Muon_mass'].content,
            pdgid=df['Muon_pdgId'].content,
            mini_iso=df['Muon_miniPFRelIso_all'].content, 
            looseid =df['Muon_looseId'].content
        )
        
        taus = JaggedCandidateArray.candidatesfromcounts(
            df['nTau'],
            pt=df['Tau_pt'].content, 
            eta=df['Tau_eta'].content, 
            phi=df['Tau_phi'].content,
            mass=df['Tau_mass'].content,
            decaymode=df['Tau_idDecayMode'].content,
            newid=df['Tau_idMVAnewDM2017v2'].content,
        )
        
        #Here, since I don't have enough information to form a 4 vector with isotracks,
        #I just use the JaggedArray format. I call branches in the same way as the
        #JaggedCandidateArray, but I can't use some of the manipulations that come with the
        #JCA format. :(
        isotracks = awkward.JaggedArray.zip(
            pt=df['IsoTrack_pt'], 
            eta=df['IsoTrack_eta'], 
            phi=df['IsoTrack_phi'], 
            rel_iso=df['IsoTrack_pfRelIso03_all'], 
        )
        
        #Jets
        jets = JaggedCandidateArray.candidatesfromcounts(
            df['nJet'],
            pt=df['Jet_pt'].content, 
            eta=df['Jet_eta'].content, 
            phi=df['Jet_phi'].content,
            btag=df['Jet_btagDeepB'].content, 
            jetid=df['Jet_jetId'].content, 
            mass=df['Jet_mass'].content,
        )
        fatjets = JaggedCandidateArray.candidatesfromcounts(
            df['nFatJet'],
            pt=df['FatJet_pt'].content, 
            eta=df['FatJet_eta'].content, 
            phi=df['FatJet_phi'].content, 
            mass=df['FatJet_mass'].content, 
            softdrop=df["FatJet_msoftdrop"].content,  
            fromH = df['FatJet_deepTagMD_HbbvsQCD'].content, 
            fromW_MD = df['FatJet_deepTagMD_WvsQCD'].content, 
            fromW_MC = df['FatJet_deepTag_WvsQCD'].content
        )
  
        
        #Now let's deal with some good ol' ak4's baby. Let's define a "good jet".
        #First, let's define what a good jet should be. Notice how I'm calling the branches
        #of the jets. Super easy, right?
        goodjcut = ((jets.pt>30) & (abs(jets.eta)<2.4) & (jets.jetid>0))
        #Perfect, now let's apply this selection to the ak4's and create a new object.
        goodjets = jets[goodjcut]
        #LIT. Okay, now I want the number of good jets. 
        njets = goodjets.counts
        #Bro, you are on fire. Good job. I'm proud of you and really appreciate you.

        jetpt_sorted = goodjets.pt.argsort(ascending=False)
        leadjet = goodjets[jetpt_sorted==0]
        subleadjet = goodjets[jetpt_sorted==1]
        leadjet_subleadjet = leadjet.cross(subleadjet)
        #leadjets = goodjets[jetpt_sorted <= 1]
        #leadjets3 = goodjets[jetpt_sorted <= 2]
        #leadjets4 = goodjets[jetpt_sorted <= 4]
      
        #ak8's
        goodfjcut = ((fatjets.pt > 200))
        goodfatjets = fatjets[goodfjcut]
        
        htagcut = ((fatjets.pt > 200) & (fatjets.fromH > 0.8365))
        htagged = fatjets[htagcut]
        
        wtagcut_mc = ((fatjets.pt > 200) & (fatjets.fromW_MC > 0.918) & (fatjets.fromH < 0.8365))
        wtagcut_md = ((fatjets.pt > 200) & (fatjets.fromW_MD > 0.704) & (fatjets.fromH < 0.8365))
        wtagged_mc = fatjets[wtagcut_mc]
        wtagged_md = fatjets[wtagcut_md]

        fatjet_sorted = goodfatjets.pt.argsort(ascending=False)
        leadFatJet    = goodfatjets[fatjet_sorted==0]
        subleadFatJet = goodfatjets[fatjet_sorted==1]
        lead_sublead_FatJets = leadFatJet.cross(subleadFatJet)
        
        #m_lead_FatJet_softdrop = goodfatjets[goodfatjets.pt.argmax()]
    
        #Let's make some b-jets and find the number of b-jets.
        bjcut = ((jets.pt>30) & (abs(jets.eta)<2.4) & (jets.jetid>0) & (jets.btag>0.4184))
        bjets = jets[bjcut]
        nbjets = bjets.counts
        #Hell yeah. 
        
        #Let's go for HT now. 
        ht = goodjets.pt.sum()
        #Remember to put that () after the sum!
          
        #dphi_met_leadjs3 = abs((leadjets3.phi - metphi + np.pi) % (2 * np.pi) - np.pi)
        #sorted_dphi_met_leadjs3 = dphi_met_leadjs3.argsort(ascending=True)
        #min_dphi_met_leadjs3 = dphi_met_leadjs3[sorted_dphi_met_leadjs3==0]
        #abs_min_dphi_met_leadjs3 = abs(min_dphi_met_leadjs3)
       
        abs_min_dphi_met_leadjs1 = np.arccos(np.cos(goodjets[:,:1].phi-metphi)).min()
        abs_min_dphi_met_leadjs2 = np.arccos(np.cos(goodjets[:,:2].phi-metphi)).min()
        abs_min_dphi_met_leadjs3 = np.arccos(np.cos(goodjets[:,:3].phi-metphi)).min()
        abs_min_dphi_met_leadjs4 = np.arccos(np.cos(goodjets[:,:4].phi-metphi)).min()

        abs_dphi_j1_j2           = abs(leadjet_subleadjet.i0.p4.delta_phi(leadjet_subleadjet.i1.p4))
        abs_dphi_fj1_fj2         = abs(lead_sublead_FatJets.i0.p4.delta_phi(lead_sublead_FatJets.i1.p4))

        dR_fj1_fj2               = lead_sublead_FatJets.i0.p4.delta_r(lead_sublead_FatJets.i1.p4)

        #Let's define lepton vetos using the same method.
        
        veto_e_cut = (electrons.pt>5) & (abs(electrons.eta) < 2.4) & (electrons.mini_iso < 0.2)
        veto_e = electrons[veto_e_cut]
        
        veto_m_cut = (muons.pt > 5) & (abs(muons.eta) < 2.4) & (muons.looseid) & (muons.mini_iso < 0.2)
        veto_m = muons[veto_m_cut]
        
        veto_t_cut = (taus.pt > 20) & (abs(taus.eta) < 2.4) & (taus.decaymode) & (taus.newid >= 8)
        veto_t = taus[veto_t_cut]
        
        veto_it_cut = (isotracks.pt > 10) & (abs(isotracks.eta) < 2.4) & ((isotracks.rel_iso < (0.1*isotracks.pt)) | (isotracks.rel_iso < 6))
        veto_it = isotracks[veto_it_cut]
        
       
       
        #Now it's time to make some selections. I'm going to guess that you can follow
        #what I'm doing from here. 

        ht_ps = (ht > 300)
        met_ps = (metpt>250)
        njet_ps = (njets >= 2)
        bjet_ps = (nbjets >= 1)


        e_sel = (veto_e.counts == 0)
        m_sel = (veto_m.counts == 0)
        it_sel = (veto_it.counts == 0)
        t_sel = (veto_t.counts == 0)
        l_sel = e_sel & m_sel & it_sel & t_sel
        
        h_sel =(htagged.counts > 0) 
        wmc_sel = (wtagged_mc.counts > 0) 

        
        sel = met_ps & njet_ps & bjet_ps & l_sel #& wmc_sel & h_sel

        
        nEventsBaseline = len(df['weight'][sel])
        signal_label = np.ones(nEventsBaseline) if (df['dataset']=='WH') else np.zeros(nEventsBaseline)   
    
        #Let's make sure we weight our events properly.
        wght = df['weight'][sel] * 137        

        output['met']               += processor.column_accumulator(metpt[sel].flatten())
        output['ht']                += processor.column_accumulator(ht[sel].flatten())
        output['lead_jet_pt']       += processor.column_accumulator(leadjet[sel].pt.flatten())
        output['sublead_jet_pt']    += processor.column_accumulator(subleadjet[sel].pt.flatten())
        output['lead_jet_eta']      += processor.column_accumulator(leadjet[sel].eta.flatten())
        output['sublead_jet_eta']   += processor.column_accumulator(subleadjet[sel].eta.flatten())
        output['njets']             += processor.column_accumulator(njets[sel].flatten())
        output['bjets']             += processor.column_accumulator(nbjets[sel].flatten())
        output['min_dphi_met_j1']   += processor.column_accumulator(abs_min_dphi_met_leadjs1[sel].flatten())
        output['min_dphi_met_j2']   += processor.column_accumulator(abs_min_dphi_met_leadjs2[sel].flatten())
        output['min_dphi_met_j3']   += processor.column_accumulator(abs_min_dphi_met_leadjs3[sel].flatten())
        output['min_dphi_met_j4']   += processor.column_accumulator(abs_min_dphi_met_leadjs4[sel].flatten())
        output['signal']            += processor.column_accumulator(signal_label)

        '''df_out = pd.DataFrame({
            'met':              metpt[sel].flatten(),
            'ht':               ht[sel].flatten(),
            'njets':            njets[sel].flatten(),
            'bjets':            nbjets[sel].flatten(),
            'min_dphi_met_j1':  abs_min_dphi_met_leadjs1[sel].flatten(),
            'min_dphi_met_j2':  abs_min_dphi_met_leadjs2[sel].flatten(),
            'min_dphi_met_j3':  abs_min_dphi_met_leadjs3[sel].flatten(),
            'min_dphi_met_j4':  abs_min_dphi_met_leadjs4[sel].flatten()#,
            #'dphi_j1_j2':       abs_dphi_j1_j2[sel].flatten(),
            #'dphi_fj1_fj2':     abs_dphi_fj1_fj2[sel].flatten(),
            #'dR_fj1_fj2':       dR_fj1_fj2[sel].flatten(),
            #'signal':           signal_label,
            #'weight':           df['weight'][sel]
        })
        df_out.to_hdf('data/data_X.h5', key='df', format='table', mode='a', append=True)'''

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
    'lead_jet_eta':   [],
    'sublead_jet_eta':   [],
    'njets':    [],
    'bjets':   [],
    'min_dphi_met_j1':    [],
    'min_dphi_met_j2':    [],
    'min_dphi_met_j3':    [],
    'min_dphi_met_j4':    [],
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
               'ttbar': fileset['ttbar'],
               'TTW/TTZ': fileset['TTW/TTZ'],
               'QCD': fileset['QCD'],
               'WJets': fileset['WJets'],
               'ZJets': fileset['ZJets'],
               'DY': fileset['DY'],
               'diboson': fileset['diboson']}
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
            'lead_jet_eta':     output['lead_jet_eta'].value.flatten(),
            'sublead_jet_eta':  output['sublead_jet_eta'].value.flatten(),
            'njets':            output['njets'].value.flatten(),
            'bjets':            output['bjets'].value.flatten(),
            'min_dphi_met_j1':  output['min_dphi_met_j1'].value.flatten(),
            'min_dphi_met_j2':  output['min_dphi_met_j2'].value.flatten(),
            'min_dphi_met_j3':  output['min_dphi_met_j3'].value.flatten(),
            'min_dphi_met_j4':  output['min_dphi_met_j4'].value.flatten(),
            #'dphi_j1_j2':       abs_dphi_j1_j2[sel].flatten(),
            #'dphi_fj1_fj2':     abs_dphi_fj1_fj2[sel].flatten(),
            #'dR_fj1_fj2':       dR_fj1_fj2[sel].flatten(),
            'signal':           output['signal'].value.flatten()
            #'weight':           df['weight'][sel]
        })

    df_out.to_hdf('data/data_X.h5', key='df', format='table', mode='a', append=True)

    #check = output['passedEvents']['all'] == len(pd.read_hdf('data/data_X.h5'))
    #print ("Analyzed events:", output['totalEvents']['all'])
    #print ("Check passed:", check)

test = pd.read_hdf('data/data_X.h5')

#print ("Got %s events for signal (WH)."%len(test[test['signal']==1]))
#print ("Got %s events for background (ttbar/ttW/ttZ)."%len(test[test['signal']==0]))

#len(test[test['signal']==1])
