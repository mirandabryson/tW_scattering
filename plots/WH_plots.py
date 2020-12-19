'''
small script that reades histograms from an archive and saves figures in a public space

ToDo:
[x] Cosmetics (labels etc)
[x] ratio pad!
  [x] pseudo data
    [ ] -> move to processor to avoid drawing toys every time!
[x] uncertainty band
[ ] fix shapes
'''


from coffea import hist
import pandas as pd
import os

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm
import numpy as np

from klepto.archives import dir_archive

# import all the colors and tools for plotting
from Tools.helpers import loadConfig
from helpers import *

# load the configuration
cfg = loadConfig()

year = 2017

if year == 2016:
    lumi = 35.9
elif year == 2017:
    lumi = 41.5
elif year == 2018:
    lumi = 60.0

# load the results
cache = dir_archive(os.path.join(os.path.expandvars(cfg['caches']['base']), 'WH_LL_%s'%year), serialized=True)
#cache = dir_archive(os.path.join(os.path.expandvars(cfg['caches']['base']), cfg['caches']['WH_small']), serialized=True)
cache.load()

histograms = cache.get('histograms')
output = cache.get('simple_output')
plotDir = os.path.expandvars(cfg['meta']['plots']) + '/plots_WH_LL_SF_W_%s/'%year
finalizePlotDir(plotDir)

if not histograms:
    print ("Couldn't find histograms in archive. Quitting.")
    exit()

print ("Plots will appear here:", plotDir )

bins = {\
    'met_baseline':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T}^{miss}\ (GeV)$', 20, 0, 800)},
    'met_CR':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T}^{miss}\ (GeV)$', 20, 0, 800)},
    'met_VR':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T}^{miss}\ (GeV)$', 7, 0, 700)},
    'met_W_CR':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T}^{miss}\ (GeV)$', 7, 0, 700)},
    'met_Higgs_CR':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T}^{miss}\ (GeV)$', 7, 0, 700)},
    'met_Higgs_W_CR':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T}^{miss}\ (GeV)$', 7, 0, 700)},
    'N_AK4_baseline':   {'axis': 'multiplicity',      'overflow':'over',  'bins': hist.Bin('pt', r'$N_{AK4}$', 10, -0.5, 9.5)},
    #'N_AK4_extra_baseline':   {'axis': 'multiplicity',      'overflow':'over',  'bins': hist.Bin('pt', r'$N_{AK4} (cleaned)$', 10, -0.5, 9.5)},
    'N_AK8_baseline':   {'axis': 'multiplicity',      'overflow':'over',  'bins': hist.Bin('pt', r'$N_{AK8}$', 10, -0.5, 9.5)},
    'N_AK8_CR':   {'axis': 'multiplicity',      'overflow':'over',  'bins': hist.Bin('pt', r'$N_{AK8}$', 10, -0.5, 9.5)},
    'N_W_CR':   {'axis': 'multiplicity',      'overflow':'over',  'bins': hist.Bin('pt', r'$N_{W}$', 5, -0.5, 4.5)},
    'N_H_CR':   {'axis': 'multiplicity',      'overflow':'over',  'bins': hist.Bin('pt', r'$N_{H}$', 5, -0.5, 4.5)},
    'min_dphiFatJetMet4':   {'axis': 'delta',      'overflow':'over',  'bins': hist.Bin('delta', r'$min \Delta \varphi(AK8, MET)$', 10, 0, 5)},
    'dphiDiFatJet':   {'axis': 'delta',      'overflow':'over',  'bins': hist.Bin('delta', r'$\Delta \varphi(AK8)$', 10, 0, 5)},
    'W_pt_W_CR':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T} (W)\ (GeV)$', 7, 0, 700)},
    'H_pt_Higgs_CR':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T} (H)\ (GeV)$', 7, 0, 700)},

    'lead_AK8_pt':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T} (lead AK8)\ (GeV)$', 7, 0, 700)},
    'lead_AK8_eta':   {'axis': 'eta',      'overflow':'over',  'bins': hist.Bin('eta', r'$\eta (lead AK8)\ (GeV)$', 15, -5.5, 5.5)},
    'lead_AK8_mass':   {'axis': 'mass',      'overflow':'over',  'bins': hist.Bin('mass', r'$M (lead AK8)\ (GeV)$', 25, 0, 250)},
    'lead_AK8_msoftdrop':   {'axis': 'mass',      'overflow':'over',  'bins': hist.Bin('mass', r'$M_{SD} (lead AK8)\ (GeV)$', 25, 0, 250)},
    'lead_AK8_Hscore':   {'axis': 'score',      'overflow':'over',  'bins': hist.Bin('score', 'Hbb score (lead AK8)', 25, 0, 1)},
    'lead_AK8_Wscore':   {'axis': 'score',      'overflow':'over',  'bins': hist.Bin('score', 'Wqq score (lead AK8)', 25, 0, 1)},

    'sublead_AK8_pt':   {'axis': 'pt',      'overflow':'over',  'bins': hist.Bin('pt', r'$p_{T} (sublead AK8)\ (GeV)$', 7, 0, 700)},
    'sublead_AK8_eta':   {'axis': 'eta',      'overflow':'over',  'bins': hist.Bin('eta', r'$\eta (sublead AK8)\ (GeV)$', 15, -5.5, 5.5)},
    'sublead_AK8_mass':   {'axis': 'mass',      'overflow':'over',  'bins': hist.Bin('mass', r'$M (sublead AK8)\ (GeV)$', 25, 0, 250)},
    'sublead_AK8_msoftdrop':   {'axis': 'mass',      'overflow':'over',  'bins': hist.Bin('mass', r'$M_{SD} (sublead AK8)\ (GeV)$', 25, 0, 250)},
    'sublead_AK8_Hscore':   {'axis': 'score',      'overflow':'over',  'bins': hist.Bin('score', 'Hbb score (sublead AK8)', 25, 0, 1)},
    'sublead_AK8_Wscore':   {'axis': 'score',      'overflow':'over',  'bins': hist.Bin('score', 'Wqq score (sublead AK8)', 25, 0, 1)},
    }

separateSignal = False
scaleSignal = 0
useData = True

#import mplhep
#plt.style.use(mplhep.style.CMS)

for name in bins:
    print (name)
    skip = False
    histogram = output[name]
    
    if not name in bins.keys():
        continue

    axis = bins[name]['axis']
    print (name, axis)
    histogram = histogram.rebin(axis, bins[name]['bins'])

    y_max = histogram.sum("dataset").values(overflow='over')[()].max()
    y_over = histogram.sum("dataset").values(overflow='over')[()][-1]


    signal = 'mC750_l1'
    #processes = ['QCD', 'ZNuNu', 'WW', 'ttW', 'ST', 'WJets', 'TTJets']
    processes = ['QCD', 'ZNuNu', 'WW', 'ST', 'WJets', 'TTJets']
    import re
    #notdata = re.compile('(?!(Data|mC750))')
    notdata = re.compile('(?!(Data))')

    notsignal = re.compile('(?!%s)'%signal)

    if useData:
        fig, (ax, rax) = plt.subplots(2, 1, figsize=(7,7), gridspec_kw={"height_ratios": (3, 1)}, sharex=True)
    else:
        fig, ax = plt.subplots(1,1,figsize=(7,7))

    # get axes
    if useData:
        hist.plot1d(histogram[notdata], overlay="dataset", ax=ax, stack=True, overflow=bins[name]['overflow'], fill_opts=fill_opts, error_opts=error_opts, order=processes)
        #hist.plot1d(histogram['QCD'], overlay="dataset", ax=ax, stack=False, overflow=bins[name]['overflow'], clear=False, line_opts=None, fill_opts=fill_opts, error_opts=error_opts, order=processes)
        hist.plot1d(histogram['Data'], overlay="dataset", ax=ax, overflow=bins[name]['overflow'], error_opts=data_err_opts, clear=False)
        #hist.plot1d(histogram[signal], overlay="dataset", ax=ax, overflow=bins[name]['overflow'], line_opts={'linewidth':3}, clear=False)

    if useData:
        # build ratio
        hist.plotratio(
            num=histogram['Data'].sum("dataset"),
            denom=histogram[notdata].sum("dataset"),
            ax=rax,
            error_opts=data_err_opts,
            denom_fill_opts={},
            guide_opts={},
            unc='num',
            overflow=bins[name]['overflow']
        )


    for l in ['linear', 'log']:
        if useData:
            saveFig(fig, ax, rax, plotDir, name, scale=l, shape=False, y_max=y_max, preliminary='Preliminary', lumi=lumi)
        else:
            saveFig(fig, ax, None, plotDir, name, scale=l, shape=False, y_max=y_max)
    fig.clear()
    if useData:
        rax.clear()
    ax.clear()

    if False:
        try:
            fig, ax = plt.subplots(1,1,figsize=(7,7))
            notdata = re.compile('(?!pseudodata|wjets|diboson)')
            hist.plot1d(histogram[notdata],overlay="dataset", density=True, stack=False, overflow=bins[name]['overflow'], ax=ax) # make density plots because we don't care about x-sec differences
            for l in ['linear', 'log']:
                saveFig(fig, ax, None, plotDir, name+'_shape', scale=l, shape=True)
            fig.clear()
            ax.clear()
        except ValueError:
            print ("Can't make shape plot for a weird reason")

    fig.clear()
    ax.clear()

    plt.close('all')

    #break

print ()
print ("Plots are here: http://uaf-10.t2.ucsd.edu/~%s/"%os.path.expandvars('$USER')+str(plotDir.split('public_html')[-1]) )
