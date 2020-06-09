import os

from importlib import import_module
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor   import PostProcessor
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel       import Collection
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop       import Module

from PhysicsTools.NanoAODTools.postprocessing.modules.tW_scattering.ObjectSelection import *
from PhysicsTools.NanoAODTools.postprocessing.modules.tW_scattering.GenAnalyzer import *
from PhysicsTools.NanoAODTools.postprocessing.modules.tW_scattering.lumiWeightProducer import *

#json support to be added

modules = [\
    lumiWeightProd(0.5), #some dummy value
    selector2018(),
    GenAnalyzer(),
    ]

# apply PV requirement
cut  = 'PV_ndof>4 && sqrt(PV_x*PV_x+PV_y*PV_y)<=2 && abs(PV_z)<=24'
# loose skim
cut += '&& MET_pt>200'
cut += ' && Sum$(Jet_pt>30&&abs(Jet_eta)<2.4)>=2'

testFile = '/hadoop/cms/store/user/mibryson/WH_hadronic/WH_had_750_1/test/WH_hadronic_nanoAOD_500.root'
p = PostProcessor('./', [testFile], cut=cut, modules=modules, maxEntries=100,\
    branchsel=os.path.expandvars('$CMSSW_BASE/src/NanoAODTools/python/postprocessing/modules/tW_scattering/keep_and_drop_in.txt'),\
    outputbranchsel=os.path.expandvars('$CMSSW_BASE/src/NanoAODTools/python/postprocessing/modules/tW_scattering/keep_and_drop.txt') )

p.run()
