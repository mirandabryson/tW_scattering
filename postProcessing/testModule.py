## git clone --branch MBv93 --depth 1  https://github.com/mirandabryson/nanoAOD-tools.git PhysicsTools/NanoAODTools

from importlib import import_module
from PhysicsTools.NanoAODTools.postprocessing.framework.postprocessor   import PostProcessor
from PhysicsTools.NanoAODTools.postprocessing.framework.datamodel       import Collection
from PhysicsTools.NanoAODTools.postprocessing.framework.eventloop       import Module
from PhysicsTools.NanoAODTools.postprocessing.modules.tW_scattering.ObjectSelection import *
from PhysicsTools.NanoAODTools.postprocessing.modules.tW_scattering.GenAnalyzer import *
from PhysicsTools.NanoAODTools.postprocessing.modules.tW_scattering.lumiWeightProducer import *

from PhysicsTools.NanoAODTools.postprocessing.modules.tW_scattering.helpers import *         
#json support to be added                                                              
year = 2016                     

selector = chooseselector(year)                                                             


modules = [\
    selector(),                                               
#    genAnalyzer()                                                      
    ]     


# apply PV requirement
cut  = 'PV_ndof>4 && sqrt(PV_x*PV_x+PV_y*PV_y)<=2 && abs(PV_z)<=24'
# loose skim
cut += '&& MET_pt>200'
cut += '&& Sum$(Jet_pt>30&&abs(Jet_eta<2.4))>=2'

files = ["/hadoop/cms/store/user/mbryson/WH_hadronic/background/TTJets_SingleLeptFromTbar_TuneCUETP8M1_13TeV-madgraphMLM-pythia8__RunIISummer16NanoAODv7-PUMoriond17_Nano02Apr2020_102X_mcRun2_asymptotic_v8-v1/1031345D-38C2-F448-B44E-61CBDF528ECD.root"]

p = PostProcessor('./', files, cut=cut, modules=modules)
#    branchsel='PhysicsTools/NanoAODTools/python/postprocessing/modules/tW_scattering/keep_and_drop_in.txt',\
#    outputbranchsel='PhysicsTools/NanoAODTools/python/postprocessing/modules/tW_scattering/keep_and_drop.txt' )
p.run()
