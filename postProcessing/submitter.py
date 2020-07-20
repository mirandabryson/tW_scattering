
import time

from metis.Sample import DirectorySample
from metis.CondorTask import CondorTask
from metis.StatsParser import StatsParser
from metis.Utils import do_cmd

from tW_scattering.Tools.helpers import *

# load samples
import yaml
from yaml import Loader, Dumper

def GetYear(filename):
    if filename.count('Autumn18') or filename.count('Run2018'):
        return 2018
    elif filename.count('Fall17') or filename.count('Run2017'):
        return 2017
    elif filename.count('Summer16') or filename.count('Run2016'):
        return 2016
    else:
        return -1

data_path = os.path.expandvars('$CMSSW_BASE/src/tW_scattering/data/')
with open(data_path+'samples.yaml') as f:
    samples = yaml.load(f, Loader=Loader)

# define other stuff
cfg = loadConfig()


outDir = os.path.join(cfg['meta']['localSkim'], str(cfg['meta']['version']).replace('.','p'))

maker_tasks = []
merge_tasks = []



#if True:
for s in samples.keys():
    sample = DirectorySample(dataset = samples[s]['name'], location = samples[s]['path'])
    
    samplename = str(sample)

    year = GetYear(samplename)

    tag = str(cfg['meta']['version']).replace('.','p')
    
    maker_task = CondorTask(
        sample = sample,
            #'/hadoop/cms/store/user/dspitzba/nanoAOD/TTWJetsToLNu_TuneCP5_13TeV-amcatnloFXFX-madspin-pythia8__RunIIAutumn18NanoAODv6-Nano25Oct2019_102X_upgrade2018_realistic_v20_ext1-v1/',
        # open_dataset = True, flush = True,
        executable = "executable.sh",
        arguments = "%s %s %d"%(str(cfg['meta']['version']).replace('.','p'), samples[s]['xsec']/samples[s]['sumWeight'], year),
        #tarfile = "merge_scripts.tar.gz",
        files_per_output = 1,
        output_dir = os.path.join(outDir, sample.get_datasetname()),
        output_name = sample.get_datasetname() + ".root",
        output_is_tree = True,
        # check_expectedevents = True,
        tag = tag,
        condor_submit_params = {"sites":"T2_US_UCSD,UAF"},
        cmssw_version = "CMSSW_10_2_9",
        scram_arch = "slc6_amd64_gcc700",
        # recopy_inputs = True,
        # no_load_from_backup = True,
        min_completion_fraction = 0.99,
    )
    
    maker_tasks.append(maker_task)



#if False:
    merge_task = CondorTask(
        sample = DirectorySample(
            dataset="merge_"+sample.get_datasetname(),
            location=maker_task.get_outputdir(),
        ),
        # open_dataset = True, flush = True,
        executable = "merge_executable.sh",
        arguments = "%s %s"%(str(cfg['meta']['version']).replace('.','p'), samples[s]['xsec']/samples[s]['sumWeight']),
        #tarfile = "merge_scripts.tar.gz",
        files_per_output = 100000,
        output_dir = maker_task.get_outputdir() + "/merged",
        output_name = sample.get_datasetname() + ".root",
        output_is_tree = True,
        # check_expectedevents = True,
        tag = tag,
        # condor_submit_params = {"sites":"T2_US_UCSD"},
        # cmssw_version = "CMSSW_9_2_8",
        # scram_arch = "slc6_amd64_gcc530",
        condor_submit_params = {"sites":"T2_US_UCSD,UAF"},
        cmssw_version = "CMSSW_10_2_9",
        scram_arch = "slc6_amd64_gcc700",
        # recopy_inputs = True,
        # no_load_from_backup = True,
        min_completion_fraction = 0.99,
    )

    merge_tasks.append(merge_task)

if True:
    for i in range(100):
        total_summary = {}
    
        for maker_task, merge_task in zip(maker_tasks,merge_tasks):
        #for maker_task in maker_tasks:
            maker_task.process()
    
            frac = maker_task.complete(return_fraction=True)
            if frac >= maker_task.min_completion_fraction:
            # if maker_task.complete():
                do_cmd("mkdir -p {}/merged".format(maker_task.get_outputdir()))
                do_cmd("mkdir -p {}/skimmed".format(maker_task.get_outputdir()))
                merge_task.reset_io_mapping()
                merge_task.update_mapping()
                merge_task.process()
    
            total_summary[maker_task.get_sample().get_datasetname()] = maker_task.get_task_summary()
            total_summary[merge_task.get_sample().get_datasetname()] = merge_task.get_task_summary()
 
        print (frac)
   
        # parse the total summary and write out the dashboard
        StatsParser(data=total_summary, webdir="~/public_html/dump/metis_tW_scattering/").do()
    
        # 15 min power nap
        time.sleep(15.*60)



