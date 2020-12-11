import os

from dask.distributed import Client
import distributed

from Tools.condor_utils import make_htcondor_cluster

from dask.distributed import Client, progress

cluster = make_htcondor_cluster(local=False, dashboard_address=13349, disk = "4GB", memory = "3GB",)

print ("Scaling cluster at address %s now."%cluster.scheduler_address)

cluster.scale(50)

with open('scheduler_address.txt', 'w') as f:
    f.write(str(cluster.scheduler_address))

c = Client(cluster)
