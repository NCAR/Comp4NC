import logging
import os
from time import sleep, time

import click
import dask.array as da
import numpy as np
import xarray as xr
from dask_jobqueue import PBSCluster, SLURMCluster
from distributed import Client
from numcodecs import Blosc

logger = logging.getLogger()
logger.setLevel(level=logging.WARNING)

here = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
results_dir = os.path.join(here, 'results')


def cluster_wait(client, n_workers):
    """ Delay process until all workers in the cluster are available.
    """
    start = time()
    wait_thresh = 600
    worker_thresh = n_workers * 0.95

    while len(client.cluster.scheduler.workers) < n_workers:
        sleep(2)
        elapsed = time() - start
        # If we are getting close to timeout but cluster is mostly available,
        # just break out
        if elapsed > wait_thresh and len(client.cluster.scheduler.workers) >= worker_thresh:
            break


class Runner:
    def __init__(self, input_file):
        import yaml

        try:
            with open(input_file) as f:
                self.params = yaml.safe_load(f)
        except Exception as exc:
            raise exc
        self.client = None

    def create_cluster(self, queue, maxcore, memory, wpn, walltime):
        cluster = PBSCluster(
            queue=queue,
            cores=maxcore,
            memory=memory,
            processes=wpn,
            local_directory='$TMPDIR',
            walltime=walltime,
            # resource_spec='select=1:ncpus=36:mem=109GB',
        )
        logger.warning(memory)
        logger.warning(walltime)
        # resource_spec='select=1:ncpus=36:mem=109GB')
        logger.warning(cluster.job_script())
        self.client = Client(cluster)

    def run(self):
        logger.warning('Reading configuration YAML config file')
        queue = self.params['queue']
        walltime = self.params['walltime']
        memory = self.params['memory']
        maxcore_per_node = self.params['maxcore_per_node']
        num_workers = self.params['number_of_workers_per_nodes']
        num_nodes = self.params['number_of_nodes']
        input_file = self.params['input_dir']
        output_dir = self.params['output_dir']
        logger.warning(memory)
        logger.warning(maxcore_per_node)
        logger.warning(num_nodes)
        logger.warning(num_workers)
        logger.warning(input_file)
        logger.warning(output_dir)
        logger.warning(walltime)
        self.create_cluster(queue, maxcore_per_node, memory, num_workers, walltime)
        self.client.cluster.scale(n=num_nodes * num_workers)
        logger.warning('scale')
        # sleep(10)
        self.client.wait_for_workers(num_nodes * num_workers)
        logger.warning('wait')
        # cluster_wait(self.client, num_nodes * num_workers)

        ds = xr.open_dataset(
            input_file, chunks={'nVertLevels': 1, 'nCells': 2621442, 'nVertLevelsP1': 1}
        )

        logger.warning(ds)
        logger.warning('done')
        self.client.cluster.close()
        self.client.close()
        return ds


@click.command()
@click.argument('config_file', type=click.Path(exists=True))
def cli(config_file):
    runner = Runner(config_file)
    runner.run()


if __name__ == '__main__':
    cli()
