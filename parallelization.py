#!/usr/bin/python
# MDreader
# Copyright (c) Manuel Nuno Melo (m.n.melo@rug.nl)
#
# Released under the GNU Public Licence, v2 or any higher version
#
import multiprocessing
import os

# Helper functions #####################################################
########################################################################

def _parallel_launcher(rdr, w_id):
    """ Helper function for the parallel execution of registered functions.

    """
    rdr.p.id = w_id
    return rdr._reader()

def _parallel_extractor(rdr, w_id):
    """ Helper function for the parallel extraction of trajectory coordinates/values.

    """
    # block seems to be faster.
    rdr.p.mode = 'block'
    rdr.p.id = w_id
    return rdr._extractor()

# Parallelization Classes ##############################################
########################################################################

class Pool():
    """ MDAnalysis and multiprocessing's map don't play along because of pickling. This solution seems to work fine.

    """
    def __init__(self, processes):
        self.nprocs = processes

    def map(self, f, argtuple):
        procs = []
        nargs = len(argtuple)
        result = [None]*nargs
        arglist = list(argtuple)
        self.outqueue = multiprocessing.Queue()
        freeprocs = self.nprocs
        num = 0
        got = 0
        while arglist:
            while arglist and freeprocs:
                procs.append(multiprocessing.Process(target=self.fcaller, args=((f, arglist.pop(0), num) )))
                num += 1
                freeprocs -= 1
                # procs[-1].daemon = True
                procs[-1].start()
            i, r = self.outqueue.get() # Execution halts here waiting for output after filling the procs.
            result[i] = r
            got += 1
            freeprocs += 1
        # Must wait for remaining procs, otherwise we'll miss their output.
        while got < nargs:
            i, r = self.outqueue.get()
            result[i] = r
            got += 1
        for proc in procs:
            proc.terminate()
        return result

    def fcaller(self, f, args, num):
        self.outqueue.put((num, f(*args)))

class Parallel():
    def __init__(self, mpi_keep_workers_alive=False):
        self.parallel = False  # Whether to parallelize
        self.smp = False  # SMP parallelization (within the same machine, or virtual machine)
        self.p_mpi = False  # MPI parallelization
        self.mode = 'block'
        self.overlap = 0
        self.num = None
        self.id = 0
        self.scale_dt = True
        self.parms_set = False
        # Check whether we're running under MPI. Not failsafe, but the user should know better than to fudge with these env vars.
        mpivarlst = ['PMI_RANK', 'OMPI_COMM_WORLD_RANK', 'OMPI_MCA_ns_nds_vpid',
                     'PMI_ID', 'SLURM_PROCID', 'LAMRANK', 'MPI_RANKID',
                     'MP_CHILD', 'MP_RANK', 'MPIRUN_RANK']
        self.mpi = bool(sum([var in os.environ.keys() for var in mpivarlst]))
        self.mpi_keep_workers_alive = mpi_keep_workers_alive

        if self.mpi:
            # The idea here is to use multiprocessing on the home node, and MPI everywhere else. This way we can share home memory properly. Since it'll be mostly write-only there's no need for synchronization.
            from mpi4py import MPI
            self.comm = MPI.COMM_WORLD
            self.id = self.comm.Get_rank()
            self.num = self.comm.Get_size()
            hn = socket.gethostname()
            hnhash = int("".join([hex(ord(l))[2:] for l in hn]),16)
            lcomm = self.comm.Split(hnhash, self.id) #local to a node, not really useful, except to find out the local rank and root.
            self.l_id = lcomm.Get_rank()
            self.lroot_id = lcomm.bcast(self.id)
            lcomm.Free()

            self.is_xcomm = int(bool(self.lroot_id) or self.is_root) #This is the proper MPI communicator: the root process plus all the non-home-node processes.
            self.xcomm = comm.Split(self.is_xcomm, self.id)
            self.is_lcomm = int(not bool(self.lroot_id)) #This is the local MPI communicator: will be replaced by multiprocessing when parallelization begins.
            self.lcomm = comm.Split(self.is_lcomm, self.id)

            #print "Hostname %s  local_root_id: %d  p_id: %d  l_id: %d" % (hn, local_root_id, p_id, l_id)

            if self.is_xcomm:
                self.x_ids = self.xcomm.gather(self.id)
            if self.is_lcomm:
                self.l_ids = self.lcomm.gather(self.id)
                if not self.is_root and not self.mpi_keep_workers_alive:
                    sys.exit(0)

    @property
    def is_root(self):
        return self.id == 0

    def _set_parallel_parms(self, parallel=True):
        self.p_mpi = parallel and self.mpi
        self.smp = parallel and not self.mpi
        self.parallel = parallel
        if self.parallel:
            if self.p_mpi:
                self.num = self.comm.Get_size() # MPI size always overrides manually set num. The user controls the pool size with mpirun -np nprocs
                if self.is_root:
                    self.homecores = multiprocessing.cpu_count()
            elif self.smp and self.num is None:
                self.num = multiprocessing.cpu_count()
        self.parms_set = True

class ManagerClient():
    def __init__(self, parent):
        self.parent = parent
        self.remote = self.parent.p.remote

    def get_workload(self):
        if self.remote:
            return self._get_workload_remote()
        else:
            return self._get_workload_local()

    def _get_workload_local(self):
        self.return_q.put((self.parent.p.p_id, self.parent.i.performance))
        return self.work_assignment_q.get()

    def _get_workload_remote(self):
        self.return_q.put((self.parent.p.p_id, self.parent.i.performance))
        return self.work_assignment_q.get()

class ManagerServer():
    def __init__(self, parent):
        self.parent = parent
        self.p = self.parent.p
        self.workload_queues = [multiprocessing.Queue() for i in range(self.p.nprocs)]
        self.return_queues = [multiprocessing.Queue() for i in range(self.p.nprocs)]
        
        self.work_qs = []
        for i in range(self.parent.p.nprocs):
            self.worq_qs.append(Queue.Queue())

        if self.p.has_remote:
            mpi_server = threading.Thread(target=ManagerServer.start_mpi_server, args=(self,))
            mpi_server.start()

        local_procs = []
        for l_id  in range(self.p.local_ids):
            local_procs.append(multiprocessing.Process(target=MDreader.mdreader._do_remote), args=(parent,l_id))
            local_procs[-1].start()
        self.run()
        for proc in local_procs:
            proc.join()
        if self.p.mpi:
            self.p.comm.Barrier()
        
    def run(self):
        pass

    def start_mpi_server(self):
        self.working_remote = self.p.num_remote
        incoming_perf = ctypes.c_float()

        for r_id in self.p.remote_ids:
            self.get_send_workload(r_id)

        while self.working_remote:
            rcv = comm.Recv(incoming_perf, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG)
            sender = rcv.tag
            self.receive_mpidata(sender)
            self.get_send_workload(sender, perf)

    def get_send_workload(self, p_id, perf):
        self.done_q.put((p_id, perf))
        workload = self.worq_qs[p_id].get()
        if workload is None:
           self.working_remote -= 1
        self.parent.p.xcomm.send(workload, dest = p_id)




