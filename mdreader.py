#!/usr/bin/python
# MDreader
# Copyright (c) Manuel Nuno Melo (m.n.melo@rug.nl)
#
# Released under the GNU Public Licence, v2 or any higher version
#
"""
Class for the all-too-frequent task of asking for options and reading in an xtc. 

version v2013.03.06
by Manuel Melo (m.n.melo@rug.nl)

"""

# TODO: account for cases where frames have no time (a GRO trajectory, for example).

import sys
import argparse
import os
import numpy
import re
import MDAnalysis
import math
import datetime
import types
import multiprocessing


# Static descriptions ##################################################
########################################################################

# Helper functions #####################################################
########################################################################

def _parallel_launcher(rdr, w_id):
    """ Helper function for the parallel execution of registered functions.

    """
    rdr.p_id = w_id
    return rdr._reader()

def _parallel_extractor(rdr, w_id):
    """ Helper function for the parallel extraction of trajectory coordinates/values.

    """
    # block seems to be faster.
    rdr.p_mode = 'block'
    rdr.p_id = w_id
    return rdr._extractor()

def concat_tseries(lst, ret=None):
    """ Concatenates a list of Timeseries objects """
    if ret is None:
        ret = lst[0]
    if len(lst[0]._tjcdx_ndx):
        ret._cdx = numpy.concatenate([i._cdx for i in lst])
    for attr in ret._props:
        setattr(ret, attr, numpy.concatenate([getattr(i, attr) for i in lst]))
    return ret

def check_file(fname):
    if not os.path.exists(fname):
        raise IOError('Can\'t find file %s' % (fname))
    if not os.access(fname, os.R_OK):
        raise IOError('Permission denied to read file %s' % (fname))
    return fname

def check_outfile(fname):
    dirname = os.path.dirname(fname)
    if not dirname:
        dirname = '.'
    if not os.access(dirname, os.W_OK):
        raise IOError('Permission denied to write file %s' % (fname))
    return fname

def check_positive(val):
    if val < 0:
        raise ValueError('Argument must be >= 0: %r' % (val))

def xtclen(xtc):
    return(len(xtc))

# Workaround for the lack of datetime.timedelta.total_seconds() in python<2.7
if hasattr(datetime.timedelta, "total_seconds"):
    dtime_seconds = datetime.timedelta.total_seconds
else:
    def dtime_seconds(dtime):
        return dtime.days*86400 + dtime.seconds + dtime.microseconds*1e-6


# Helper Classes #######################################################
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
                procs[-1].daemon = True
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
        return result

    def fcaller(self, f, args, num):
        self.outqueue.put((num, f(*args)))


class ProperFormatter(argparse.RawTextHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    """A hackish class to get proper help format from argparse.

    """
    def __init__(self, *args, **kwargs):
        super(ProperFormatter, self).__init__(*args, **kwargs)


class ThenNow:
    def __init__(self, oldval=None, newval=None):
        self.set(oldval, newval)
    def set(self, oldval, newval):
        self.old = oldval
        self.new = newval
    def fill(self, val):
        # Fill variant for the initial case where we have to assign both at initialization.
        self.set(val, val)
    def update(self, val, fill=False):
        if fill:
            self.fill(val)
        else:
            self.old = self.new
            self.new = val


class memoryCheck():
    """Checks memory of a given system
    Lifted from http://doeidoei.wordpress.com/2009/03/22/python-tip-3-checking-available-ram-with-python/

    """
    def __init__(self):
        if sys.platform == "linux2":
            self.value = self.linuxRam()
        elif sys.platform == "darwin":
            self.value = self.macRam()
        elif sys.platform == "win32":
            self.value = self.windowsRam()
        else:
            self.value = float('inf')
            raise EnvironmentError("Memory detection only works with Mac, Win, or Linux. Memory val set to 'inf'.")
 
    def windowsRam(self):
        """Uses Windows API to check RAM in this OS"""
        kernel32 = ctypes.windll.kernel32
        c_ulong = ctypes.c_ulong
        class MEMORYSTATUS(ctypes.Structure):
            _fields_ = [("dwLength", c_ulong),
                        ("dwMemoryLoad", c_ulong),
                        ("dwTotalPhys", c_ulong),
                        ("dwAvailPhys", c_ulong),
                        ("dwTotalPageFile", c_ulong),
                        ("dwAvailPageFile", c_ulong),
                        ("dwTotalVirtual", c_ulong),
                        ("dwAvailVirtual", c_ulong)]
        memoryStatus = MEMORYSTATUS()
        memoryStatus.dwLength = ctypes.sizeof(MEMORYSTATUS)
        kernel32.GlobalMemoryStatus(ctypes.byref(memoryStatus))
        return int(memoryStatus.dwTotalPhys/1024**2)
 
    def linuxRam(self):
        """Returns the RAM of a linux system"""
        import subprocess
        process = subprocess.Popen("free -m".split(), stdout=subprocess.PIPE)
        process.poll()
        totalMemory = process.communicate()[0].split("\n")[1].split()[1]
        return int(totalMemory)

    def macRam(self):
        """Returns the RAM of a mac system"""
        import subprocess
        process = subprocess.Popen("vm_stat", stdout=subprocess.PIPE)
        process.poll()
        outpt = process.communicate()[0]
        try:
            totalPages = int(re.search("Pages free:\s*(\d+).", outpt).groups()[0])
            bytperPage = int(re.search("page size of (\d+) bytes", outpt).groups()[0])
        except:
            raise EnvironmentError("Can't detect how much free memory is available from 'vm_stat'.")
        return int(float(totalPages*bytperPage/(1024**2)))

class SeriesCdx():
    """ Placeholder class for a variable behavior of Timeseries.coords"""
    def __init__(self):
        pass

class Timeseries():
    def __getstate__(self):
        statedict = self.__dict__.copy()
        for attr in ["coords","_coords","_cdx_unpacker"]:
            if attr in statedict:
                del statedict[attr]
        return statedict

    def __init__(self):
        self._coords = SeriesCdx()
        self._props = []
        self._tjcdx_ndx = []
        self._tjcdx_relndx = []
        self._cdx = None
        self._xyz = (True, True, True)
        self._coords_istuple = False
        def _cdx_unpacker(n):
            return self._cdx[:,self._tjcdx_relndx[n]]
        self._coords.__getitem__ = _cdx_unpacker

    @property
    def coords(self):
        if self._coords_istuple:
            return self._coords
        else:
            return self._cdx


# MDreader Class #######################################################
########################################################################

class MDreader(MDAnalysis.Universe, argparse.ArgumentParser):
    """An object class inheriting from both argparse.ArgumentParser and MDAnalysis.Universe. Should be initialized as for argparse.ArgumentParser, with additional named arguments:
    Argument 'arguments' should be passed the list of command line arguments; it defaults to sys.argv[1:], which is very likely what you'll want.
    Argument 'outstats' defines how often to output frame statistics. Defaults to 1.
    Argument 'statavg' defines over how many frames to average statistics. Defaults to 100.

    
    Command-line argument list will default to:
    
    usage: %prog% [-h] [-f TRAJ] [-c STRUCT] [-o OUT] [-b TIME] [-e TIME] [-fmn]
                  [-skip FRAMES] [-v LEVEL]

    optional arguments:
      -h, --help    show this help message and exit
      -f TRAJ       file	The trajectory to analyze. (default: traj.xtc)
      -s TOPOL      file	.tpr, .gro, or .pdb file with the same atom numbering as the trajectory. (default: topol.tpr)
      -o OUT        file	The main data output file. (default: data.xvg)
      -b TIME       real	Time to begin analysis from. (default: 0)
      -e TIME       real	Time to end analysis at. (default: inf)
      -fmn          bool	Whether to interpret -b and -e as frame numbers. (default: False)
      -skip FRAMES  int 	Number of frames to skip when analyzing. (default: 1)
      -v LEVEL      enum	Verbosity level. 0:quiet, 1:progress 2:debug (default: 1)

    where %prog% is the 'prog' argument as supplied during initialization, or sys.argv[0] if none is provided.
    
    After MDreader instantiation the values of the defaults to the arguments can be changed using MDreader.setargs() (see function documentation). If a 'ver' argument is passed to setargs it will be displayed as the program version, and a '-V'/'--version' option for that purpose will be automatically created.
    The arguments for an MDreader instance can also be added or overridden using the add_argument() method (see the argparse documentation).
    Finally, the iterate() method will iterate over the trajectory according to the supplied options, yielding frames as it goes. You'll probably want to use it as part of a for-loop header.
    
    """

    def __init__(self, arguments=sys.argv[1:], conflict_handler='resolve', formatter_class=ProperFormatter, outstats=1, statavg=100, *args, **kwargs):
        argparse.ArgumentParser.__init__(self, *args, conflict_handler=conflict_handler, formatter_class=formatter_class, **kwargs)
        self.arguments = arguments
        self.setargs()
        self.parsed = False
        self.hasindex = False
        self.nframes = None
        self.outstats = outstats
        self.statavg = statavg
        # Stuff pertaining to progress output/parallelization
        self.parallel = False  # Whether to parallelize
        self.p_smp = False  # SMP parallelization (within the same machine, or virtual machine)
        self.p_mpi = False  # MPI parallelization
        self.progress = None
        self.p_mode = 'block'
        self.p_overlap = 0
        self.p_num = None
        self.p_id = 0
        self.p_scale_dt = True
        self.p_mpi_keep_workers_alive = False
        self.p_parms_set = False
        self.i_parms_set = False
        self._cdx_meta = False # Whether to also return time/box arrays when extracting coordinates.

        # Check whether we're running under MPI. Not failsafe, but the user should know better than to fudge with these env vars.
        mpivarlst = ['PMI_RANK', 'OMPI_COMM_WORLD_RANK', 'OMPI_MCA_ns_nds_vpid',
                     'PMI_ID', 'SLURM_PROCID', 'LAMRANK', 'MPI_RANKID',
                     'MP_CHILD', 'MP_RANK', 'MPIRUN_RANK']
        self.mpi = bool(sum([var in os.environ.keys() for var in mpivarlst]))

    # The overridable function for parallel processing.
    def p_fn(self):
        pass

    def __len__(self):
        return self.totalframes

    def setargs(self, f='traj.xtc', s='topol.tpr', o='data.xvg', b=0, e=float('inf'), skip=1, v=1, version=None):
        """ This function allows the modification of the default parameters of the default arguments without having
            to go through the hassle of overriding the args in question. The arguments to this function are self-explanatory
            and will override the defaults of the corresponding options.
        """
        self.add_argument('-f', metavar='TRAJ', dest='xtc', default=f,
                help = 'file\tThe trajectory to analyze.')
        self.add_argument('-s', metavar='TOPOL', dest='top', default=s,
                help = 'file\t.tpr, .gro, or .pdb file with the same atom numbering as the trajectory.')
        self.add_argument('-o', metavar='OUT', dest='outfile', default=o,
                help = 'file\tThe main data output file.')
        self.add_argument('-b', metavar='TIME', type=float, dest='starttime', default=b,
                help = 'real\tTime to begin analysis from.')
        self.add_argument('-e', metavar='TIME', type=float, dest='endtime', default=e,
                help = 'real\tTime to end analysis at.')
        self.add_argument('-fmn',  action='store_true', dest='asframenum',
                help = 'bool\tWhether to interpret -b and -e as frame numbers.')
        self.add_argument('-skip', metavar='FRAMES', type=int, dest='skip', default=skip,
                help = 'int \tNumber of frames to skip when analyzing.')
        self.add_argument('-v', metavar='LEVEL', type=int, choices=[0,1,2], dest='verbose', default=v,
                help = 'enum\tVerbosity level. 0:quiet, 1:progress 2:debug')
        if version is not None:
            self.add_argument('-V', '--version', action='version', version='%%(prog)s %s'%ver,
                help = 'Prints the script version and exits.')

    def add_ndx(self, ng=1, ndxparms=[], ndxdefault='index.ndx', ngdefault=1, smartindex=True):
        """ Adds an index read to the MDreader. A -n option will be added.
        ng controls how many groups to ask for.
        If ng is set to 'n' a -ng option will be added, which will then control how many groups to ask for.
        ndxparms should be a list of strings to be printed for each group selection. The default is "Select a group" (a colon is inserted automatically).
        To allow for one or more reference groups plus n analysis groups, ndxparms will be interpreted differently according to ng and the -ng option:
            If ng is "n" it will be set to the number of groups specified by option -ng plus the number of ndxparms elements before the last.
            If ng is greater than the number of elements in ndxparms, then the last element will be repeated to fulfill ng. If ndxparms is greater, all its elements will be used and ng ignored.
        ndxdefault and ngdefault set the defaults for the -n and -ng options ('index.ndx' and 1, respectively).
        smartindex controls smart behavior, by which an index with a number of groups equal to n is taken as is without prompting. You'll want to disble it when it makes sense to pick the same index group multiple times.

        Example:
        #Simple index search for a single group. Default message:
        MDreader_obj.add_ndx()
        #Ask for more groups:
        MDreader_obj.add_ndx(ng=3)
        #Ask for a reference group plus a number of analysis groups to be decided with -ng
        MDreader_obj.add_ndx(ng="n", ndxparms=["Select a reference group", "Select a group for doing stuff"])

        """
        if self.hasindex:
            raise AttributeError("Index can only be set once.")
        self.hasindex = True
        self.add_argument('-n', metavar='INDEX', dest='ndx', default=ndxdefault,
                help = 'file\tIndex file.')
        self.ng = ng
        if ng == "n":
            self.add_argument('-ng', metavar='NGROUPS', type=int, dest='ng', default=ngdefault,
                    help = 'file\tNumber of groups for analysis.')
        self.ndxparms = ndxparms
        self.smartindex = smartindex
        
    def do_parse(self):
        """ Parses the command-line arguments according to argparse and does some basic sanity checking on them. It also prepares some argument-dependent loop variables.
        If it hasn't been called so far, do_parse() will be called by the iterate() method.
        You'll want to call it manually before iteration if you need to perform some more argument sanity checking outside MDreader, or to prepare for the loop based on the argument values.

        """
        self.opts = self.parse_args(self.arguments)

        if self.mpi:
            from mpi4py import MPI
            self.comm = MPI.COMM_WORLD
            self.p_id = self.comm.Get_rank()
            self.p_num = self.comm.Get_size()

        if self.opts.verbose and self.p_id == 0:
            sys.stderr.write("Loading...\n")
        ## Post option handling
        map(check_file,(self.opts.top,self.opts.xtc))
        map(check_outfile,(self.opts.outfile,))
        map(check_positive,(self.opts.starttime,self.opts.endtime,self.opts.skip))
        if self.opts.endtime < self.opts.starttime:
            raise ValueError('Endtime lower than starttime.')
        MDAnalysis.Universe.__init__(self, self.opts.top, self.opts.xtc)

        # Trajectory indexing can be slow. No need to do for every MPI worker: we just pass the offsets around.
        if self.p_id == 0:
            self.nframes = len(self.trajectory)
            if self.nframes is None or self.nframes < 1:
                raise IOError('No frames to be read.')
        if self.mpi:
            self.trajectory._TrjReader__offsets = self.comm.bcast(self.trajectory._TrjReader__offsets, root=0)
            if self.p_id != 0:
                self.trajectory._TrjReader__numframes = len(self.trajectory._TrjReader__offsets)
                self.nframes = len(self.trajectory._TrjReader__offsets)

        self.hastime = True
        if not hasattr(self.trajectory.ts, 'time') or self.trajectory.dt == 0.:
            if not self.opts.asframenum:
                sys.stderr.write("Trajectory has no time information. Will interpret limits as frame numbers.\n")
            self.hastime = False
            self.opts.asframenum = True

        if self.opts.starttime > self.opts.endtime:
            raise ValueError("starttime/frame (%f) greater than endtime/frame (%f)." % (self.opts.starttime, self.opts.endtime))
        if self.opts.asframenum:
            self.startframe=int(max(0, self.opts.starttime))
            self.endframe=int(min(self.nframes-1, self.opts.endtime))
        else:
            self.startframe=int(max(math.ceil((self.opts.starttime-self.trajectory[0].time)/self.trajectory.dt), 0))
            self.endframe=int(min((self.opts.endtime-self.trajectory[0].time)/self.trajectory.dt,self.nframes-1))
        if self.startframe > self.nframes:
            if self.opts.asframenum:
                raise ValueError("You requested to start at frame %d but trajectory only has %d frames." % (self.opts.starttime, self.nframes))
            else:
                raise ValueError("You requested to start at time %f ps but trajectory only goes up to %f ps." % (self.opts.starttime, (self.nframes-1)*self.trajectory.dt))
        self.totalframes = int(math.ceil(float(self.endframe-self.startframe+1)/self.opts.skip))
        #print "startframe: %d, endframe: %d, totalframes: %d" % (self.startframe, self.endframe, self.totalframes)

        self.parsed = True
        if self.p_id == 0:  # Either there is no MPI, or we're root
            if self.hasindex:
                self._parse_ndx()

        if self.mpi:   # Get ready to broadcast the index list
            if self.p_id == 0:
                tmp_ndx = [grp.indices() for grp in self.ndxgs]
            else:
                if self.hasindex:
                    tmp_ndx = None
            tmp_ndx = self.comm.bcast(tmp_ndx, root=0)
            if self.p_id != 0:
                self.ndxgs = [self.atoms[ndx] for ndx in tmp_ndx]

    def _parse_ndx(self):
        with open(self.opts.ndx) as NDX:
            tmpstr=""
            ndx_atids=[]
            ndxheader=None
            while True:
                line = NDX.readline()
                mtx = re.match('\s*\[\s*(\S+)\s*\]\s*',line)
                if mtx or not line:
                    if ndxheader is not None:
                        ndx_atids.append((ndxheader, numpy.array(tmpstr.split(), dtype=int)-1))
                        tmpstr = ""
                    if not line:
                        break
                    ndxheader = mtx.groups()[0]
                else:
                    tmpstr += line

        # How many groups to auto assign (it may be useful to have the same group as a reference and as an analysis group, so we check it a bit more thoroughly).
        if self.ng == "n":
            refng = max(0,len(self.ndxparms)-1)
            otherng = self.opts.ng
            self.ng = refng+otherng
        elif self.ng > len(self.ndxparms):
            refng = max(0,len(self.ndxparms)-1)
            otherng = self.ng-refng
        else:
            self.ng = len(self.ndxparms)
            otherng = self.ng
            refng = 0

        if not self.ndxparms:
            self.ndxparms = ["Select a group"]*self.ng
        elif self.ng > len(self.ndxparms):
            self.ndxparms.extend([self.ndxparms[-1]]*(otherng-1))

        autondx = 0
        if self.smartindex and len(ndx_atids)==otherng:
            autondx = otherng

        # Check for interactivity, otherwise just eat it from stdin
        if sys.stdin.isatty():
            maxlen = str(max(map(len, zip(*ndx_atids)[0])))
            maxidlen = str(len(str(len(ndx_atids)-1)))
            maxlenlen = str(max(map(len, (map(str, (map(len, zip(*ndx_atids)[1])))))))
            for id, hd in enumerate(ndx_atids):
                sys.stderr.write(("Group %"+maxidlen+"d (%"+maxlen+"s) has %"+maxlenlen+"d elements\n") % (id, hd[0], len(hd[1])))
            sys.stderr.write("\n")
            self.stdin = None
            def getinputline(dum):
                return raw_input()
        else:
            self.stdin = "".join(sys.stdin.readlines()).split()
            def getinputline(inp):
                return inp.pop(0)

        self.ndxgs=[]
        auto_id = 0       # for auto assignment of group ids
        for gid, ndxstr in enumerate(self.ndxparms):
            if gid < refng or not autondx:
                if self.stdin is None:
                    sys.stderr.write("%s:\n" % (ndxstr))
                self.ndxgs.append(self.atoms[ndx_atids[int(getinputline(self.stdin))][1]])
            else:
                if gid == refng:
                    sys.stderr.write("Only %d groups in index file. Reading them all.\n" % len(ndx_atids))
                self.ndxgs.append(self.atoms[ndx_atids[auto_id][1]])
                auto_id += 1

    def iterate(self):
        """ Yields snapshots from the trajectory according to the specified start and end boundaries and skip.
        Calculations on AtomSelections will automagically reflect the new snapshot, without needing to refer to it specifically.
        Output and parallelization will depend on a number of MDreader properties that are automatically set, but can be changed before invocation of iterate():
          MDreader.progress (default: None) can be one of 'frame', 'pct', 'both', 'empty', or None. It sets the output to frame numbers, %% progress, both, or nothing. If set to None behavior defaults to 'frame', or 'pct' when iterating in parallel block mode.
          MDreader.p_mode (default: 'block') sets either 'interleaved' or 'block' parallel iteration.
          When MDreader.p_mode=='block' MDreader.p_overlap (default: 0) sets how many frames blocks overlap, to allow multi frame analyses (say, an average) to pick up earlier on each block.
          MDreader.p_num (default: None) controls in how many blocks/segments to divide the iteration (the number of workers; will use all the processing cores if set to None) and MDreader.p_id sets the id of the current worker for reading and output purposes (to avoid terminal clobbering only p_id 0 will output). 
            **
            Beware to always set a different p_id per worker when iterating in parallel, otherwise you'll end up with repeated trajectory chunks.
            **
          MDreader.p_scale_dt (default: True) controls whether the reported time per frame will be scaled by the number of workers, in order to provide an absolute, albeit estimated, per-frame time.

        """
        if not self.parsed:
            self.do_parse()

        if not self.p_parms_set:
            self._set_parallel_parms(False) # By default do a serial iteration. _set_parallel_parms should have been set by whichever internal function called iterate() instead.
        verb = self.opts.verbose and (not self.parallel or self.p_id==0)
        # We're only outputting after each worker has picked up on the pre-averaging frames
        self.i_overlap = True
        self.iterframe = 0
        if verb:
            sys.stderr.write("Iterating through trajectory...\n")
            self.loop_time = ThenNow()
            self.loop_time.fill(datetime.datetime.now())
            self.loop_dtimes = numpy.empty(self.statavg, dtype=datetime.timedelta)
        sys.stdout.flush()
        sys.stderr.flush()

        if not self.i_parms_set:
            self._set_iterparms()

        if self.progress is None:
            if self.parallel and self.p_mode == "block":
                self.progress = 'pct'
            else:
                self.progress = 'frame'
        # Python-style implementation of a switch/case. It also avoids always comparing the flag every frame.
        if self.progress == "frame":
            framestr = "Frame {0:d}"
            if self.hastime:
                framestr += "  t= {2:.1f} ps  "
        elif self.progress == "pct":
            framestr = "{1:3.0%}  "
        elif self.progress == "both":
            framestr = "Frame {0:d}, {1:3.0%}"
            if self.hastime:
                framestr += "  t= {2:.1f} ps  "
        elif self.progress == "empty":
            framestr = ""
        else:
            raise ValueError("Unrecognized progress mode \"%r\"" % (self.progress))

        # The LOOP!
        for self.snapshot in self.trajectory[self.i_startframe:self.i_endframe+1:self.i_skip]:
            if self.iterframe >= self.p_overlap:
                self.i_overlap = False # Done overlapping. Let the output begin!
            if verb:
                self.loop_time.update(datetime.datetime.now())
                self.loop_dtime = self.loop_time.new - self.loop_time.old
                self.loop_dtimes[self.iterframe % self.statavg] = self.loop_dtime
                # Output stats every outstats step or at the last frame.
                if (not self.snapshot.frame % self.outstats) or self.iterframe == self.i_totalframes-1:
                    avgframes = min(self.iterframe+1,self.statavg)
                    self.loop_sumtime = numpy.sum(self.loop_dtimes[:avgframes])
                    # No float*dt multiplication before python 3. Let's scale the comparing seconds and do set the dt ourselves.
                    etaseconds = dtime_seconds(self.loop_sumtime)*float(self.i_totalframes-self.iterframe)/avgframes
                    eta = datetime.timedelta(seconds=etaseconds)
                    if etaseconds > 300:
                        etastr = (datetime.datetime.now()+eta).strftime("Will end %Y-%m-%d at %H:%M:%S.")
                    else:
                        etastr = "Will end in %ds." % round(etaseconds)
                    loop_dtime_s = dtime_seconds(self.loop_dtime)
                    if self.parallel:
                        if self.p_scale_dt:
                            loop_dtime_s /= self.p_num

                    if self.hastime:
                        progstr = framestr.format(self.snapshot.frame-1, float(self.iterframe+1)/(self.i_totalframes), self.snapshot.time)
                    else:
                        progstr = framestr.format(self.snapshot.frame-1, float(self.iterframe+1)/(self.i_totalframes))

                    sys.stderr.write("\033[K%s(%.4f s/frame) \t%s\r" % (progstr, loop_dtime_s, etastr))
                    if self.iterframe == self.i_totalframes-1: 
                        #Last frame. Clean up.
                        sys.stderr.write("\n")
                    sys.stderr.flush()
            yield self.snapshot
            self.iterframe += 1
        self.i_parms_set = False
        self.p_parms_set = False
    
    def timeseries(self, coords=None, props=None, x=True, y=True, z=True, parallel=True):
        """ Extracts coordinates and/or other time-dependent attributes from a trajectory.
        'coords' can be an AtomGroup, an int, a selection text, or a tuple of these. In case of an int, it will be taken as the mdreader index group number to use.
        'props' must be a str or a tuple of str, which will be used as attributes to extract from the trajectory's timesteps. These must be valid attributes of the mdreader.trajectory.ts class, and cannot be bound functions or reserved '__...__' attributes.
        Will return a mdreader.Timeseries object, holding an array, or a tuple, for each coords, and having named properties holding the same-named time-arrays. If both coords and props are are None the default is to return the time-coordinates array for the entire set of atoms.
        'props' attributes should be set in quotes, ommitting the object's name.
        'parallel' (default=True) controls parallelization behavior.
        'x', 'y', and 'z' (default=True) set whether the three coordinates, or only a subset, are extracted.

        Examples:
        1. Timeseries with whole time-coordinate array for all atoms:
        mdreader.timeseries()

        2. Equivalent to above:
        mdreader.timeseries(mdreader.atoms)

        3. Timeseries with a tuple of two selections time-coordinates (the NC3 atoms, and the fifth chosen group from the index):
        mdreader.timeseries(("name NC3", 4))

        4. Timeseries with an array of box time-coordinates:
        mdreader.timeseries(props='dimensions')

        5. Timeseries with a time-coordinate array correspondig to the x,y components of the second index group, and time-arrays of the system's box dimensions and time.
        mdreader.timeseries(coords=1, props=('dimensions', 'time'), z=False)

        """
        # First things first
        if not self.parsed:
            self.do_parse()
        if not self.p_parms_set:
            self._set_parallel_parms(parallel)

        self._tseries = Timeseries()
        tjcdx_atgrps = []
        if coords is None and props is None:
            tjcdx_atgrps = [self.atoms]
        elif coords is not None:
            if type(coords) == MDAnalysis.core.AtomGroup.AtomGroup:
                tjcdx_atgrps = [coords]
            elif type(coords) == types.IntType:
                tjcdx_atgrps = [self.ndxgs[coords]]
            elif isinstance(coords, basestring):
                tjcdx_atgrps = [self.selectAtoms(coords)]
            else:
                self._tseries._coords_istuple = True
                try:
                    for atgrp in coords:
                        if type(atgrp) == types.IntType:
                            tjcdx_atgrps.append(self.ndxgs[atgrp])
                        elif type(atgrp) == MDAnalysis.core.AtomGroup.AtomGroup:
                            tjcdx_atgrps.append(atgrp)
                        else:
                            tjcdx_atgrps.append(self.selectAtoms("%s" % atgrp))
                except:
                    raise TypeError("Error parsing coordinate groups.\n%r" % sys.exc_info()[1])

        # Get the unique list of indices, and the pointers to that list for each requested group.
        indices = [grp.indices() for grp in tjcdx_atgrps]
        indices_len = [len(ndx) for ndx in indices]
        self._tseries._tjcdx_ndx, self._tseries._tjcdx_relndx = numpy.unique(numpy.concatenate(indices), return_inverse=True)
        self._tseries._tjcdx_relndx = numpy.split(self._tseries._tjcdx_relndx, numpy.cumsum(indices_len[:-1])) 

        self._tseries._xyz = (x,y,z)
        mem = self.atoms[self._tseries._tjcdx_ndx].coordinates()[0].nbytes*sum(self._tseries._xyz)

        if props is not None:
            if isinstance(props, basestring):
                props = [props]
            self._tseries._props = []
            #validkeys = self.trajectory.ts.__dict__.keys()
            for attr in props:
                if not hasattr(self.trajectory.ts, attr):
                    raise AttributeError('Invalid attribute for extraction. It is not an attribute of trajectory.ts')
                self._tseries._props.append(attr)
                # Rough memory checking
                mem += sys.getsizeof(getattr(self.trajectory.ts, attr))
                setattr(self._tseries, attr, None)
        mem *= len(self)

        # This is potentially a lot of memory. Check it beforehand, except for MPI, which we trust the user to do themselves.
        if not self.p_mpi:
            avail_mem = memoryCheck()
            if 2*mem/(1024**2) > avail_mem.value:
                raise EnvironmentError("You are attempting to read approximately %dMB of coordinates/values but your system only seems to have %dMB of physical memory (and we need at least twice as much memory as read bytes)." % (mem/(1024**2), avail_mem.value))

        tseries = self._tseries
        if not self.p_smp:
            tseries = self._extractor()
            if self.p_mpi:
                tseries = self.comm.gather(tseries, root=0)
                if self.p_id == 0:
                    tseries = concat_tseries(tseries)
        else:
            pool = Pool(processes=self.p_num)
            concat_tseries(pool.map(_parallel_extractor, [(self, i) for i in range(self.p_num)]), tseries)

        if self.p_mpi and not self.p_mpi_keep_workers_alive and self.p_id != 0:
            sys.exit(0)
        else:
            self._tseries = None
            tseries.atgrps = tjcdx_atgrps
            return tseries


    def do_in_parallel(self, fn, parallel=True):
        """ Applies fn to every frame, taking care of parallelization details. Returns a list with the returned elements, in order. parallel can be set to false to force serial behavior.
        Refer to the documentation on MDreader.iterate() for information on which MDreader attributes to set to change default parallelization options.

        """
        self.p_fn = fn
        if not self.p_parms_set:
            self._set_parallel_parms(parallel)

        if not self.p_smp:
            if not self.p_mpi:
                return self._reader()
            else:
                res = self._reader()
                res = self.comm.gather(res, root=0)
                if self.p_id == 0:
                    return [val for subl in res for val in subl] 
                elif not self.p_mpi_keep_workers_alive:
                    sys.exit(0)

        else:
            pool = Pool(processes=self.p_num)
            results = pool.map(_parallel_launcher, [(self, i) for i in range(self.p_num)]) 
            # 1-level unravelling
            linres = results[0][:]
            for res in results[1:]:
                linres.extend(res)
            return linres  

    def _reader(self):
        """ Applies self.p_fn for every trajectory frame. Parallelizable!

        """
        # We need a brand new file descriptor per SMP worker, otherwise we have a nice chaos.
        if self.p_smp:
            self._Universe__trajectory._reopen()
        if not self.i_parms_set:
            self._set_iterparms()

        reslist = []
        if self.i_totalframes:
            for frame in self.iterate():
                if not self.i_overlap:
                    reslist.append(self.p_fn(self))
        return reslist

    def _extractor(self):
        """ Extracts the values asked for in mdreader._tseries. Parallelizable!

        """
        # We need a brand new file descriptor per SMP worker, otherwise we have a nice chaos.
        if self.p_smp:
            self._Universe__trajectory._reopen()
        if not self.i_parms_set:
            self._set_iterparms()

        if len(self._tseries._tjcdx_ndx):
            self._tseries._cdx = numpy.empty((self.i_totalframes, len(self._tseries._tjcdx_ndx), sum(self._tseries._xyz)), dtype=numpy.float32)
        for attr in self._tseries._props:
            try:
                shape = (self.i_totalframes,) + getattr(self.trajectory.ts, attr).shape
            except AttributeError:
                shape = (self.i_totalframes,)
            try:
                setattr(self._tseries, attr, numpy.empty(shape, dtype=(getattr(self.trajectory.ts, attr)).dtype))
            except AttributeError:
                setattr(self._tseries, attr, numpy.empty(shape, dtype=type(getattr(self.trajectory.ts, attr))))
        if self.i_totalframes:
            for frame in self.iterate():
                if self._tseries._cdx is not None:
                    self._tseries._cdx[self.iterframe] = self.atoms[self._tseries._tjcdx_ndx].coordinates()[:,numpy.where(self._tseries._xyz)[0]]
                for attr in self._tseries._props:
                    getattr(self._tseries, attr)[self.iterframe,...] = getattr(self.trajectory.ts, attr)
        return self._tseries

    
    def _set_iterparms(self):
        # Because of parallelization lots of stuff become limited to the iteration scope.
        # defined a group of i_ variables just for that.
        if self.parallel:
            if self.p_num < 2 and self.p_smp:
                raise ValueError("Parallel iteration requested, but only one worker (MDreader.p_num) sent to work.")

            if self.p_mode == "interleaved":
                frames_per_worker = numpy.ones(self.p_num,dtype=numpy.int)*(self.totalframes/self.p_num)
                frames_per_worker[:self.totalframes%self.p_num] += 1 # Last workers to arrive work less. That's life for you.
                self.i_skip = self.opts.skip * self.p_num
                self.i_startframe = self.startframe + self.opts.skip*self.p_id
                self.i_endframe = self.i_startframe + int(frames_per_worker[self.p_id]-1)*self.i_skip 
            elif self.p_mode == "block":
                # As-even-as-possible distribution of frames per workers, allowing the first one to work more to compensate the lack of overlap.
                frames_per_worker = numpy.ones(self.p_num,dtype=numpy.int)*((self.totalframes-self.p_overlap)/self.p_num)
                frames_per_worker[:(self.totalframes-self.p_overlap)%self.p_num] += 1 
                # Let's check the overlap for zero work
                frames_per_worker[0] += self.p_overlap # Add extra overlap frames to the first worker.
                self.i_skip = self.opts.skip
                self.i_startframe = self.startframe + int(numpy.sum(frames_per_worker[:self.p_id]))*self.i_skip
                self.i_endframe = self.i_startframe + int(frames_per_worker[self.p_id]-1)*self.i_skip 
                # And now we subtract the overlap from the startframe, except for worker 0
                if self.p_id:
                    self.i_startframe -= self.p_overlap*self.i_skip
            else:
                raise ValueError("Unrecognized p_mode \"%r\"" % (self.p_mode))
        else:
            self.i_skip = self.opts.skip
            self.i_startframe = self.startframe
            self.i_endframe = self.endframe
        self.i_totalframes = int(math.ceil(float(self.i_endframe-self.i_startframe+1)/self.i_skip))
        self.i_parms_set = True


    def _set_parallel_parms(self, parallel=True):
        self.p_mpi = parallel and self.mpi
        self.p_smp = parallel and not self.mpi
        self.parallel = parallel
        if self.parallel:
            if self.p_mpi:
                self.p_num = self.comm.Get_size() # MPI size always overrides manually set p_num. The user controls the pool size with mpirin -np nprocs
            elif self.p_smp and self.p_num is None:
                self.p_num = multiprocessing.cpu_count()
        self.p_parms_set = True

