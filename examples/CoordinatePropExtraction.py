#!/usr/bin/env python
import mdreader
import numpy
"""
Examples of different types of extractions using mdreader.MDreader.timeseries()
Extraction is parallel by default. Pass parallel=False to timeseries() to make it serial.
See the docstrings of the timeseries() function in mdreader.

"""


md = mdreader.MDreader()
md.add_ndx(ndxparms=["Test group 1", "Test group 2", "Test group 3"])

tseries = md.timeseries()   # tseries.coords is a coordinates array of the entire system.
                            #  shape=(numframes, numatoms, 3)

tseries = md.timeseries(2, y=False) # tseries.coords is a coordinates array of the atoms in the
                                    #  third chosen index group, excluding the y coordinate.
                                    #  shape=(numframes, grp3_numatoms, 2)

tseries = md.timeseries(2, y=False, z=False)    # tseries.coords is a coordinates array of the atoms in the
                                                #  third chosen index group, excluding the y and z coordinates.
                                                #  shape=(numframes, grp3_numatoms, 1)

tseries = md.timeseries((0,1))  # tseries.coords is a tuple of two elements: the coordinates 
                                #  array of the atoms in the first, resp. second chosen index group.
                                #  each of shape=(numframes, grp_numatoms, 3)

tseries = md.timeseries((2,))   # tseries.coords is a single-element tuple of the coordinates 
                                #  array of the atoms in the third chosen index group.

tseries = md.timeseries((2,"name CA"))  # tseries.coords is a tuple of two elements: the coordinates 
                                        #  array of the atoms in the third chosen index group, and that
                                        #  of the atoms with name "CA".

sel = md.select_atoms("name CA")
tseries = md.timeseries((2,sel))  # same as above 

tseries = md.timeseries(props="dimensions") # tseries gets a 'dimensions' attribute; tseries.dimensions 
                                            #  is the array (frames, boxvectors). tseries.coords is not set.

tseries = md.timeseries((2,sel), props=("dimensions", "time"))  # tseries gets 'dimensions' and 'time' attributes;
                                                                #  tseries.dimensions is the array (frames, boxvectors);
                                                                #  tseries.time is the array (frametimes);
                                                                #  tseries.coords is a tuple of two elements: the coordinates
                                                                #  array of the atoms in the third chosen index group, and that
                                                                #  of the atoms in the 'sel' AtomGroup selection.
