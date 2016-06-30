#!/usr/bin/env python
import mdreader
import numpy
"""
A simple example of a calculation done every frame on the coordinates
of groups chosen from an index. (the angle of a bond with the Z axis).
Iteration is done in parallel, and results must be manually gathered.
(A future improvement will be the automatic handling of these cases).
"""

md = mdreader.MDreader()
# An index will now be expected from the user
md.add_ndx(ndxparms=["Select cholines", "Select phosphates"]) 

nbonds = len(md.ndxgs[0])
if len(md.ndxgs[0]) != len(md.ndxgs[1]):
    raise ValueError("Both groups must have the same number of atoms.")

# The function that will be called every frame, distributed by all workers.
#  Only the returned values will be available to the calling script.
def calc_frame_angles():
    vecs = md.ndxgs[1].positions-md.ndxgs[0].positions
    norms = numpy.hypot.reduce(vecs, axis=1)
    return (180/numpy.pi)*numpy.arccos(vecs[:,2]/norms)

result = md.do_in_parallel(calc_frame_angles)   # Result is now a list of as many elements as frames,
                                                #  each being a returned value from the called function.
angles = numpy.array(result)    # Gathering the results in an array.
numpy.savetxt(md.opts.outfile, angles, header=md.info_header())
