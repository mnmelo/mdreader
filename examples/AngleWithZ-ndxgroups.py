#!/usr/bin/env python
import mdreader
import numpy
"""
A simple example of a calculation done every frame on the coordinates
of groups chosen from an index. (the angle of a bond with the Z axis).
See the add_ndx documentation from mdreader.
"""

md = mdreader.MDreader()
# An index will now be expected from the user
md.add_ndx(ndxparms=["Select cholines", "Select phosphates"]) 

nbonds = len(md.ndxgs[0])
if len(md.ndxgs[0]) != len(md.ndxgs[1]):
    raise ValueError("Both groups must have the same number of atoms.")

angles = numpy.empty((len(md),nbonds)) # Angle data will be appended as the trajectory is parsed
                                       #  len(md) returns the total number of frames

for fm in md.iterate(p=1):
    # We can now refer to the atom groups according to their index.
    vecs = md.ndxgs[1].positions - md.ndxgs[0].positions
    norms = numpy.hypot.reduce(vecs, axis=1)
    angles[fm.frame,:] = (180/numpy.pi)*numpy.arccos(vecs[:,2]/norms)

numpy.savetxt(md.opts.outfile, angles)
