#!/usr/bin/env python
import mdreader
import numpy
"""
A simple example of a calculation done every frame on the coordinates
of predefined groups. (the angle of a bond with the Z axis).
"""

md = mdreader.MDreader()
md.do_parse() # Must do this to have access to the trajectory objects *before* the start of the loop.

topNC3 = md.selectAtoms("name NC3 and prop z > 200")
topPO4 = md.selectAtoms("name PO4 and prop z > 200")
nbonds = len(topNC3)

angles = numpy.empty((len(md),nbonds)) # Angle data will be appended as the trajectory is parsed
                                       #  len(md) returns the total number of frames

for fm in md.iterate():
    # The power of NumPy
    vecs = topPO4.coordinates()-topNC3.coordinates()        # vecs'  shape is (700,3)
    norms = numpy.hypot.reduce(vecs, axis=1)                # norms' shape is (700,)
    frame_angs = (180/numpy.pi)*numpy.arccos(vecs[:,2]/norms) # ":" notation: all the values of the axis
    angles[fm.frame-1,:] = frame_angs                       # direct assigment to the results array (could be condensed with previous line).

numpy.savetxt(md.opts.outfile, angles)  # Save data to file
