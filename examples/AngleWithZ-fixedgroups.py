#!/usr/bin/env python
import mdreader
import numpy
"""
A simple example of a calculation done every frame on the coordinates
of predefined groups. (the angle of a bond with the Z axis).
"""

md = mdreader.MDreader()

topNC3 = md.select_atoms("name NC3 and prop z > 200")
topPO4 = md.select_atoms("name PO4 and prop z > 200")
nbonds = len(topNC3)

angles = numpy.empty((len(md),nbonds)) # Angle data will be appended as the trajectory is parsed
                                       #  len(md) returns the total number of frames

for fm in md.iterate(p=1):
    # The power of NumPy
    vecs = topPO4.positions - topNC3.positions              # vecs'  shape is (700,3)
    norms = numpy.hypot.reduce(vecs, axis=1)                # norms' shape is (700,)
    frame_angs = (180/numpy.pi)*numpy.arccos(vecs[:,2]/norms) # ":" notation: all the values of the axis
    angles[fm.frame,:] = frame_angs                       # direct assigment to the results array

numpy.savetxt(md.opts.outfile, angles)  # Save data to file
