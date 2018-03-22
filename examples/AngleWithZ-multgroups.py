#!/usr/bin/env python
import mdreader
import numpy
"""
A simple example of a calculation done every frame on the coordinates
of groups chosen from an index. (the angle of a bond with the Z axis).
The user can use the -ng option to specify an arbitrary number of groups
for analysis.
See the add_ndx documentation from mdreader.
"""

md = mdreader.MDreader()
# We'll have a user-set number of groups besides the 1st.
md.add_ndx(ng="n", ndxparms=["Select vertex", "Select atoms"])

nbonds = len(md.ndxgs[0])
# The result array is now 3-dimensional
angles = numpy.empty((len(md),len(md.ndxgs)-1,nbonds)) # Angle data will be appended as the trajectory
                                                       # is parsed
                                                       # we have nbonds * number of groups besides the
                                                       # first * number of frames
for fm in md.iterate(p=1):
    for i,grp in enumerate(md.ndxgs[1:]):  # md.ndxgs[0] is the vertex reference
        vecs = md.ndxgs[0].positions - grp.positions
        norms = numpy.hypot.reduce(vecs, axis=1)
        # place the result at the correct frame and group
        angles[fm.frame-1,i,:] = (180/numpy.pi)*numpy.arccos(vecs[:,2]/norms)

# numpy can't print as text arrays with dimensions > 2. We'll condense the last two just as an example. (the angles of each group are now together)
numpy.savetxt(md.opts.outfile, angles.reshape(len(md), (len(md.ndxgs)-1)*nbonds))
