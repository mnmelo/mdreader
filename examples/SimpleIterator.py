#!/usr/bin/env python
import mdreader
"""
This example simply iterates through a trajectory, doing nothing.
The fm Timestep object contain several trajectory-related attributes
(see the MDAnalysis documentation).
Access to coordinates can be achieved by defining an AtomGroup selection
before iteration, which will then have its coordinates updated at each cycle.
"""

md = mdreader.MDreader()
for fm in md.iterate(p=1):
    pass
