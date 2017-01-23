#!/usr/bin/env python
import mdreader
import numpy
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
"""
Number-density profile calculation in z.
Extra options are defined (argparse syntax) and a default index setting with
 an arbitrary number of groups is set up.
Iteration is done only as coordinates/dimensions extraction.
"""
# Initialize the thing
md = mdreader.MDreader()

# Let's add the slice selection properly
md.add_argument("-bw", dest="bw",type=float,default=0.1,help="Bin width")
md.add_argument("-prec", dest="prec",type=int,default=2,help="Trajectory precision (number of decimal places)")
md.add_argument("-oplot", dest="oplot",default="dens",help="Output plots filename (no extension)")
md.add_ndx(ng='n')

tseries = md.timeseries(md.ndxgs[0],props="dimensions", x=False, y=False)
box_z = tseries.dimensions[:,2]/10
z = tseries.coords.reshape(len(md),len(md.ndxgs[0]))/10

cogs = numpy.average(z, axis=1)

centered = z - cogs[:,None]
maxz = numpy.max(numpy.abs(centered))
maxz = round(maxz,md.opts.prec)+(10**(-md.opts.prec))/2
bins = numpy.arange(-maxz,maxz+md.opts.bw,md.opts.bw)

hist, edges = numpy.histogram(centered,bins)

# Numpy way of saving the output
numpy.savetxt(md.opts.outfile, numpy.vstack((bins[:-1]+md.opts.bw/2,hist)).T)

# Plot the histogram
fig, ax = plt.subplots(1)
ax.plot(bins[:-1]+md.opts.bw/2, hist, linewidth=2)
ax.fill_between(bins[:-1]+md.opts.bw/2, hist, 0 ,alpha=0.4)
fig.savefig('{}.eps'.format(md.opts.oplot), transparent='True')
fig.savefig('{}.png'.format(md.opts.oplot), transparent='True')


