basemean = 43000.0
baseerr  = basemean * 0.0133

import numpy
import math

def abserr(mean, abserr):
    return fracerr(mean, mean/abserr)

def fracerr(mean, fracerr):
    global basemean
    global baseerr

    print (mean/basemean) * numpy.sqrt(math.pow(fracerr, 2) + math.pow(baseerr/basemean, 2))

def fracerr(mean, err):
    global basemean
    global baseerr

    print (mean/basemean) * numpy.sqrt(math.pow(err/mean, 2) + math.pow(baseerr/basemean, 2))

    
