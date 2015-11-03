import pylab
import __main__ as main

def listfiles(path):
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [ f for f in listdir(path) if isfile(join(path,f)) ]
    return onlyfiles

def getMeanAndErr(vers, type, nrun, index):

    if vers == 'riak_2.1.1':
        if type == 'w1c':
            path = '/tmp/riak_test_scratch/ts_puttest_211_w1c_' + str(nrun) + '/'
        else:
            path = '/tmp/riak_test_scratch/ts_puttest_211_put_' + str(nrun) + '/'
    else:
        if type == 'w1c':
            path = '/tmp/riak_test_scratch/ts_puttest_ts_' + str(nrun) + '/'
        else:
            path = '/tmp/riak_test_scratch/ts_puttest_ts_put_' + str(nrun) + '/'

    files = listfiles(path)

    vals = []
    n = pylab.size(files)
    for i in range(0,n):
        data = pylab.loadtxt(path + files[i])
        print 'Checking file ' + path + files[i]

        # Convert to ops per second

        ndata = pylab.size(data)
        if ndata > 0:
            if data[index] > 0:
                vals.append(1e6 * nrun/data[index])

    n    = pylab.size(vals)
    mean = pylab.mean(vals)
    rms  = pylab.std(vals)/pylab.sqrt(n)

    return (mean, rms)

def getVals(vers, type, xvals):
    means = []
    errs = []

    n = pylab.size(xvals)
    for i in range(0,n):
        (mean, err) = getMeanAndErr(vers, type, xvals[i], 1)
        means.append(mean)
        errs.append(err)

    return (means, errs)

def display(vals):
    (xvals, y211_w1c, e211_w1c, yts_w1c, ets_w1c, y211_put, e211_put, yts_put, ets_put) = vals;

    fig = pylab.figure()
    ax = fig.add_subplot(1,1,1)

    pylab.rc("text", usetex=True)
    pylab.rc('font', family='serif')

    l0=pylab.plot( xvals, y211_w1c, 'b-')
    pylab.hold(True)
    l1=pylab.plot( xvals, yts_w1c,  'c-')
    l2=pylab.plot( xvals, y211_put, 'k-')
    l3=pylab.plot( xvals, yts_put,  'm-')

    pylab.legend(['riak 2.1.1 w1c', 'TS 1.0 w1c', 'riak 2.1.1 put', 'TW 1.0 put'], loc='lower right')
    
    pylab.errorbar(xvals, y211_w1c, e211_w1c, color='b')
    pylab.errorbar(xvals, yts_w1c,  ets_w1c,  color='c')
    pylab.errorbar(xvals, y211_put, e211_put, color='k')
    pylab.errorbar(xvals, yts_put,  ets_put,  color='m')

    pylab.hold(False)

    pylab.xlabel("$N_{keys}$")
    pylab.ylabel("Ops/sec")

    ax = pylab.axis()
    xr = ax[1] - ax[0]
    yr = ax[3] - ax[2]
    pylab.axis([ax[0] - xr*0.1, ax[1] + xr*0.1, ax[2] - yr*0.1, ax[3] + yr*0.1])

    doBlock = hasattr(main, '__file__')
    pylab.show(block=doBlock)

#------------------------------------------------------------
# Main
#------------------------------------------------------------

xvals = [250, 500, 1000, 1500, 5000, 10000, 20000, 30000, 40000]
(means_211_w1c, errs_211_w1c) = getVals('riak_2.1.1', 'w1c', xvals)
(means_ts_w1c,  errs_ts_w1c)  = getVals('riak_ts',    'w1c', xvals)
(means_211_put, errs_211_put) = getVals('riak_2.1.1', 'put', xvals)
(means_ts_put,  errs_ts_put)  = getVals('riak_ts',    'put', xvals)

display((xvals, means_211_w1c, errs_211_w1c, means_ts_w1c, errs_ts_w1c,
         means_211_put, errs_211_put, means_ts_put, errs_ts_put))






