import os
import sys
import pylab
import __main__ as main

def setprofilerdir():
    global profiler_dir
    if len(sys.argv) > 1:
        profiler_dir = sys.argv[1] + '/'
    else:
        profiler_dir = './profiler_results'

def listfiles(path):
    from os import listdir
    from os.path import isfile, join
    onlyfiles = [ f for f in listdir(path) if isfile(join(path,f)) ]
    return onlyfiles

def getMeanAndErr(prefix, index):

    path  = profiler_dir + '/segment_' + prefix + '/'

    if(os.path.exists(path)):
        valid = True;
    else:
        return (0, 0, False)

    files = listfiles(path)

    vals = []
    n = pylab.size(files)
    print 'Calculating mean for segment: ' + prefix

    for i in range(0,n):
        data = pylab.loadtxt(path + files[i])

        # Convert to ops per second

        ndata = pylab.size(data)
#        print str(ndata)
        if ndata == 21:
 #           print pylab.size(data[index])
            if data[index] > 0:
                vals.append(data[index])

    n    = pylab.size(vals)
    mean = pylab.mean(vals)
    rms  = pylab.std(vals)/pylab.sqrt(n)

    return (mean, rms, valid)

def getVals(prefices):

    means  = []
    errs   = []
    valids = []

    n = pylab.size(prefices)
    for i in range(0,n):
        print 'About to get mean for prefix = ' + prefices[i]
        (mean, err, valid) = getMeanAndErr(prefices[i], 1)
        print 'Got mean for prefix = ' + prefices[i] + ' mean = ' + str(mean) + ' err = ' + str(err)
        means.append(mean)
        errs.append(err)
        valids.append(valid)

    return (means, errs, valids)

def meanerr(err1, err2, fac):
    return pylab.sqrt(err1*err1 + err2*err2) / fac

def display(prefices, means, errs, valids):
    
    fig = pylab.figure()
    sub = fig.add_subplot(1,1,1)

    n = pylab.size(prefices)
    xvals = range(0,n)

    dm = {}
    de = {}

    for i in range(0,n):
        if(valids[i]):
            dm[prefices[i]] = means[i];  
            de[prefices[i]] = errs[i];  
      
    pyvals = []
    pevals = []
    labels = []
    colors = []

    enone = de['none']

    if('clientnosend' in dm.keys()):
        pyvals.append(dm['clientnosend']/dm['none']);
        labels.append("riakc\_ts:put() to gen\_tcp:send()")
        colors.append('b')
        e = de['clientnosend']/dm['none'];
        pevals.append(e)

    clientnosend = dm['clientnosend'];

    pyvals.append((dm['clientmultserialize'] - dm['none'])/(dm['none']*100))
    labels.append("client serialize put req")
    colors.append('c')
    e1 = de['clientmultserialize']
    e2 = de['none']
    e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none']*100)
    pevals.append(e)

    clientserialize = (dm['clientmultserialize'] - dm['none'])/100

    if('clientmultencode' in dm.keys()):
        pyvals.append((dm['clientmultencode'] - dm['none'])    / (dm['none']*100))
        labels.append("client encode put req")
        colors.append('c')
        e1 = de['clientmultencode']
        e2 = de['none']
        e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none']*100)
        pevals.append(e)

    clientencode = (dm['clientmultencode'] - dm['none'])/100

    if('clientmultdecode' in dm.keys()):
        pyvals.append((dm['clientmultdecode'] - dm['none'])    / (dm['none']*1000))
        labels.append("client decode put resp")
        colors.append('c')
        e1 = de['clientmultdecode']
        e2 = de['none']
        e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none']*1000)
        pevals.append(e)

    clientdecode = (dm['clientmultdecode'] - dm['none'])/1000

    pyvals.append((dm['clientmultdeserialize'] - dm['none'])/(dm['none']*100))
    labels.append("client deserialize put resp")
    colors.append('c')
    e1 = de['clientmultdeserialize']
    e2 = de['none']
    e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none']*100)
    pevals.append(e)

    clientdeserialize = (dm['clientmultdeserialize'] - dm['none'])/100

    clientgenserver = (clientnosend - clientserialize - clientdeserialize - clientencode - clientdecode)/dm['none'];

    pyvals.append(clientgenserver)
    labels.append("client gen\_server calls")
    colors.append('c')
    pevals.append(e)

    if('clientnosend' in dm.keys() and 'process' in dm.keys()):
        pyvals.append((dm['process'] - dm['clientnosend'])/dm['none']);
        labels.append("gen\_tcp:send() to riak\_kv:process()")
        colors.append('b')
        e1 = de['clientnosend']/dm['none'];
        e2 = de['process']
        e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none'])
        pevals.append(e)

    serverprocess = dm['process'] - dm['clientnosend']

    pyvals.append((dm['pbservermultencode'] - dm['none'])/(dm['none']*1000))
    labels.append("server encode put resp")
    colors.append('c')
    e1 = de['pbservermultencode']
    e2 = de['none']
    e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none']*1000)
    pevals.append(e)

    serverencode = (dm['pbservermultencode'] - dm['none'])/1000

    pyvals.append((dm['pbservermultdecode'] - dm['none'])/(dm['none']*10))
    labels.append("server decode put req");
    colors.append('c')
    e1 = de['pbservermultdecode']
    e2 = de['none']
    e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none']*10)
    pevals.append(e)

    serverdecode = (dm['pbservermultencode'] - dm['none'])/10

    servergenserver = (serverprocess - serverencode - serverdecode)/dm['none'];
    pyvals.append(servergenserver)
    labels.append("server gen\_server calls (+ TCP)");
    colors.append('c')
    pevals.append(e)

    pyvals.append((dm['asyncput'] - dm['process'])/dm['none'])
    labels.append("riak\_kv:process() to riak\_kv:async\_put()")
    colors.append('b')
    print 'Err = ' + str(meanerr(de['asyncput'], de['process'], dm['none']))
    pevals.append(meanerr(de['asyncput'], de['process'], dm['none']))

    if('process_multblock1' in dm.keys()):
        pyvals.append((dm['process_multblock1'] - dm['none']) / (dm['none']*100))
        labels.append("module\_name + decode\_rows")
        colors.append('c')
        pevals.append(meanerr(de['process_multblock1'], de['none'], dm['none']*100))

    if('process_multblock2' in dm.keys()):
        pyvals.append((dm['process_multblock2'] - dm['none']) / (dm['none']*1000))
        labels.append("validate\_object")
        colors.append('c')
        pevals.append(meanerr(de['process_multblock2'], de['none'], dm['none']*100))

#    if('process_multblock3' in dm.keys()):
#        pyvals.append((dm['process_multblock3'] - dm['none']) / (dm['none']*100))
#        labels.append("block3")
#        colors.append('c')
#        pevals.append(meanerr(de['process_multblock3'], de['none'], dm['none']*100))

    if('process_multblock3encode' in dm.keys()):
        pyvals.append((dm['process_multblock3encode'] - dm['none']) / (dm['none']*100))
        labels.append("eleveldb\_ts:encode\_key()")
        colors.append('c')
        pevals.append(meanerr(de['process_multblock3encode'], de['none'], dm['none']*100))

    if('process_multblock3getkeys' in dm.keys()):
        pyvals.append((dm['process_multblock3getkeys'] - dm['none']) / (dm['none']*100))
        labels.append("riak\_ql\_ddl:get\_partition+local\_key()")
        colors.append('c')
        pevals.append(meanerr(de['process_multblock3getkeys'], de['none'], dm['none']*100))

    if('process_multblock4' in dm.keys()):
        pyvals.append((dm['process_multblock4'] - dm['none']) / (dm['none']*100))
        labels.append("riak\_object:new/update\_metadata")
        colors.append('c')
        pevals.append(meanerr(de['process_multblock4'], de['none'], dm['none']*100))

    pyvals.append((dm['eleveldb'] - dm['asyncput'])   /dm['none'])
    labels.append("riak\_kv:async\_put() to eleveldb:async\_put()")
    colors.append('b')
    e1 = de['asyncput']
    e2 = de['eleveldb']
    e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none'])
    pevals.append(e)

    pyvals.append((dm['asyncput_trunc1'] - dm['asyncput'])    /dm['none'])
    labels.append("riak\_kv:async\_put() through get\_ts\_local\_key")
    colors.append('c')
    e1 = de['asyncput']
    e2 = de['asyncput_trunc1']
    e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none'])
    pevals.append(e)

    pyvals.append((dm['asyncput_trunc2'] - dm['asyncput_trunc1'])    /dm['none'])
    labels.append("riak\_kv:async\_put() from get\_ts\_local\_key through validate\_options")
    colors.append('c')
    e1 = de['asyncput']
    e2 = de['asyncput_trunc2']
    e = pylab.sqrt(e1*e1 + e2*e2)/(dm['none'])
    pevals.append(e)

    if('asyncput_trunc20' in dm.keys()):
        pyvals.append((dm['asyncput_trunc20'] - dm['asyncput_trunc2']) / dm['none'])
        labels.append("riak\_kv:async\_put() from validate\_options to apply\_updates")
        colors.append('c')
        pevals.append(meanerr(de['asyncput'], de['asyncput_trunc20'], dm['none']))

    if('asyncput_multblock1' in dm.keys()):
        pyvals.append((dm['asyncput_multblock1'] - dm['none']) / (dm['none'] * 100))
        labels.append("manipulate riak\_object"),
        colors.append('m')
        pevals.append(meanerr(de['asyncput_multblock1'], de['none'], dm['none']*100))

#    if('asyncput_multblock3' in dm.keys()):
#        pyvals.append((dm['asyncput_multblock3'] - dm['none']) / (dm['none'] * 100))
#        labels.append("riak\_kv:async\_put() block3"),
#        colors.append('c')
#        pevals.append(meanerr(de['asyncput_multblock3'], de['none'], dm['none']*100))

    pyvals.append((dm['asyncput_trunc3'] - dm['asyncput_trunc20']) / dm['none'])
    labels.append("riak\_kv:async\_put() from apply\_updates to end")
    colors.append('c')
    pevals.append(meanerr(de['asyncput_trunc3'], de['asyncput_trunc20'], dm['none']))

    if('multsext' in dm.keys()):
        pyvals.append((dm['multsext'] - dm['none']) / (dm['none']*100))
        print 'Multsext = ' + str((dm['multsext'] - dm['none']) / (dm['none']*100))
        labels.append("sext encoding")
        colors.append('c')
        pevals.append(meanerr(de['multsext'], de['none'], dm['none']*100))

    pyvals.append((dm['none'] - dm['eleveldb']) / dm['none'])
    labels.append("Leveldb writes")
    colors.append('b')
    pevals.append(meanerr(de['eleveldb'], de['none'], dm['none']))

    props = {'ha': 'left', 'va': 'bottom'};

    for i in range(0,pylab.size(pyvals)):
        pylab.bar(i, pyvals[i], width=0.5, bottom=None, hold=None, color=colors[i], yerr=pevals[i])
        pylab.text(float(i)-0.3, 0, labels[i], props, rotation=90)

    pylab.rc("text", usetex=True)
    pylab.rc('font', family='serif')
    pylab.rcParams['text.latex.preamble'] = [r'\boldmath']

    pylab.ylabel('$\Delta t (\%)$')

#    pylab.plot(pxvals, pyvals, 'b-')

    ax = pylab.axis()
#    xmin = ax[0]
#    xmax = ax[1]
    xmin = 0.0
    xmax = 23.0
    ymin = -0.02
    ymax =  0.40
    
    xr = xmax - xmin
    yr = ymax - ymax
    pylab.axis([xmin - xr*0.1, xmax + xr*0.1, ymin - yr*0.1, ymax + yr*0.1])

    pylab.rc("text", usetex=True)
    pylab.rc('font', family='serif')
    pylab.rcParams['text.latex.preamble'] = [r'\boldmath']

    ax2 = sub.twinx()
    ax2.set_ylim(ymin*dm['none']/1000, ymax*dm['none']/1000)

    pylab.ylabel('$\Delta t (\mu s)$')

    pylab.gca().axes.get_xaxis().set_visible(False)

    doBlock = hasattr(main, '__file__')
    pylab.show(block=doBlock)

#------------------------------------------------------------
# Main
#------------------------------------------------------------

setprofilerdir()

prefices = ['none', 'clientmultserialize', 'eleveldb', 'asyncput', 'process', 'clientmultdeserialize', 'pbservermultencode', 'pbservermultdecode', 'asyncput_trunc1', 'asyncput_multblock1', 'asyncput_multblock3', 'asyncput_trunc2', 'asyncput_trunc3', 'asyncput_trunc20', 'clientmultdecode', 'multsext', 'clientmultencode', 'clientnosend', 'process_multblock1', 'process_multblock2', 'process_multblock3', 'process_multblock4', 'process_multblock3encode', 'process_multblock3getkeys'];

(means, errs, valids) = getVals(prefices);

display(prefices, means, errs, valids)






