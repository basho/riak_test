#!/usr/bin/env python

from csv import reader, writer
from glob import glob
from sys import argv as args, exit

interval = 10

def mem(l):
    #make into a percentage
    ## note that this is ONLY true for the current perf setup
    free = float(l[21]) 
    ##print free, free/ (48*1024*1024*1024)
    return (free / (48*1024*1024*1024)) * 100

def cpu(l):
    return 100 - float(l[2])

def disk_util(l):
    return sum(map(float, l[42:]))/8

def page_faults(l):
    maj = float(l[26]) * 3000 
    min = float(l[27])
    return maj + min

statlist = ['memfree', 'cpu-util', 'iowait', 'disk-util', 'page-faults']
statcol  = [      mem,        cpu,        3,   disk_util,   page_faults] 

def digest(in_list, active_count, outfile):
    outfile.writerow(statlist) 
    while active_count > 0:
        
        acc = [0.0]*(len(statlist)) 
        for i in range(interval):
            acc_t = [0.0]*(len(statlist)) 
            for r in in_list: 
                try:
                    statline = r.next()
                    #print statline
                    for n, stat in enumerate(statcol): 
                        if type(stat) is int:
                            acc_t[n] = float(statline[stat])
                        else:
                            acc_t[n] = stat(statline)
                except StopIteration:
                    active_count -= 1

            #print acc_t
            for n in range(len(statlist)):
                acc[n] += acc_t[n]

        #this isn't going to work correctly for the end
        for n in range(len(statlist)):
            acc[n] = acc[n]/interval
        #print acc
        outfile.writerow(acc)


def main(dir):
    files = glob(dir+'/dstat-10*')

    readers = []
    for file in files: 
        r = reader(open(file))
        #discard the first lines of informational output
        for i in range(7):
            r.next()
        readers.append(r)


    out = writer(open('dstat-digest', 'w'), delimiter = ' ')
    count = len(readers)
    
    digest(readers, count, out)



if __name__ == '__main__':
    if len(args) == 2 and type(args[1]) is str:
        main(args[1])
    else:
        exit(1)
