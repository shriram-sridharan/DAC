#!/usr/bin/python

import sys
if(len(sys.argv) != 5):
    print "Usage: auth.py inpfile outfile authvec sep"
    print "eg: auth.py usertable.keys key.with.authvector 1111111111 ,"
    exit(1)


infile = open(str(sys.argv[1]), 'r')
outfile = open(str(sys.argv[2]), 'w')
lines = infile.readlines()
lines = [line.strip() for line in lines]
authvec = (str(sys.argv[4]) + str(sys.argv[3] )) * 16
outlist = []
for line in lines:
    s = line + authvec
    outlist.append(s)
    print s

#for item in outlist:
#      outfile.write("%s\n" % item)
