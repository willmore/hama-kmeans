#! /usr/bin/python2.7

from numpy import *
import pylab as p
#import matplotlib.axes3d as p3
import mpl_toolkits.mplot3d.axes3d as p3
import struct
import os
import re
import matplotlib.pyplot as plt
import sys

dir = sys.argv[1]

fig=p.figure()
ax = p3.Axes3D(fig)

cmap=plt.get_cmap('spectral')

colors = ['b','g','c','m','y','k', "chartreuse", "DarkSeaGreen", "DarkSlateBlue"]
 
markers = ['s','o','^','>','v' , '<' ,'d','p','h','8','+']

files = [os.path.join(dir, f) for f in os.listdir(dir)]

crcRe = re.compile(".*\.crc")

for i,file in enumerate(files):
   
    #ignore .crc files
    if crcRe.match(file):
        continue;
   
    xs = []
    ys = []
    zs = []
    f = open(file, 'r')
    
    #First point in file is center
    s = f.read(24)
    x,y,z = struct.unpack(">ddd", s)
    ax.scatter([x], [y], [z], c='r', marker='x') 
    
    #Now handle rest of cluster
    while True:
        s = f.read(24)
        if len(s) == 0:
            break;
        x,y,z = struct.unpack(">ddd", s)
        xs.append(x)
        ys.append(y)
        zs.append(z)
    
    ax.scatter(xs, ys, zs, c=colors[i], marker=markers[i], edgecolors='none') 
      

ax.set_xlabel('X')
ax.set_ylabel('Y')
ax.set_zlabel('Z')

p.show()

