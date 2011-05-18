from numpy import *
import pylab as p
#import matplotlib.axes3d as p3
import mpl_toolkits.mplot3d.axes3d as p3
import struct
import os
import re
import matplotlib.pyplot as plt

fig=p.figure()
ax = p3.Axes3D(fig)

cmap=plt.get_cmap('spectral')

colors = ['b','g','r','c','m','y','k', "chartreuse", "DarkSeaGreen", "DarkSlateBlue"]
 
markers = ['s','o','^','>','v' , '<' ,'d','p','h','8','+','x']

dir = "/tmp/1305737986917/local"
files = [os.path.join(dir, f) for f in os.listdir(dir)]

crcRe = re.compile(".*\.crc")



for i,file in enumerate(files):
   
    if crcRe.match(file):
        continue;
   
    xs = []
    ys = []
    zs = []
    f = open(file, 'r')
    
    print "Parsing file " + file

    while True:
        s = f.read(24)
        if len(s) == 0:
            break;
        x,y,z = struct.unpack(">ddd", s)
        xs.append(x)
        ys.append(y)
        zs.append(z)
    print markers[i]
    ax.scatter(xs, ys, zs, c=colors[i], marker=markers[i]) 
      

ax.set_xlabel('X')
ax.set_ylabel('Y')
ax.set_zlabel('Z')

p.show()

