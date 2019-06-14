#!/usr/bin/env python

import sys
from os import remove, removedirs
from os.path import dirname, join, isfile
from time import time

topBooks = """1, Classical Mythology by Mark P. O. Morford 2002 
65, The Dragons of Eden: Speculations on the Evolution of Human Intelligence by Carl Sagan 1978
97,Fast Women by Jennifer Crusie 2001
176,The Martian Chronicles by RAY BRADBURY 1984
222,Bleachers by John Grisham 2003
247,Life of Pi by Yann Martel 2002
255,Night Watch by Terry Pratchett 2002
278,Martian Chronicles by Ray Bradbury 1997
319,Angel of Hope (Mercy Trilogy) by Lurlene McDaniel 2000
327,Wolf Moon by John R. Holt 1997
359,Digital Fortress : A Thriller by Dan Brown 2003"""

parentDir = dirname(dirname(__file__))
ratingsFile = join(parentDir, "src/datas/personalRatings.txt")

if isfile(ratingsFile):
    r = input("Looks like you've already rated the books. Overwrite ratings (y/N)? ")
    if r and r[0].lower() == "y":
        remove(ratingsFile)
    else:
        sys.exit()

prompt = "Please rate the following book (1-10 (best), or 0 if not read): "
print(prompt)

now = int(time())
n = 0

f = open(ratingsFile, 'w')
for line in topBooks.split("\n"):
    ls = line.strip().split(",")
    valid = False
    while not valid:
        rStr = input(ls[1] + ": ")
        r = int(rStr) if rStr.isdigit() else -1
        if r < 0 or r > 10:
            print(prompt)
        else:
            valid = True
            if r > 0:
                f.write("0;%s;%d;%d\n" % (ls[0], r, now))
                n += 1
f.close()

if n == 0:
    print("No rating provided!")