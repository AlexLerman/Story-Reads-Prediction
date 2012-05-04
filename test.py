#!/usr/bin/python2.5
#PYTHONPATH='/Library/Frameworks/Python.framework/Versions/2.5/lib/python2.5/site-packages/'
import sys
sys.path.append('/Library/Frameworks/Python.framework/Versions/2.5/lib/python2.5/site-packages/')
sys.path.append('/Library/Frameworks/Python.framework/Versions/2.5/lib/python2.5/site-packages/gensim-0.8.3-py2.5.egg')
#sys.path.remove('/Library/Python/2.5/site-packages/gensim-0.8.3-py2.5.egg')
sys.path.append('/Users/Alex/Downloads/liblinear-1.8/python')
#print sys.path

from gensim import corpora, models, similarities
from liblinearutil import *
from liblinear import *
import numpy
import tokenize
import logging
import sqlite3
import random
from scipy.sparse import *
from scipy import *
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


Testvector=[{3: 0.34, 5: 0.56}, {2: 0.34, 6: 0.56, 11: 0.1}, {3: 0.34, 5: 0.56}, {2: 0.34, 6: 0.56, 11: 0.1},]
print Testvector

labels=[1, 1, -1, -1]

prob= problem(labels, Testvector)

param = parameter('-s 0')

modellog = train(prob, param)
lp_labs, lp_acc, lp_vals = predict(labels, Testvector, modellog)
print lp_vals
