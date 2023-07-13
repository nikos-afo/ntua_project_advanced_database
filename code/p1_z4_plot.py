from wsgiref.simple_server import WSGIRequestHandler
import matplotlib.pyplot as plt
import numpy as np

query = ["Q1", "Q2", "Q3", "Q4", "Q5"]
cases = ["rdd", "sql_csv", "sql_par"]
for q in query:
    plt.clf()
    seconds = []
    for c in cases:
       file = open(q + "_" + c +".txt", "r")
       seconds.append(float(file.read().split('\n')[0]))
       file.close() 
    plt.bar(cases, seconds)
    plt.savefig(q + '.png')