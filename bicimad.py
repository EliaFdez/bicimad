import sys
from pyspark import SparkContext
import json, os
import matplotlib.pyplot as plt
import numpy as np

def getDate(str):
    date = str.split('T')
    return tuple(date)

def getInfo(line):
  data = json.loads(line)
  user = data["user_day_code"]
  station0 = data["idplug_station"]
  station1 = data["idunplug_station"]
  time = data["travel_time"]
  start = getDate(data["unplug_hourTime"]["$date"])
  usertype = data["user_type"]
  return station0, station1, time, start, usertype

def flujo_diario(rdd):
    # devuelve un rdd con las estaciones y una lista con las fechas y el flujo diario por cada estación.
    rdd_date = rdd.map(lambda x: (x[4], tuple(x[:4])))\
        .filter(lambda x: (x[0] == 1))
    rdd0 = rdd_date.map(lambda x: ((x[1][3][0], x[1][0]), 1))\
        .groupByKey()\
        .mapValues(len)
    rdd1 = rdd_date.map(lambda x: ((x[1][3][0], x[1][1]), 1))\
        .groupByKey()\
        .mapValues(len)    
    rdd_final = rdd0.join(rdd1)\
        .map(lambda x: (x[0], x[1][0]-x[1][1]))\
        .map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .groupByKey()\
        .map(lambda x: (x[0], list(x[1])))
        
    return rdd_final

def flujo(rdd_date):
    rdd = rdd_date.map(lambda x: (x[1], x[0]))
    rdd_station0 = rdd_date.map(lambda x: (x[0], 1))\
        .groupByKey()\
        .mapValues(len)
    rdd_station1 = rdd_date.map(lambda x: (x[1], 1))\
        .groupByKey()\
        .mapValues(len)
    rdd_flujo = rdd_station0.join(rdd_station1)\
        .map(lambda x: (x[0], x[1][0]-x[1][1]))
        
    return rdd_flujo

def set_values(stas, vals):
    max_station = 0
    for s in stas:
        if s[-1]>max_station:
            max_station = s[-1]

    stations = list(range(max_station))
    aux_vals = [0 for i in range(max_station)]
    for s in stations:
        stations[s] += 1
        
    for v in range(len(vals)):
        j = 0
        for i in range(len(stations)):
            if j<len(stas[v]) and stations[i] == stas[v][j]:
                aux_vals[i] = vals[v][j]
                j += 1

        vals[v] = aux_vals
        aux_vals = [0 for i in range(max_station)]

    return stations, vals

def plot_day(date, data):
    # data es una lista cuyos elementos son una tupla de (estación, flujo).

    estaciones = []
    flujos = []
    for d in data:
        estaciones.append(str(d[0]))
        flujos.append(d[1])

    plt.bar(estaciones, flujos)
    plt.axhline(y=0, color='black')
    plt.xlabel('estaciones')
    plt.xticks(estaciones, estaciones, fontsize=8, rotation=90)
    plt.ylabel('flujo')
    plt.title(date)
    plt.show()
        

def main(date, file):

    sc = SparkContext()
    rdd = sc.textFile(file).map(getInfo)
    rdd_date = flujo_diario(rdd)\
        .filter(lambda x: (x[0] == date))\
        .map(lambda x: x[1])\
        .flatMap(lambda x: x)\
        .sortByKey()
    
    data = rdd_date.collect()
    # print(data)
    plot_day(date ,data)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])