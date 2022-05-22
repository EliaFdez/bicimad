import json, os
import sys
import random
from pyspark import SparkContext

N = 10

def main(files):
    sc = SparkContext()
    final = []
    for f in files:
        percent = random.random()/N

        sample = sc.textFile(f)
        count = sample.count()

        sample = sample.takeSample(False,int(percent*count),0)
        sample = map(json.loads, sample)
        final.append(list(sample))
    
    final = [item for sublist in final for item in sublist]

    with open('json_data.json', "w") as file:
        for i in final:
            obj = json.dumps(i)
            file.write(obj)
            file.write('\n')

if __name__ == "__main__":
    main(sys.argv[1:])