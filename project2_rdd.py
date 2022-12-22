from pyspark import SparkContext, SparkConf
#from operator import add
import sys
from math import log10


class Problem1:
    def run(self, inputPath, outputPath,inputPath2,k):
        conf = SparkConf().setAppName("project2")
        sc = SparkContext(conf=conf)

        file = sc.textFile(inputPath)
        stop = sc.textFile(inputPath2)
        stopwords =stop.map(lambda line: (line.split("\n")[0],1))

        fields = file.map(lambda line: (line.split(",")[0][:4], line.split(",")[1].split(" ")))
        fields2 = fields.map(lambda x:(x[0],list(dict.fromkeys(x[1]))))     #remove the duplicate term in a headline
        pairs = fields2.flatMap(lambda x: [(i,x[0]) for i in x[1]])         #create (term,year) key-value pairs

        rf = pairs.subtractByKey(stopwords)
        rf = rf.map(lambda x: ( (x[0],x[1]) ,1))

        df = rf.reduceByKey(lambda x,y: x+y)

        #get the total year
        test1 = df.map(lambda x:(   x[0][1] ,1 )  )
        test1 = test1.reduceByKey(lambda x,y:x+y)
        total_year =test1.count()

        #get the divider
        rf2 = df.map(lambda x:(x[0][0], set(  [  x[0][1] ]  )   ))
        rf3 = rf2.reduceByKey(lambda x,y: set(x | y))
        divider = rf3.map(lambda x: (x[0],len(x[1])))

        #get the IDT
        idt = divider.map(lambda x:(x[0],log10(  total_year/int(x[1]) )     ))

        #get the TF
        tf = df.map(lambda x: (x[0][0], (x[0][1],x[1]) ) )

        final = tf.join(idt)
        final2 = final.map(lambda x: (x[1][0][0], int(x[1][0][1]) * x[1][1],x[0]) )

        #sort the result
        final2 = final2.sortBy(lambda x: x[2],ascending = True)
        final2 = final2.sortBy(lambda x: x[1], ascending=False)
        final2 = final2.sortBy(lambda x: x[0], ascending=True)

        #put (term,weight) in a list
        final3 = final2.map(lambda x:(x[0],(  x[1],x[2] )  ))
        final4 = final3.groupByKey().mapValues(list)

        #take the Top-k (term,weight) pairs
        final5 = final4.map(lambda x: (x[0],[  str(x[1][i][1])+","+str(round(x[1][i][0],6)    ) for i in range(int(k))     ] ))

        #sort the key(year) again
        final5 = final5.sortBy(lambda x: x[0], ascending=True)


        final6 = final5.map(lambda x: f"{x[0]}\t{';'.join([str(item) for item in x[1]])}")
        final6.repartition(1).saveAsTextFile(outputPath)

        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    else:
        Problem1().run(sys.argv[1], sys.argv[2], sys.argv[3],  sys.argv[4])
