from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys
#import re

class Problem1:
    def run(self, inputPath, outputPath,inputPath2,k):
        spark = SparkSession.builder.master("local").appName("proj2df").getOrCreate()

        fileDF = spark.read.text(inputPath)
        wordsDF = fileDF.withColumn('year', split(col('value'),',').getItem(0)).withColumn('headline', split(col('value'), ',').getItem(1))
        wordsDF = wordsDF.select("year", "headline")

        wordsDF2 = wordsDF.withColumn("word",explode(split("headline","[ ]")))

        # remove the duplicate words in one headline
        wordsDF2 = wordsDF2.select("year", "headline","word").distinct().orderBy(col("year"))
        wordsDF2 = wordsDF2.select("year", "word")

        # remove the stopwords
        stop = spark.read.text(inputPath2)
        wordsDF3 = wordsDF2.join(stop, stop.value==wordsDF2.word,how='left')
        wordsDF3 = wordsDF3.where(wordsDF3.value.isNull()).select("year","word")

        # count the headlines have term t in specific year (find the TF)
        wordsDF4 = wordsDF3.withColumn("year", col("year").substr(0,4))
        wordsDF4 = wordsDF4.groupBy("year","word").count().toDF("year", "word","TF").orderBy(col("year"))

        # find the total number of the year
        total_year = wordsDF4.select("year").distinct().count()
        year_list = wordsDF4.select("year").distinct().sort(col("year")).take(total_year)

        # find the IDF
        wordsDF5 = wordsDF4.select("year","word").groupBy("word").count()
        wordsDF5 = wordsDF5.toDF("word2","count")

        wordsDF5 = wordsDF5.withColumn("count", col("count").cast("int"))
        wordsDF6 = wordsDF5.withColumn("IDF",   log10(total_year/col("count")     )  ).select("word2","IDF")


        # get the final result
        final =  wordsDF4.join(wordsDF6, wordsDF4.word==wordsDF6.word2,how='left')
        final = final.select("year","word","TF","IDF")
        final = final.withColumn("WT",round( col("TF")*col("IDF") , 6 )  )

        # sort the order of the result
        final = final.sort(col("year"),-col("WT"),col("word"))

        # print the output
        output_list = []
        for i in range(int(total_year)):

            res = final.select("year", "word", "WT").where(final.year == year_list[i].year).take(int(k))
            output_str = str(res[0].year) + "\t"

            for j in range(int(k)):
                t = str(res[j].word)
                w = str(res[j].WT)

                output_str = output_str + t +","+ w
                if j != int(k)-1:
                    output_str = output_str + ";"
            output_list.extend([output_str])

        last = spark.createDataFrame(output_list, "string").toDF("string")
        last.write.format("text").save(outputPath)



        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    Problem1().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

