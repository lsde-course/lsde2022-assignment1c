from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys


# should return a pyspark.sql.DataFrame
def cruncher(spark, datadir, a1, a2, a3, a4, d1, d2):
    person = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema",
                                                                                               "true").load(
        datadir + "/person*.csv*")
    interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema",
                                                                                                 "true").load(
        datadir + "/interest*.csv*")
    knows = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema",
                                                                                              "true").load(
        datadir + "/knows*.csv*")

    # select the relevant (personId, interest) tuples,
    # and add a boolean column "nofan" (true iff this is not a a1 tuple)
    focus = interest \
        .filter(col("interest").isin(a1, a2, a3, a4)) \
        .withColumn("nofan", col("interest") != a1)

    # compute person score (#relevant interests):
    # join with focus, groupby & aggregate. Note: nofan=true iff person does not like a1
    scores = person.join(focus, "personId") \
        .groupBy("personId", "locatedIn", "birthday") \
        .agg(count("personId").alias("score"), min("nofan").alias("nofan"))

    # filter (personId, score, locatedIn) tuples with score>1, being nofan, and having the right birthdate
    cands = scores.filter((col("score") > 0) & col("nofan")) \
        .withColumn("bday", month(col("birthday")) * 100 + dayofmonth(col("birthday"))) \
        .filter((d1 <= col("bday")) & (col("bday") <= d2))

    # create (personId, ploc, friendId, score) pairs by joining with knows (and renaming locatedIn into ploc)
    pairs = cands.select(col("personId"), col("locatedIn").alias("ploc"), col("score")) \
        .join(knows, "personId")

    # re-use the scores dataframe to create a (friendId, floc) dataframe of persons who are a fan (not nofan)
    fanlocs = scores.filter(~ col("nofan")) \
        .select(col("personId").alias("friendId"), col("locatedIn").alias("floc"))

    # join the pairs to get a (personId, ploc, friendId, floc, score),
    # and then filter on same location, and remove ploc and floc columns
    results = pairs.join(fanlocs, "friendId") \
        .filter(col("ploc") == col("floc")) \
        .select(col("personId").alias("p"), col("friendId").alias("f"), col("score"))

    #  do the bidirectionality check by joining towards knows,
    #  and keeping only the (p, f, score) pairs where also f knows p
    bidir = results.join(knows.select(col("personId").alias("f"), col("friendId")), "f") \
        .filter(col("p") == col("friendId"))

    # keep only the (p, f, score) columns and sort the result
    ret = bidir.select(col("score"), col("p"), col("f"), ).orderBy(desc("score"), asc("p"), asc("f"))

    return ret


def run_cruncher(spark, datadir, query_file_path, results_file_path, number_of_queries = 10):
    query_file = open(query_file_path, 'r')
    results_file = open(results_file_path, 'w')

    i = 0
    for line in query_file.readlines():
        i = i + 1
        q = line.strip().split("|")

        # parse rows like: 1|1989|1990|5183|1749|2015-04-09|2015-05-09
        qid = int(q[0])
        a1 = int(q[1])
        a2 = int(q[2])
        a3 = int(q[3])
        a4 = int(q[4])
        d1 = 100 * (int(q[5][5:7])) + int(q[5][8:10])
        d2 = 100 * (int(q[6][5:7])) + int(q[6][8:10])

        print(f"Processing Q{qid}")
        result = cruncher(spark, datadir, a1, a2, a3, a4, d1, d2)

        # write rows like: Query.id|TotalScore|P|F
        for row in result.collect():
            results_file.write(f"{qid}|{row[0]}|{row[1]}|{row[2]}\n")
        
        if i >= number_of_queries:
            break

    query_file.close()
    results_file.close()

    # return the last result
    return result


def main():
    if len(sys.argv) < 4:
        print("Usage: cruncher.py [datadir] [query file] [results file]")
        sys.exit()

    datadir = sys.argv[1]
    query_file_path = sys.argv[2]
    results_file_path = sys.argv[3]

    spark = SparkSession.builder.getOrCreate()


if __name__ == "__main__":
    main()
