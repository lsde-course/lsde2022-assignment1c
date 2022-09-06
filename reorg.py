from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys


def reorg(spark, datadir):
    # nothing here (yet)
    pass


def main():
    if len(sys.argv) < 2:
        print("Usage: reorg.py [datadir]")
        sys.exit()

    datadir = sys.argv[1]

    spark = SparkSession.builder.getOrCreate()

    reorg(spark, datadir)


if __name__ == "__main__":
    main()
