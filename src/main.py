import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType
from pyspark.sql.functions import expr, broadcast, desc, asc
from pyspark.sql.functions import pandas_udf

spark = SparkSession.builder.getOrCreate()


def main():

    def add_series(*marks: pd.Series) -> pd.Series:
        return pd.concat(objs=list(marks), axis=1).sum(axis=1)

    def get_result(*marks: pd.Series) -> pd.Series:
        min_series: pd.Series = pd.concat(objs=list(marks), axis=1).min(axis=1)

        def compute_result(x):
            return 'F' if x < 40 else 'P'
        return min_series.map(compute_result)

    """Schema for the two dataframes"""
    rollno_schema = StructType([StructField('roll_no', IntegerType(), True),
                                StructField('name', StringType(), True)])
    marks_schema = StructType([StructField('roll_no', IntegerType(), True),
                               StructField('mark_1', IntegerType(), True),
                               StructField('mark_2', IntegerType(), True),
                               StructField('mark_3', IntegerType(), True),
                               StructField('mark_4', IntegerType(), True),
                               StructField('mark_5', IntegerType(), True),
                               StructField('mark_6', IntegerType(), True),
                               StructField('sem', IntegerType(), True),
                               StructField('total', IntegerType(), False),
                               StructField('percentage', FloatType(), False),
                               StructField('result', StringType(), False)
                               ])

    rollno_df = spark.read.format("org.apache.spark.csv").option("header", "false").option("delimiter", "	").schema(
        rollno_schema).csv("StudentRollNo.txt")
    marks_df = spark.read.format("org.apache.spark.csv").option("header", "false").option("delimiter", "	").schema(
        marks_schema).csv("StudentMarks.txt")

    """Slicing the Mark columns explicitly"""
    mark_cols = marks_df.columns[1:7]

    pandas_add = pandas_udf(add_series, returnType=LongType())
    pandas_get_result = pandas_udf(get_result, returnType=StringType())

    marks_df = marks_df.withColumn('mark_1', expr('COALESCE(mark_1, 0)'))\
        .withColumn('mark_2', expr('COALESCE(mark_2, 0)'))\
        .withColumn('mark_3', expr('COALESCE(mark_3, 0)'))\
        .withColumn('mark_4', expr('COALESCE(mark_4, 0)'))\
        .withColumn('mark_5', expr('COALESCE(mark_5, 0)'))\
        .withColumn('mark_6', expr('COALESCE(mark_6, 0)')) \
        .withColumn('total', pandas_add('mark_1', 'mark_2', 'mark_3', 'mark_4', 'mark_5', 'mark_6')) \
        .withColumn('percentage', expr(f"total*100/({len(mark_cols)}*100)")) \
        .withColumn('result', pandas_get_result('mark_1', 'mark_2', 'mark_3', 'mark_4', 'mark_5', 'mark_6'))

    """The smaller dataframe is broad casted here to improve the performance"""
    result_df = marks_df.join(broadcast(rollno_df), ['roll_no'], how='inner') \
        .select('roll_no', 'name', 'sem', 'total', 'percentage', 'result').orderBy(asc('sem'), desc('total'))

    result_df.show()


if __name__ == '__main__':
    main()

