from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

spark = SparkSession.builder.getOrCreate()


def main():

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
        rollno_schema).csv("resources/StudentRollNo.txt")
    marks_df = spark.read.format("org.apache.spark.csv").option("header", "false").option("delimiter", "	").schema(
        marks_schema).csv("resources/StudentMarks.txt")

    """Registering the Temporary Views"""
    rollno_df.createOrReplaceTempView("rollno_df")
    marks_df.createOrReplaceTempView("marks_df")

    """Calculating the Results with Spark SQL API"""
    result_df = spark.sql("""
    
        SELECT 
            /*+  BROADCASTJOIN(rollno_df) */
            rollno_df.name AS name,
            rollno_df.roll_no as roll_no,
            marks_df.sem as sem,
            ( COALESCE(mark_1, 0) + 
              COALESCE(mark_2, 0) + 
              COALESCE(mark_3, 0) + 
              COALESCE(mark_4, 0) + 
              COALESCE(mark_5, 0) + 
              COALESCE(mark_6, 0)
            ) as total,
            ( COALESCE(mark_1, 0) + 
              COALESCE(mark_2, 0) + 
              COALESCE(mark_3, 0) + 
              COALESCE(mark_4, 0) + 
              COALESCE(mark_5, 0) + 
              COALESCE(mark_6, 0)
            )*100/(6*100) as percentage,
            CASE
                WHEN LEAST( COALESCE(mark_1, 0) , 
                  COALESCE(mark_2, 0) ,
                  COALESCE(mark_3, 0) , 
                  COALESCE(mark_4, 0) , 
                  COALESCE(mark_5, 0) , 
                  COALESCE(mark_6, 0)
                ) < 40 THEN 'F'
                ELSE 'P'
            END as result
        FROM marks_df
        INNER JOIN rollno_df ON marks_df.roll_no = rollno_df.roll_no
        ORDER BY marks_df.sem ASC, total DESC
            
    """)

    result_df.show()


if __name__ == '__main__':
    main()

