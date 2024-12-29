# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read a CSV and print output to console 

# COMMAND ----------

# reading the csv file using the format method 

q1_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("dbfs:/FileStore/assignment/ecom_data.csv")
q1_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Read a JSON file and print output to console 
# MAGIC

# COMMAND ----------

q2_df = spark.read \
    .format("json") \
    .option("mode","DROPMALFORMED") \
    .load("dbfs:/FileStore/assignment/sample_file.json")
q2_df.show()
display(q2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Read parquet file and print output to console 
# MAGIC

# COMMAND ----------

q3_df = spark.read \
    .format("parquet") \
    .load("dbfs:/FileStore/assignment/weather.parquet")

q3_df.show()

# COMMAND ----------

q4_df = spark.read \
    .format("avro") \
    .load("dbfs:/FileStore/assignment/users.avro/part-00000-tid-8463009780074691585-d2a017c2-26db-4bbc-9c93-9a699fc53afa-123-1-c000.avro")

q4_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Example for broadcast join (Inner join 2 dataframes)

# COMMAND ----------

order_data = [("01", "02", 350, 1),
              ("01", "04", 580, 1),
              ("01", "07", 320, 2),
              ("02", "03", 450, 1),
              ("02", "06", 220, 1),
              ("03", "01", 195, 1),
              ("04", "09", 270, 3),
              ("04", "08", 410, 1)]

product_data = [("01", "Scroll Mouse", 250, 20),
                ("02", "Optical Mouse", 350, 20),
                ("03", "Wireless Mouse", 450, 50),
                ("04", "Wireless Keyboard", 580, 50),
                ("05", "Standard Keyboard", 360, 10),
                ("06", "16 GB Flash Storage", 240, 100),
                ("07", "32 GB Flash Storage", 320, 50),
                ("08", "64 GB Flash Storage", 430, 25)]

order_df = spark.createDataFrame(order_data).toDF("order_id", "prod_id", "unit_price", "qty")

product_df = spark.createDataFrame(product_data).toDF("prod_id", "prod_name", "list_price", "qty")

# handling column ambiguity 
renamed_product_df = product_df.withColumnRenamed("qty", "product_qty")

join_expr = order_df.prod_id == renamed_product_df.prod_id 

joined_df = order_df.join(renamed_product_df, join_expr, "inner") \
            .drop(renamed_product_df.prod_id) \
            .select("prod_id", "prod_name","list_price","order_id")

joined_df.show()



# COMMAND ----------

# MAGIC %md
# MAGIC 6. Example for Filtering the data 

# COMMAND ----------

# working on the json df 
# using the column object expressions  
from pyspark.sql.functions import *

q2_df.printSchema()
q6_df = q2_df.where("email like '%yahoo.com'") \
            .where("phone like '+1%'") \
            .select("name", "email", "phone")

q6_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Example for applying aggregate functions like  max, min, avg

# COMMAND ----------

display(q1_df)

# COMMAND ----------

# again using column object expressions 
q7_df = q1_df.groupBy("Category") \
    .agg(sum(expr("Price * Quantity")).alias("TotalPrice"),
         avg("Price").alias("AvgPrice"))
q7_df.show()

# COMMAND ----------

# an example using the SQL string expression 
from pyspark.sql.functions import *

spark.catalog.dropTempView("sales")
q1_df.createTempView("sales")

q7_2df = spark.sql("""
        select
            Category,
            SUM(Price * Quantity) AS TotalPrice,
            AVG(Price) AS AvgPrice
        from sales 
        group by Category
             """)
q7_2df.show()
spark.catalog.dropTempView("sales")


# COMMAND ----------

# MAGIC %md
# MAGIC 8. Example for Read json file with typed schema (without infering the schema) with Structtype, StructField .....

# COMMAND ----------

# reading the file normally, letting spark infer the schema 
q8_df = spark.read \
        .format("json") \
        .option("multiline","true") \
        .load("dbfs:/FileStore/assignment/schema_example.json")
q8_df.printSchema()
q8_df.show()

# specifying the schema and reading again 

from pyspark.sql.types import *

json_schema = StructType([
    StructField("age", IntegerType()),
    StructField("birth_date", DateType()),
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("salary", DoubleType())
])

q8_2df = spark.read \
        .schema(json_schema) \
        .option("multiline", "true") \
        .json("dbfs:/FileStore/assignment/schema_example.json") 

q8_2df.printSchema()
q8_2df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Questions: 
# MAGIC   1. Does the col_name specified in the struct field have to match the key values in the json always? 

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Example for increase and decrease number of dataframe partitions 

# COMMAND ----------

# Example for increasing partition can be before doing transformations in local machine to simulate realistic behavior of the cluster environment 

q9_df = q8_df.repartition(4)
q9_df.rdd.getNumPartitions()


# COMMAND ----------

# decreasing partitions can be used when writing the file, as each partition is simultaneously written by executors 

q9 = q9_df.repartition(1)
q9.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC 10. Example for renaming the column of the dataframe 

# COMMAND ----------

#renaming the columns to json dataframe in q8 

q10_df = q8_2df \
        .withColumnRenamed("age", "Age") \
        .withColumnRenamed("birth_date", "Dob") \
        .withColumnRenamed("id", "Id") \
        .withColumnRenamed("name", "FirstName") \
        .withColumnRenamed("Salary", "MonthlySalary")

q10_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 11. Example for adding a new column to the dataframe 

# COMMAND ----------

q11_df = q10_df \
        .withColumn("AnnualSalary", expr("12 * MonthlySalary"))
q11_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC 12. Changing the structure of the dataframe:
# MAGIC   A. Comeup with your own data , add more data points 
# MAGIC   B. I want you to read the above dataset into dataframe 
# MAGIC   C. Now, change the dataframe structure and write the dataframe as json file 

# COMMAND ----------

from pyspark.sql.functions import *

json_data = [
    """{ "name" : "john doe", "dob" : "01-01-1980" }""",
    """{ "name" : "john adam", "dob" : "01-01-1960", "phone" : 1234567890 }""",
    """{ "name" : "jane doe", "dob" : "15-06-1990", "email" : "jane.doe@example.com" }""",
    """{ "name" : "mike ross", "dob" : "20-12-1985", "phone" : 9876543210, "email" : "mike.ross@example.com" }"""
]

my_rdd = spark.sparkContext.parallelize(json_data)
df = spark.read.json(my_rdd)
df.show()

transformed_df = df.select(struct("name", "dob", "email", "phone").alias("personal_data"))
display(transformed_df)

output_path = "dbfs:/FileStore/assignment/q12/output.json"

transformed_df.write \
    .format("json") \
    .mode("overwrite") \
    .save(output_path)

