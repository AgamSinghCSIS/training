# Databricks notebook source
# MAGIC %fs ls /mnt/landing-zone/

# COMMAND ----------

# Ingesting Data from ADLS mount point to DELTA TABLES
# Creating a SCHEMALESS TABLE for each directory in the landing zone
# USING COPY COMMAND METHOD to ingest data. 
# mergingSchema as schema of data can change over time, so want to ingest extra cols as well 
# Tables Stored inside Hive-Metastore
""" IS THIS BEST PRACTICE? IDK """

class Ingestion():

    def __init__(self, schema_name, landing_zone_path):
        self.landing_zone_path = landing_zone_path
        self.schema_name = schema_name

    def create_schema(self):
        spark.sql(f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE")
        print(f"Creating schema {self.schema_name}")
        return spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}")

    def create_table(self, table_name):
        # Dropping the table if it exists to ensure no duplicate ingestion
        spark.sql(f"DROP TABLE IF EXISTS {self.schema_name}.{table_name}")
        print(f"Creating table {self.schema_name}.{table_name}")
        return spark.sql(f"CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name}") 
    
    def populate_table(self, table_name, file_path):
        print(f"\tIngesting data from {file_path} to {self.schema_name}.{table_name}")
        spark.sql(f"""COPY INTO {self.schema_name}.{table_name}
                      FROM  '{file_path}'
                      FILEFORMAT = CSV 
                      FORMAT_OPTIONS('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
                      COPY_OPTIONS('mergeSchema' = 'true')
                      """)

    def test_table_population(self, table_name, file_path):
        df = spark.read.format("csv").option("header", "true").load(file_path)
        tbl_row_count = spark.sql(f"SELECT COUNT(*) FROM {self.schema_name}.{table_name}").collect()[0][0]
        if df.count() == tbl_row_count:
            return True
        else:
            return False

    def ingest(self):
        cs = self.create_schema()
        if cs.count() == 0:
            for itr in dbutils.fs.ls(self.landing_zone_path):
                dir_name = itr.name
                file_path = self.landing_zone_path + dir_name
                table_name = dir_name.split("/")[0] + '_stg'
                ct = self.create_table(table_name)
                if ct.count() == 0:
                    self.populate_table(table_name, file_path)
                    if self.test_table_population(table_name, file_path):
                        print(f"\t Ingested Data into {table_name} Successfully! ")
                        continue
                    else:
                        print(f"Something went wrong!")
                        break
                    




# COMMAND ----------

ig = Ingestion("Stg", "/mnt/landing-zone/")
ig.ingest()