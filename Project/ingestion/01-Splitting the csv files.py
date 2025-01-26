# Databricks notebook source
# MAGIC %md
# MAGIC 1. Create the Split Class to Split the csv files into multiple csvs 

# COMMAND ----------

class splitRawFiles():
    def __init__(self, landing_zone_path, num_partitions):
        self.landing_zone_path = landing_zone_path
        self.num_partitions = num_partitions
    
    def read_file(self, fpath):
        print(f"\tstarting reading file at {fpath}")
        return (
            spark.read
                .format("csv")
                .option("header", "true")
                .load(fpath)
        )
    def add_columns(self, df):
        from pyspark.sql.functions import input_file_name, current_timestamp
        if 'source_file' not in df.columns:
            df = df.withColumn('source_file', input_file_name())
        if 'loaded_ts' not in df.columns:
            df = df.withColumn('loaded_ts', current_timestamp())
        return df
        

    def split_df(self, df):
        print(f"\tSplitting the df in {self.num_partitions} partitions")
        re_df = df.repartition(self.num_partitions)
        return re_df
    
    def write_file(self, df, fpath):
        print(f"\t Writing the df to {fpath}")
        (df.write
            .format("csv")
            .option("header", "true")
            .mode("overwrite")
            .save(fpath))
        

    def split_process(self):
        for itr in dbutils.fs.ls(self.landing_zone_path):
            dir_name = itr.name
            file_path = self.landing_zone_path + dir_name
            print(f"Starting splitting {dir_name}")
            df       = self.read_file(file_path)
            og_count = df.count()
            df       = self.add_columns(df)
            repartitoned_df   = self.split_df(df)
            repartition_count = repartitoned_df.count()
            if og_count == repartition_count:
                print("Repartition Successful! Starting to Write")
                self.write_file(repartitoned_df, file_path)
            # reading the split files as a df again to check if all the rows split successfully 
            print(f"Testing {dir_name} split to make sure all data was split properly")
            re_read_df = self.read_file(file_path)
            re_count = re_read_df.count()
            if(og_count != re_count):
                print(f"\tSplit Failed! Expected Count: {og_count}, count after split: {re_count}")
                break
            else:
                print(f"\tSplitting {dir_name} completed successfully!")
            print("\n\n")



        

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 2. Create an instance of the splitRawFiles class and call the split_process method
# MAGIC

# COMMAND ----------

sp = splitRawFiles('/mnt/landing-zone/', 10)
sp.split_process()