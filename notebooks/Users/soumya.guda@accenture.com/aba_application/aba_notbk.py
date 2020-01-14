# Databricks notebook source
#dbutils.fs.mount(
#source = "wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net",
#mount_point = "/mnt/<mount-name>",
#extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

spark.conf.set("fs.azure.sas.container-for-data.storageacctsoumya.blob.core.windows.net","https://storageacctsoumya.blob.core.windows.net/?sv=2019-02-02&ss=bfqt&srt=sco&sp=rwdlacup&se=2020-12-25T04:54:54Z&st=2020-01-14T20:54:54Z&spr=https&sig=GO%2FONqIoWgLYOzbPWzL%2FZCu0QnhmfCZSIGo5wRTsxcA%3D")



# COMMAND ----------

# Creating widgets for leveraging parameters, and printing the parameters
dbutils.widgets.text("business_date", "", "")
dbutils.widgets.get("business_date")
cur_bus_date = getArgument("business_date")
print("business_date = {}".format(cur_bus_date))


# COMMAND ----------

df_aba_path = "wasbs://container-for-data@storageacctsoumya.blob.core.windows.net/asis/sampledata/data_as_of_date={}/".format(cur_bus_date)

# COMMAND ----------

print(df_aba_path)

# COMMAND ----------

df_aba= spark.read.csv(df_aba_path,inferSchema=True,header=True)

# COMMAND ----------

df_aba.show()

# COMMAND ----------

df_aba.createOrReplaceTempView("aba_tbl")

# COMMAND ----------

df_dt_fmt = spark.sql("""select Member_ID,TO_DATE(CAST(UNIX_TIMESTAMP(DOB,'MM/dd/yyyy')as TIMESTAMP)) as dob_fmtd
                      ,TO_DATE(CAST(UNIX_TIMESTAMP(Start_Date,'MM/dd/yy')as TIMESTAMP)) as st_dt_fmtd
                      ,TO_DATE(CAST(UNIX_TIMESTAMP(End_Date,'MM/dd/yyyy')as TIMESTAMP)) as end_dt_fmtd,
                      year(TO_DATE(CAST(UNIX_TIMESTAMP(DOB,'MM/dd/yyyy')as TIMESTAMP))) as yob,
                      year(TO_DATE(CAST(UNIX_TIMESTAMP(Start_Date,'MM/dd/yy')as TIMESTAMP))) as ystrt,
                      year(TO_DATE(CAST(UNIX_TIMESTAMP(End_Date,'MM/dd/yyyy')as TIMESTAMP))) as yenddt,
                      `BMI`,`BMI percent`
from aba_tbl""")

# COMMAND ----------

df_dt_fmt.show()

# COMMAND ----------

df_dt_fmt.createOrReplaceTempView('aba_all_FormattedDates')

# COMMAND ----------

df_age = spark.sql("""select Member_ID,ystrt-yob as Age, BMI, `BMI percent` from aba_all_FormattedDates""")

# COMMAND ----------

df_age.show()

# COMMAND ----------

df_age_path = "wasbs://container-for-data@storageacctsoumya.blob.core.windows.net/gld/data_as_of_date={}/".format(cur_bus_date)

# COMMAND ----------

print(df_age_path)

# COMMAND ----------

# Write data to gld layer
df_age.coalesce(1).write.mode('overwrite').option("header","true").format("com.databricks.spark.csv").save(df_age_path)