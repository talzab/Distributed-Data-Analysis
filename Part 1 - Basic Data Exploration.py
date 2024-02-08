# Databricks notebook source
# Importing modules
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

# Loading data
delays = spark.read.table("hive_metastore.default.tilinajay2")
delays

# COMMAND ----------

# MAGIC %md
# MAGIC **A summary table showing the number of flights per month across the history of the data**

# COMMAND ----------

# Calculate the aggregates
total_flights = delays \
    .withColumn("FL_DATE_MONTH", F.month("FL_DATE")) \
    .groupBy("FL_DATE_MONTH") \
    .agg(F.count("*").alias("TOTAL_FLIGHTS_BY_MONTH")).orderBy("FL_DATE_MONTH")
    
total_flights.show()

# Create the plot
ax = total_flights.toPandas().plot(kind="bar", x="FL_DATE_MONTH", y="TOTAL_FLIGHTS_BY_MONTH", legend=False,
                                   title="Total Flights by Month", color = "steelblue")

ax.set_xlabel("Month")
ax.set_ylabel("Total Flights")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '{:,.0f}'.format(x)))

# Show the plot
plt.show()              

# COMMAND ----------

# MAGIC %md
# MAGIC **The percentage of flights delayed per week, plotted over the entire length of the data**

# COMMAND ----------

# Create a delay flag
delay_condition = (
    (F.col("CARRIER_DELAY") != 0) |
    (F.col("WEATHER_DELAY") != 0) |
    (F.col("NAS_DELAY") != 0) |
    (F.col("SECURITY_DELAY") != 0) |
    (F.col("LATE_AIRCRAFT_DELAY") != 0)
)

# Calculate the aggregate
perc_delayed_week = delays \
    .withColumn("FL_DATE_WEEK", F.weekofyear("FL_DATE")) \
    .withColumn("DELAYED", F.when(delay_condition, 1).otherwise(0)) \
    .groupBy("FL_DATE_WEEK") \
    .agg((F.sum(F.col("DELAYED"))/F.count("*") * 100).alias("PERCENTAGE_DELAYS")) \
    .select("FL_DATE_WEEK", "PERCENTAGE_DELAYS") \
    .orderBy("FL_DATE_WEEK")

perc_delayed_week.show()

#  Create the plot
ax = perc_delayed_week.toPandas().plot(kind="line", x="FL_DATE_WEEK", y="PERCENTAGE_DELAYS", legend=False,
                                   title="Percentage of Delayed Flights by Week", color = "steelblue")

ax.set_xlabel("Week")
ax.set_ylabel("Percentage of Delayed Flights")
ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: '{:,.0f}'.format(x)))

# Show the plot
plt.show()    

# COMMAND ----------

# MAGIC %md
# MAGIC **The number of delayed flights per week, by type of delay, plotted over time**

# COMMAND ----------

# Calculate the Aggregate
delayed_flights_by_week_type = delays \
    .withColumn("FL_DATE_WEEK", F.weekofyear("FL_DATE")) \
    .groupBy("FL_DATE_WEEK") \
    .agg(F.sum(F.col("CARRIER_DELAY")).alias("TOTAL_CARRIER_DELAYS"),
         F.sum(F.col("WEATHER_DELAY")).alias("TOTAL_WEATHER_DELAYS"), 
         F.sum(F.col("NAS_DELAY")).alias("TOTAL_NAS_DELAYS"), 
         F.sum(F.col("SECURITY_DELAY")).alias("TOTAL_SECURITY_DELAYS"), 
         F.sum(F.col("LATE_AIRCRAFT_DELAY")).alias("TOTAL_LATE_AIRCRAFT_DELAYS")) \
    .select("FL_DATE_WEEK", "TOTAL_CARRIER_DELAYS", "TOTAL_WEATHER_DELAYS", "TOTAL_NAS_DELAYS", "TOTAL_SECURITY_DELAYS", "TOTAL_LATE_AIRCRAFT_DELAYS") \
    .orderBy("FL_DATE_WEEK")

delayed_flights_by_week_type.show()

# Create the plot
plt.figure(figsize=(10, 6)) 
for i in ["TOTAL_CARRIER_DELAYS", "TOTAL_WEATHER_DELAYS", "TOTAL_NAS_DELAYS", "TOTAL_SECURITY_DELAYS", "TOTAL_LATE_AIRCRAFT_DELAYS"]:
    plt.plot(delayed_flights_by_week_type.select("FL_DATE_WEEK").toPandas(),
             delayed_flights_by_week_type.select(i).toPandas(),
             label=i.title())
    
plt.title("Delayed flights by Week Per Delay Type")
plt.xlabel("Week")
plt.ylabel("Total Delayed Flights")
plt.legend()

# Show the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **A table of air carriers, showing the number of flights scheduled, the number canceled, the percentage delayed, and the average delay among those delayed. Sort by total number of flights, so the biggest air carriers come first.**

# COMMAND ----------

# Create a delay flag
delay_condition = (
    (F.col("CARRIER_DELAY") != 0) |
    (F.col("WEATHER_DELAY") != 0) |
    (F.col("NAS_DELAY") != 0) |
    (F.col("SECURITY_DELAY") != 0) |
    (F.col("LATE_AIRCRAFT_DELAY") != 0)
)

# Calculate the aggregate
delays \
    .withColumn("DELAYED", F.when(delay_condition, 1).otherwise(0)) \
    .groupBy("OP_CARRIER") \
    .agg(
        F.count("OP_CARRIER_FL_NUM").alias("TOTAL_FLIGHTS_SCHEDULED"),
        F.sum(F.when(F.col("CANCELLED") == 1, 1).otherwise(0)).alias("TOTAL_CANCELLED_FLIGHTS"),
        (F.sum("DELAYED") / F.count("*") * 100).alias("PERCENTAGE_DELAYED_FLIGHTS"),
         F.avg(F.when(F.col("DELAYED") == 1, F.col("ARR_DELAY"))).alias("AVERAGE_DELAYED_FLIGHTS")) \
    .select("OP_CARRIER", "TOTAL_FLIGHTS_SCHEDULED", "TOTAL_CANCELLED_FLIGHTS", "PERCENTAGE_DELAYED_FLIGHTS",   
            "AVERAGE_DELAYED_FLIGHTS") \
    .orderBy("TOTAL_FLIGHTS_SCHEDULED", ascending=False) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Table of top 50 airports by percentage of flights delayed, showing the airport code, percentage of flights delayed, and average number of flights per day**

# COMMAND ----------

# Calculate the percentage of flights delays
perc_airport_delays = delays \
    .groupBy("ORIGIN") \
    .agg((F.sum(F.when(F.col("DEP_DELAY") >= 15, 1).otherwise(0)) / F.count("*") * 100).alias("PERCENTAGE_DELAYED"))

# Calculate the flights per day
flights_per_day = delays \
    .groupBy("ORIGIN", "FL_DATE") \
    .count() \
    .groupBy("ORIGIN") \
    .agg(F.avg("count").alias("AVG_FLIGHTS_PER_DAY"))

airport_summary = perc_airport_delays.join(flights_per_day, "ORIGIN") \
    .orderBy(F.desc("PERCENTAGE_DELAYED")) \
    .limit(50) \
    .show()

# COMMAND ----------

# END
