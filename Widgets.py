# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.combobox(name="databases",defaultValue="naval",choices=["naval","krishna","shiv","raghu","sabya"],label="Schema")

# COMMAND ----------

dbutils.widgets.get("databases")

# COMMAND ----------

dbutils.widgets.multiselect(name="calender",defaultValue="2023",choices=["2023","2022","2021"],label="Year")

# COMMAND ----------

dbutils.widgets.get("databas")

# COMMAND ----------

dbutils.widgets.get("calender")

# COMMAND ----------


