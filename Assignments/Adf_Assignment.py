# Databricks notebook source
#creating mount point
dbutils.fs.mount(source = 'wasbs://manidbcontainer@manidb1azure.blob.core.windows.net',
mount_point = '/mnt/adfassignment',
extra_configs = {'fs.azure.account.key.manidb1azure.blob.core.windows.net':'X96T9Vc26NOxHA2ZnMomDDDLtPhUeGwNXub8cFQ1cOZgSDJNk5yHdKE/h1hIZjtawWRb2gozVuCH+AStZlT6sw=='}
)

# COMMAND ----------

