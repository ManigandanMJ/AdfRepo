#AdfRepo

Created pipeline to load the below data and follow the below steps:

1. collected the nested JSON data from Open API and store the data in bronze layer.

2. Written Schema and explode the columns from the nested JSON

3. Read the data from bronze layer

4. Written the data into silver and use merge as a write method
