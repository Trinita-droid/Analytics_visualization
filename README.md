This repository contains python code that performs analysis on the data fetched by forming an API connection on unstructured data type XML from : https://data.cityofchicago.org/api/views/7ce8-bpr6/rows.xml?accessType=DOWNLOAD

The cleaned dataset is uploaded as `chicago.csv`. Data is stored before processing and after cleaning in databases- MongoDB, Postgres respectively. The databases images are pulled from Docker engine and containers are created. 
Visualizations are done on the processed dataset from Docker Postgres, connected with Metabase - a visualization platform with SQL queries.
