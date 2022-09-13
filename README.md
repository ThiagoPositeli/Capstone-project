<h1>Udacity Data Engeneering Nanodegree Program - The Capstone Project</h1>

<h2>Project Sumary</h2>

This is the final project of udacity's data engineer training - the capstone project.
I decided to bring a real case that we deal with every day with json files.

<h4>Business Case</h4>

The data source is well used in the market.
It comes from a log of a famous library in data companies called "Great Expectations".
We use great expectations to validate data quality in our pipelines and automatic decision making in airflow, in case a dag fails.
This library generates a processing log file that you can analyze later and in this case that's what we're doing here in this project.


<h2>Why Google Cloud Platform and not AWS</h2>

That's because the company already partnered with Google, the free tier seems to fit our needs better and the services (especially BigQuery) too.


<h2>Summary of Contents</h2>

 - Data Dicionary and Data Modelling 
 - Data Files Json and Parquet
 - Architecture solution design
 - SQL create statement
 - Jupyter notebook using to processing the data and ETL
 - Tableau Dashboard Report example from analytics data


<h2>How to run</h2>

All reference on how to run and read the files, is in the jupyter notebook in here: https://github.com/ThiagoPositeli/Capstone-project/blob/main/Data%20Quality%20Pipeline%20Big%20Query.ipynb

<h2>Project Write Up</h2>

<h4>Tools and Technologies</h4>

GCS for data storage
Pyspark and python for exploration
PySpark for large data set data processing to transform staging table to dimensional table

<h4>Data Update Frequency</h4>

Tables created from great expectations validations log files and data set should be updated daily since the raw data set is built up daily.
All tables should be update in an append-only mode, but only the fact table. The dimension tables are only created and updated once.


<h4>Design Considerations</h4>

The data was increased by 100x.
The dataproc (hadoop) cluster is already built to handle 100 times the data size with autoscale


The data populates a dashboard that must be updated on a daily basis by 8am every day.

Apache Airflow could be used for building up a ETL data pipeline to regularly update the date and populate a report. Apache Airflow also integrate with Python. More applications can be combined together to deliever more powerful task automation.


The database needed to be accessed by 2000+ people.

Big query can handle up to N connections, the new data wharehouse on cloud can handle with this easy.



