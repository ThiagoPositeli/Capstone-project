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

<h2>Improvements</h2>

Apache Airflow could be used for building up a ETL data pipeline to regularly update the date and populate a report. Apache Airflow also integrate with Python. More applications can be combined together to deliever more powerful task automation.
