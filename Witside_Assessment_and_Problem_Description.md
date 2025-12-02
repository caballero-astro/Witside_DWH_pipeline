Context: We are in a production floor composed with multiple production lines. We keep track of each production line status and based on these data we calculate our production KPIs for production manager. 

* Data: The data we collect have 3 columns:
1) production_line_id: the unique identifier of the production line.
2) status : the status of production line. Takes three distinct values.
	ON : the production line is operating normally. 
	START : the production started and this is generated with the production line initiation. 
	STOP : the production line stopped the operation. This is generated with production line termination.
3) timestamp : the exact timestamp of the production line's status update.

* Business Questions
1) For production line "gr-np-47", give me table with columns:
	a) start_timestamp: the timestamp with the initiation of the production process. 
	b) stop_timestamp: the timestamp with the termination of the production process after the last initiation.
	c) duration: the total duration of the production process. 

2) What is the total uptime and downtime of the whole production floor?

3) Which production line had the most downtime and how much that was?


* Exercise:

1) Provide a python package with the functions that can answer "Business Questions", with a how-to-use guide.


2) Provide the .sql files with the code that creates the tables/views needed to answer the "Business Questions", with a guide to implement it into our DWH.
