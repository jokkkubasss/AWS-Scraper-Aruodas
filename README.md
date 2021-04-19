# AWS-Scraper-Aruodas
An automated scraping solution.

the included python script runs on AWS Lambda. In total, there are 3 variations of this script that are used for different data types. Daily postings of real estate are scraped, and stored into a MySQL RDBMS. The lambda functions are orchestrated by AWS Step Functions using a state machine model.

The state machine diagram example for the lambda functions looks as follows:

![image](https://user-images.githubusercontent.com/58790520/115300394-3733b500-a160-11eb-9727-35e83b57a7fe.png)
