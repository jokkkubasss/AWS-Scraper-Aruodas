# AWS-Scraper-Aruodas
An automated scraping solution.

the included python script runs on AWS Lambda. In total, there are 3 variations of this script that are used for different data types. Daily postings of real estate are scraped, and stored into a MySQL RDBMS. The lambda functions are orchestrated by AWS Step Functions using a state machine model.
