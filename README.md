# Reddit Sentiment Analysis Using Praw API 

## Description
A dashboard displaying the sentiment and popularity of chosen subreddit(s). Retrieves subreddit, submission, comment, and Redditor data tables from the Praw Reddit API, which can be loaded into a relational database, exported to CSV, or used in a web application. 

Please note this script works within the limits of Praw's free tier, which is 100 API calls per minute. Several measures are taken to limit the number of necessary API calls and consequentially limit the dataset itself. This repo filters the top 500 submissions and their comments by a list of keywords before adding them to our dataset. While this repo may provide insight into a chosen community on Reddit, in no way does it fully represent the entire community. 

Please be aware that it does take several hours to run, so I'd recommend using a modified script to fetch data for the top 5 submissions when testing for the first time.

## Packages Used
## Usage
Steps:
1. Create a Reddit developer account and create an app.
2. Confirm a chosen endpoint for extracted data. This repo contains the necessary code to create and load data into a local Microsoft SQL Server database.

## SQL Queries
## Power BI Reports
## Fetching data from multiple subreddits
## Further usage
