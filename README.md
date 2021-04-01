# /reddit-comment-extracting.py
This file is run from the dockerfile and searches for mentions of the selected ticker in the reddit comment text. Tickers are either provided from the OPTIONAL ENVIRONMENT VARIABLES or they are created from the tables ‘nasdaqticker’ and ‘nysetickers’. Code to create these databases can be found in the nasdaq-ticker-scrpaer and nyse-ticker-scraper repositories. 
Each time a ticker is mentioned a entry to update the mentions in a  python dict is updated example {'A': 9, 'IT': 6, 'SO': 3, 'ARE': 3, 'ON': 3}.
In addition to collecting mentions of tickers in the comment, sentimental analysis is performed using python  library TextBlob to extract the polarity and subjectivity of the comments. Polarity is measured from [-1,1], -1 meaning negative sentiment, 0 is neutral, and 1 being very positive.
Subjectivity is measured [0,1] with 0 being objective and 1 being subjective. Objective is observation of measurable facts and subjective is personal opinions, assumptions, interpretations and beliefs.

# ISSUES AND SOLUTIONS
- The database can be vary large so a generator is used to only process one days worth of comments at a time. 
- To prevent processing items multiple times and to preserve resources will attempt to load “redditlastcommentupdate” from the database. This is a datetime to process from till current
- Tickers not detected because surrouding values. Special characters and emojis are removed from the text ['$','.','"',"'","!","?","*","/",'(',')'] before processing ticker search.
- Sentimental analysis is done before removing the special characters since the package uses some of these to calculate the sentiment. 

# REQUIRED ENVIRONMENT VARIABLES 
- postgreshost=24.224.224.224
- postgrespassword=lakjsdhnfsakjd

# OPTIONAL ENVIRONMENT VARIABLES
- tickers =['GMC','AMC']
- update_from_date=2021-03-15

# COLUMNS CREATED INSIDE redditcommentliteralextraction TABLE
- commentid
- comment-tickers-used
- comment-polarity
- comment-subjectivity

# WARNING
- Update_from_date will take precedence of over database info from redditlastcommentupdate table
- Using literal mentions to calculate mentions certain tickers like ‘ON’ , ‘FOR’, and ‘A’ create false positive mentions of selected tickers
- Redditlastcommentupdate table must exists to prevent erros. Can be empty!
