
import psycopg2
import os
import datetime
import pandas as pd
import ast
from textblob import TextBlob


# add to scrapper the last update method

class redditcommentliteralextraction():

    def __init__(self, tickers=None, update_from_date=None):
        # list of tickers to search for in text
        # if no tickers provided this will search all tickers
        self.tickers = tickers
        # Optional inject formated date as string "2021-03-21"
        self.update_from_date = update_from_date
        # theses are used to track the progress and print when the int percent complete changes
        self.counter = 0
        self.percent_tracker = 0
        self.time_started_class = datetime.datetime.now() # never changes
        self.time_temp = datetime.datetime.now() # used to track time for each comment
        self.first_hundred_comment_time = list() 
        self.percentage_processed = 0 

    def __enter__(self, ticker=None):
        # connect to DB and create required tables if needed
        self.conn = psycopg2.connect(
                host= os.getenv('postgreshost'),
                database="postgres",
                user="postgres",
                password=os.getenv('postgrespassword'))
        self.conn.autocommit=True
        self.curr = self.conn.cursor()
        # Create Tables 
        self._create_literal_comment_extraction_table()
        # create ticker list
        self.ticker = ticker
        self._find_tickers_to_search()
        print('searching test for ', self.tickers)

        self._get_last_comment_update()
        print("updating comments starting from ",self.update_from_date)
        
        for df in self._comment_generator():
            # self._extract_noun_from_chunk(df)
            # must run _extract_literal_from_chunk last for time estimate to be accurate
            self._extract_literal_from_chunk(df)
            del(df)

    def _extract_literal_from_chunk(self, df):
        for index, comment in df.iterrows():
            #used to store metions for each comment
            literal_mentions_text = dict()
            text = self._format_text(comment['commenttext'])
            #cycle through tickers and update mention dicts
            for ticker in self.tickers:
                # #PROCESS COMMENT TEXT
                found_tickers = self._search_text_for_matches(text, ticker)
                if found_tickers:
                    literal_mentions_text[ticker] = len(found_tickers)
            text_polarity, text_subjectivity = self._sentiment(comment['commenttext'])

            temp = [ comment['id'], 
                    str(literal_mentions_text), 
                    text_polarity,
                    text_subjectivity
                   ]
            
            self._save_literal_extraction(temp)
            
            # SECTION USED FOR TRACKING ESTIMATE RUN TIME
            self.counter += 1
            self.percentage_processed =  (self.counter /self.comment_count) * 100
            self.temp_percent = int(self.percentage_processed)
            if self.counter < 100: # create average of first 100 transactions
                time_elapsed = datetime.datetime.now() - self.time_temp
                self.first_hundred_comment_time.append(time_elapsed)
                self.time_temp = datetime.datetime.now()
            if self.counter == 100:
                self.average_time = sum(self.first_hundred_comment_time ,datetime.timedelta())
                self.estimate_time_to_run = (self.comment_count / 100) * self.average_time
                print('\nfrom the first 100 processed comment\nthe estimate run time is ',self.estimate_time_to_run)
            # END TRACKING ESTIMATE RUN TIME

    def _search_text_for_matches(self, text, ticker):
        matching_list = [word for word in text.split() if word == ticker]    
        return matching_list

    def _sentiment(self,text):
        text = str(text)
        blob = TextBlob(text)
        text_polarity, self.text_subjectivity = blob.sentiment
        return text_polarity, self.text_subjectivity
    
    def _save_literal_extraction(self, temp):
        # try to qury extraction
        self.curr.execute("""SELECT * FROM redditcommentliteralextraction WHERE commentid=(%s)""",(temp[0],))
        comment = self.curr.fetchone()
     
        if comment:# if comment exists update values
            # tickers may have been added and comments could have been updated changing setiment
            # add the dicts together
            qury_text_tickers = ast.literal_eval(comment[1])
            new_text_tickers = ast.literal_eval(temp[1])

            updated_text_tickers = self._add_dicts(dict1=qury_text_tickers, dict2=new_text_tickers)

            self.curr.execute("""UPDATE redditcommentliteralextraction SET
                        comment_tickers_used=%s,
                        comment_polarity=%s,
                        comment_subjectivity=%s
                        WHERE commentid=%s
                        """,
                        (str(updated_text_tickers), temp[2], temp[3], temp[0]))
        else: # save the data for the first time
            self.curr.execute("""INSERT INTO redditcommentliteralextraction
                            (commentid,
                            comment_tickers_used,
                            comment_polarity,
                            comment_subjectivity)
                            VALUES (%s , %s, %s, %s)
                            """,(temp[0], str(temp[1]), temp[2], temp[3],))

    def _add_dicts(self, dict1, dict2):
        #error not adding amc not found 
        out_dict = dict()
     
        for ticker in dict1:
            out_dict[ticker] = dict1[ticker]
        for ticker in dict2:
            try:
                out_dict[ticker]
                out_dict[ticker] += dict2[ticker]
            except KeyError:
                out_dict[ticker] = dict2[ticker]
        return out_dict
    
    def _create_literal_comment_extraction_table(self):
        self.curr.execute("""CREATE TABLE IF NOT EXISTS redditcommentliteralextraction(
                commentid int references redditcomment(id) UNIQUE,
                comment_tickers_used varchar(100000) NOT NULL,
                comment_polarity float NOT NULL,
                comment_subjectivity float NOT NULL,
                CONSTRAINT fk_redditcomment
                    FOREIGN KEY(commentid)
                        REFERENCES redditcomment(id)
            );""")
            
    def _format_text(self, text):
        text = str(text)
        remove_elements = ['$','.','"',"'","!","?","*","/",'(',')']
        for element in remove_elements:
            text = text.replace(element,' ')
        text = text.encode('ascii', 'ignore').decode('ascii')
        text = text.upper()
        return text

    def _ectract_nouns_from_chunk(self):
        pass

    def _extract_names_from_chunk(self):
        pass

    def _find_tickers_to_search(self):
        # senario1=  get all tickers from database
        # senario2= set self.tickers to the user input value
        if self.tickers == None:
            self.curr.execute("""SELECT ticker FROM nasdaqtickers """)
            nasdaq_tickers = self.curr.fetchmany(100000)
            nasdaq_tickers = [qury[0].upper() for qury in nasdaq_tickers]
            self.curr.execute("""SELECT ticker FROM nysetickers """)
            nyse_tickers = self.curr.fetchmany(100000)
            nyse_tickers = [qury[0].upper() for qury in nyse_tickers]
            self.tickers = nyse_tickers + nasdaq_tickers
        else:
            self.tickers = ast.literal_eval(str(self.tickers))
        
    def _comment_generator(self):
        # get how many comments to update
        self.curr.execute("""
                SELECT count(id) 
                FROM redditcomment
                WHERE datetime BETWEEN
                %s and %s 
            ;""",[self.update_from_date, datetime.datetime.now()])
        self.comment_count = self.curr.fetchone()[0]
        
        # generates a pandas DF for each day going foward until current
        # each dataframe is one days worth of comments
        # starts at the self.update_from_date which keeps adding a day for each interation
        print(self.update_from_date)
        print(type(self.update_from_date))
        print(datetime.datetime.now())
        while self.update_from_date <= datetime.datetime.now():
            # calculate self.end_chunk_date one day from the self.update_from_date
            self.end_chunk_date = datetime.timedelta(days=1) + self.update_from_date
            
            # PRGOGRESS report
            # used to calculate the estimate time and percentage complete
            print("\nChunk request between {} and {}".format(self.update_from_date, self.end_chunk_date))     
            print(self.percentage_processed,'% processed')
            if self.counter > 100: # after hundred comment crate estimate run time
                time_elapsed =  datetime.datetime.now() - self.time_started_class 
                time_per_comment = time_elapsed / self.counter
                new_estimate = time_per_comment *  (self.comment_count - self.counter)
                print("Estimated remaining run time ", new_estimate)
                original_estimate_acuracy = ( self.estimate_time_to_run - new_estimate) - time_elapsed
                # formate for negative to make more human readable
                print("add ",original_estimate_acuracy,' to original estimate of ',self.estimate_time_to_run)

            # qury the comments
            self.curr.execute("""
                SELECT * 
                FROM redditcomment
                WHERE datetime BETWEEN
                %s and %s 
            ;""",[self.update_from_date, self.end_chunk_date])
            chunk = self.curr.fetchmany(1000000)
        
            # after qurying comments update self.update_from_date by adding a day
            self.update_from_date += datetime.timedelta(days=1)
            if chunk:
                columns = ['postid', 'id','commentusername', 'upvotes', 'commenttext', 'datetime', 'subreddit']
                df = pd.DataFrame(chunk, columns=columns).fillna(value=0)
                yield df
        
    def _get_last_comment_update(self):
        #  To preserve resouces calcualte as little as possible
        # senerio 1= qury redditlastcommentupdate in DB for date to update FROM
        # senerio 2= if user provides start date set to update_from_date provided
        # senerio 3= if no qury or provided datetime is found set to oldest comment datetime
        if self.update_from_date:
            string_date = self.update_from_date
            format = "%Y-%m-%d"
            self.update_from_date = datetime.datetime.strptime(string_date, format)
            # self.update_from_date = datetime.datetime(self.update_from_date[0])
        else: # no provided date try to set to redditlastcommentupdate value from DB
            self.curr.execute("""SELECT datetime FROM redditlastcommentupdate""")
            redditlastcommentupdate = self.curr.fetchall()
            if redditlastcommentupdate:
                # go through all dates to find oldest date
                oldestdate = redditlastcommentupdate[0][0]
                for date in redditlastcommentupdate:
                    if date[0] < oldestdate:
                        oldestdate = date
                self.update_from_date = oldestdate[0]
                
                # self.update_from_date = datetime.datetime(self.update_from_date[0])
            else: # set to oldest date comment
                self.curr.execute("""
                SELECT min(datetime) 
                FROM redditcomment
                ;""")
                self.update_from_date = self.curr.fetchone()[0]

    
    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.close()
        self.curr.close()

with redditcommentliteralextraction(tickers=os.getenv('tickers'),
                        update_from_date=os.getenv('update_from_date'),
                        ) as runner:
    runner
