import os
import praw
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import pandas as pd
from sqlalchemy import create_engine
import urllib
import pyodbc 
from sqlalchemy.types import NVARCHAR
import time

def initialize_reddit_client():
    return praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )

def fetch_data(reddit):
    subreddit_names = [

        'SUBREDDIT_NAME' # replace with the name of the subreddit you want to recover

    ]

    subreddit_data = {
        'subreddit_id': [],
        'subreddit_name': [],
        'description': [],
        'subscribers': [],
        'timestamp': []
    }

    submission_data = {
        'submission_id': [],
        'title': [],
        'body': [],
        'timestamp':[],
        'upvotes': [],
        'num_comments': [],
        'subreddit_id': [],
        'author_id': [],
        'url': []
    }
    
    comment_data = {
        'comment_id': [],
        'body': [],
        'timestamp': [],
        'upvotes': [],
        'submission_id': [],
        'subreddit_id': [],
        'author_id': [],
    }

    author_data = {
        'author_id': [],
        'comment_karma': [],
        'has_verified_email': [],
        'is_mod': [],
        'timestamp': []
    }

    user_cache = {}

    keywords = ['schwab', 'fidelity', 'robinhood', 'thinkorswim', 'tdameritrade', 'TD Ameritrade', 'ameritrade', 'tos'
                'fidelity', 'app', 'vanguard', 'td', 'Goldman Sachs', 'complaint', 'issue', 'webull', 'stock', 'problem'
                'resolv', 'fix', 'feature', 'customer', 'transfer', 'securit', 'option', 'goldman', 'sachs', 
                'fee', 'account', 'check', 'Roth', 'IRA', 'IPO', 'broker', 'serv', '401', 'financ', 'deposit', 'withdrawl', 
                'mutual fund', 'invest', 'Morgan Stanley', 'etf', 'trad', 'usability', 'interfac', 'navigat',
                'Interactive Brokers', 'eToro', 'BlackRock', 'Black Rock', 'Morgan Stanley', 'Chase', 'JP Morgan', 'security', 'JPM'
                'platform', 'limit', 'bank', 'fee', 'job', 'profess', 'career', 'card', 'loan', 'credit', 'mortgage', 'debit'
                'pric', 'disabl', 'SCHW', 'FNF', 'zelle', 'goldman', 'sachs', 'bull', 'bear', 'launder',
                'plaid', 'api', 'email', 'crediential', 'restrict', 'member', 'join', 'portfol', 'margin',
                'leverag', 'analy', 'leverag', 'forecast', 'divest', 'allocat', 'benchmark', 'capital', 'divid', 'equit',
                'grow', 'declin', 'hedg', 'index', 'liqu', 'manag', 'perform', 'risk', 'sav', 'scal', 'specul', 'tax', 'volatil',
                'yield', 'arbitrag', 'audit', 'comply', 'compli', 'fund', 'quant', 'regula', 'market', 'crypto', 'bitcoin', 'btc',
                'SCHW', 'eth', 'ethereum', 'solana', 'sol', 'nft', 'token', 'blockchain', 'futures', 'oil', 'gold', 'silver', 
                'commodit', 'forex', 'foreign', 'international', 'software', 'mobile', 'tech', 'platform', 'sec', 'finra', 'law',
                'econom', 'histor', 'interest', 'rate', 'employ', 'inflation', 'federal funds', 'treasury', 'support', 'accessib'
                'corporat', 'business', 'enterprise', 'merge', 'acquire', 'acquisition', 'train', 'educat', 'litera', 'teach', 
                'feedback', 'innovat', 'updat', 'upgrad', 'bug', 'patch', 'release', 'corrupt', 'trend', 'recession', 'boom', 
                'advis', 'web', 'execut', 'order', 'sav', 'budget', 'debt', 'testimon', 'story', 'complain', 'resol',
                'loyal', 'sustainab', 'stakeholder', 'gdpr', 'protect', 'aml', 'customer', 'social responsibility', 'responsib'
                'learn', 'instruct', 'retir', 'coinbase', 'addepar', 'annuity', 'pension', 'venture capital', 'ai', 
                'artificial intelligence', 'ethic', 'footprint', 'govern', 'advi'
                ] # edit based on preferences and context
    
    for name in subreddit_names:
        fetch_subreddit_data(reddit, name, subreddit_data, submission_data, comment_data, author_data, user_cache, keywords)
    
    return subreddit_data, submission_data, comment_data, author_data

def fetch_subreddit_data(reddit, name, subreddit_data, submission_data, comment_data, author_data, user_cache, keywords):
    subreddit = reddit.subreddit(name)
    subreddit_data['subreddit_id'].append(subreddit.name)
    subreddit_data['subreddit_name'].append(subreddit.display_name)
    subreddit_data['subscribers'].append(subreddit.subscribers)
    subreddit_data['timestamp'].append(subreddit.created_utc)
    for submission in subreddit.top(limit=None):
        time.sleep(0.6)
        combined_text = (submission.selftext + " " + submission.title).lower()
        if any(keyword.lower() in combined_text for keyword in keywords):
            submission_data['submission_id'].append(submission.id)
            submission_data['title'].append(submission.title)
            submission_data['body'].append(submission.selftext)
            submission_data['timestamp'].append(submission.created_utc)
            submission_data['upvotes'].append(submission.score)        
            submission_data['num_comments'].append(submission.num_comments)
            submission_data['url'].append(submission.url)
            submission_data['subreddit_id'].append(subreddit.name)
            submission.comments.replace_more(limit=0)
            for comment in submission.comments.list():
                time.sleep(0.6)
                if any(keyword.lower() in comment.body.lower() for keyword in keywords):
                    try:
                        comment_data['comment_id'].append(comment.id)
                        comment_data['body'].append(comment.body)
                        comment_data['timestamp'].append(comment.created_utc)
                        comment_data['upvotes'].append(comment.ups)
                        comment_data['submission_id'].append(submission.id)
                        comment_data['subreddit_id'].append(subreddit.name)
                    except Exception as e:
                        print(f"Error fetching comment {comment.id}: {e}")
                    if submission.author:
                        author_name_submission = str(submission.author)
                        if author_name_submission not in user_cache:
                            time.sleep(0.6)
                            try:
                                user_details = reddit.redditor(author_name_submission)
                                user_cache[author_name_submission] = {
                                    'comment_karma': user_details.comment_karma,
                                    'has_verified_email': user_details.has_verified_email,
                                    'timestamp': user_details.created_utc,
                                    'is_mod': user_details.is_mod
                                    }
                            except Exception as e:
                                print(f"Error fetching user {author_name_submission}: {e}")
                        if author_name_submission in user_cache:
                            user_info = user_cache[author_name_submission] 
                            if author_name_submission not in author_data['author_id']:
                                submission_data['author_id'].append(author_name_submission)
                                author_data['author_id'].append(author_name_submission)
                                author_data['comment_karma'].append(user_info['comment_karma'])
                                author_data['has_verified_email'].append(user_info['has_verified_email'])
                                author_data['is_mod'].append(user_info['is_mod'])   
                                author_data['timestamp'].append(user_info['timestamp'])
                    if comment.author:
                        author_name_comment = str(comment.author)
                        if author_name_comment not in user_cache:
                            time.sleep(0.6)
                            try:
                                user_details = reddit.redditor(author_name_comment)
                                user_cache[author_name_comment] = {
                                    'comment_karma': user_details.comment_karma,
                                    'has_verified_email': user_details.has_verified_email,
                                    'timestamp': user_details.created_utc,
                                    'is_mod': user_details.is_mod
                                }
                            except Exception as e:
                                print(f"Error fetching user {author_name_comment}: {e}")
                        if author_name_comment in user_cache:
                            user_info = user_cache[author_name_comment] 
                            if author_name_comment not in author_data['author_id']:
                                comment_data['author_id'].append(author_name_comment)
                                author_data['author_id'].append(author_name_comment)
                                author_data['comment_karma'].append(user_info['comment_karma'])
                                author_data['has_verified_email'].append(user_info['has_verified_email'])
                                author_data['is_mod'].append(user_info['is_mod'])   
                                author_data['timestamp'].append(user_info['timestamp'])

def equalize_dictionaries(subreddit_data, submission_data, comment_data, author_data, placeholder=None):
    def equalize_single_dict(d, placeholder=None):
        max_length = max(len(lst) for lst in d.values() if isinstance(lst, list))
        for key, value in d.items():
            if isinstance(value, list) and len(value) < max_length:
                d[key] = value + [placeholder] * (max_length - len(value))
        return d

    subreddit_data = equalize_single_dict(subreddit_data, placeholder)
    submission_data = equalize_single_dict(submission_data, placeholder)
    comment_data = equalize_single_dict(comment_data, placeholder)
    author_data = equalize_single_dict(author_data, placeholder)
    
    return subreddit_data, submission_data, comment_data, author_data

def convert_to_dataframes(subreddit_data, submission_data, comment_data, author_data):
    subreddit_df = pd.DataFrame(subreddit_data)
    submission_df = pd.DataFrame(submission_data)
    comment_df = pd.DataFrame(comment_data)
    author_df = pd.DataFrame(author_data)

    submission_df['author_id'] = submission_df['author_id'].apply(lambda x: x.name if isinstance(x, praw.models.Redditor) else x)
    comment_df['author_id'] = comment_df['author_id'].apply(lambda x: x.name if isinstance(x, praw.models.Redditor) else x)

    return subreddit_df, submission_df, comment_df, author_df

def convert_columns_to_datetime(submission_df, comment_df, subreddit_df, author_df):
    submission_df['timestamp'] = pd.to_datetime(submission_df['timestamp'], unit='s')
    comment_df['timestamp'] = pd.to_datetime(comment_df['timestamp'], unit='s')
    subreddit_df['timestamp'] = pd.to_datetime(subreddit_df['timestamp'], unit='s')
    author_df['timestamp'] = pd.to_datetime(author_df['timestamp'], unit='s')

def apply_sentiment_analysis(submission, comment):
    sia = SentimentIntensityAnalyzer()
    nltk.download('vader_lexicon')

    def pos_vader_sentiment(text):
        if text is None:
            return None
        return sia.polarity_scores(text)['pos']
    
    def neg_vader_sentiment(text):
        if text is None:
            return None
        return sia.polarity_scores(text)['neg']
    
    def neu_vader_sentiment(text):
        if text is None:
            return None
        return sia.polarity_scores(text)['neu']
    
    def compound_vader_sentiment(text):
        if text is None:
            return None
        return sia.polarity_scores(text)['compound']

    submission['title_pos_vader_sentiment'] = submission['title'].apply(pos_vader_sentiment)
    submission['title_neg_vader_sentiment'] = submission['title'].apply(neg_vader_sentiment)
    submission['title_neu_vader_sentiment'] = submission['title'].apply(neu_vader_sentiment)
    submission['title_compound_vader_sentiment'] = submission['title'].apply(compound_vader_sentiment)

    submission['body_pos_vader_sentiment'] = submission['body'].apply(pos_vader_sentiment)
    submission['body_neg_vader_sentiment'] = submission['body'].apply(neg_vader_sentiment)
    submission['body_neu_vader_sentiment'] = submission['body'].apply(neu_vader_sentiment)
    submission['body_compound_vader_sentiment'] = submission['body'].apply(compound_vader_sentiment)
    
    comment['pos_vader_sentiment'] = comment['body'].apply(pos_vader_sentiment)
    comment['neg_vader_sentiment'] = comment['body'].apply(neg_vader_sentiment)
    comment['neu_vader_sentiment'] = comment['body'].apply(neu_vader_sentiment)
    comment['compound_vader_sentiment'] = comment['body'].apply(compound_vader_sentiment)

def save_to_database(subreddit_df, submission_df, comment_df, author_df,):
    engine = create_engine(get_connection_string())

    subreddit_dtypes = {
        'subreddit_id': NVARCHAR(50),
    }
    submission_dtypes = {
        'submission_id': NVARCHAR(50),
        'subreddit_id': NVARCHAR(50),
        'author_id': NVARCHAR(50)
    }
    comment_dtypes = {
        'comment_id': NVARCHAR(50),
        'submission_id': NVARCHAR(50),
        'subreddit_id': NVARCHAR(50),
        'author_id': NVARCHAR(50)
    }
    author_dtypes = {
        'author_id': NVARCHAR(50)
    }

    temp_comment = "temp_comment"
    comment_df.to_sql(name=temp_comment, con=engine, if_exists='replace', index=False, dtype=comment_dtypes)
    
    temp_submission = "temp_submission"
    submission_df.to_sql(name=temp_submission, con=engine, if_exists='replace', index=False, dtype=submission_dtypes)

    temp_subreddit = "temp_subreddit"
    subreddit_df.to_sql(name=temp_subreddit, con=engine, if_exists='replace', index=False, dtype=subreddit_dtypes)

    temp_author = "temp_author"
    author_df.to_sql(name=temp_author, con=engine, if_exists='replace', index=False, dtype=author_dtypes)

def get_connection_string():
    server = 'TABLET-JQOOCB8O\MYSQLSERVER'
    database = 'reddit'
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes")
    return f"mssql+pyodbc:///?odbc_connect={params}"

def main():
    reddit_client = initialize_reddit_client()
    subreddit_data, submission_data, comment_data, author_data = fetch_data(reddit_client)
    subreddit_data, submission_data, comment_data, author_data = equalize_dictionaries(
        subreddit_data, submission_data, comment_data, author_data
    )
    subreddit_df, submission_df, comment_df, author_df = convert_to_dataframes(
        subreddit_data, submission_data, comment_data, author_data
    )
    apply_sentiment_analysis(submission_df, comment_df)
    convert_columns_to_datetime(submission_df, comment_df, subreddit_df, author_df)
    save_to_database(subreddit_df, submission_df, comment_df, author_df)

if __name__ == "__main__":
    main()
