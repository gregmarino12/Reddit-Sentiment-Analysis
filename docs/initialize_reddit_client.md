## Initializing PRAW and Setting Reddit Client Variables

set credentials as environment variables
mac:
windows:

alternatively, input your credentials directly in the main.py file

def initialize_reddit_client():
    return praw.Reddit(
        client_id=os.getenv('REDDIT_CLIENT_ID'),
        client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
        user_agent=os.getenv('REDDIT_USER_AGENT')
    )
