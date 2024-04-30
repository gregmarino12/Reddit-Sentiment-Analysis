USE reddit
GO

MERGE INTO author_table AS target
USING (
    SELECT 
        CAST(author_id AS NVARCHAR(50)) AS author_id,
        CAST(comment_karma AS BIGINT) AS comment_karma, 
        CAST(has_verified_email AS BIT) AS has_verified_email, 
		CAST(is_mod AS BIT) AS is_mod,
        CAST(timestamp AS DATETIME) AS timestamp
    FROM temp_author
) AS source
ON target.author_id = source.author_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.comment_karma = source.comment_karma,
        target.has_verified_email = source.has_verified_email,
        target.timestamp = source.timestamp,
		target.is_mod = source.is_mod
WHEN NOT MATCHED THEN 
    INSERT (author_id, comment_karma, has_verified_email, is_mod, timestamp) 
    VALUES (source.author_id, source.comment_karma, source.has_verified_email, source.is_mod, source.timestamp);

MERGE INTO subreddit_table AS target
USING (
    SELECT 
        CAST(subreddit_id AS NVARCHAR(50)) AS subreddit_id, 
        CAST(subreddit_name AS NVARCHAR(MAX)) AS subreddit_name,
        CAST(subscribers AS BIGINT) AS subscribers, 
        CAST(timestamp AS DATETIME) AS timestamp
    FROM temp_subreddit
) AS source
ON target.subreddit_id = source.subreddit_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.subreddit_name = source.subreddit_name, 
        target.subscribers = source.subscribers, 
        target.timestamp = source.timestamp
WHEN NOT MATCHED THEN 
    INSERT (subreddit_id, subreddit_name, subscribers, timestamp) 
    VALUES (source.subreddit_id, source.subreddit_name, source.subscribers, source.timestamp);

MERGE INTO submission_table AS target
USING (
    SELECT 
        CAST(submission_id AS NVARCHAR(50)) AS submission_id, 
        CAST(title AS NVARCHAR(MAX)) AS title, 
        CAST(body AS NVARCHAR(MAX)) AS body, 
        CAST(timestamp AS DATETIME) AS timestamp, 
        CAST(upvotes AS BIGINT) AS upvotes,
        CAST(num_comments AS BIGINT) AS num_comments, 
        CAST(subreddit_id AS NVARCHAR(50)) AS subreddit_id, 
        CAST(author_id AS NVARCHAR(50)) AS author_id, 
        CAST(title_pos_vader_sentiment AS FLOAT) AS title_pos_vader_sentiment, 
        CAST(title_neg_vader_sentiment AS FLOAT) AS title_neg_vader_sentiment, 
        CAST(title_neu_vader_sentiment AS FLOAT) AS title_neu_vader_sentiment,
        CAST(title_compound_vader_sentiment AS FLOAT) AS title_compound_vader_sentiment, 
        CAST(body_pos_vader_sentiment AS FLOAT) AS body_pos_vader_sentiment,
        CAST(body_neg_vader_sentiment AS FLOAT) AS body_neg_vader_sentiment, 
        CAST(body_neu_vader_sentiment AS FLOAT) AS body_neu_vader_sentiment,
        CAST(body_compound_vader_sentiment AS FLOAT) AS body_compound_vader_sentiment
    FROM temp_submission
) AS source
ON target.submission_id = source.submission_id 
WHEN MATCHED THEN 
    UPDATE SET 
        target.title = source.title, 
        target.body = source.body, 
        target.timestamp = source.timestamp, 
        target.upvotes = source.upvotes, 
        target.num_comments = source.num_comments,
        target.subreddit_id = source.subreddit_id,
        target.author_id = source.author_id,
        target.title_pos_vader_sentiment = source.title_pos_vader_sentiment,
        target.title_neg_vader_sentiment = source.title_neg_vader_sentiment,
        target.title_neu_vader_sentiment = source.title_neu_vader_sentiment,
        target.title_compound_vader_sentiment = source.title_compound_vader_sentiment, 
        target.body_pos_vader_sentiment = source.body_pos_vader_sentiment,
        target.body_neg_vader_sentiment = source.body_neg_vader_sentiment, 
        target.body_neu_vader_sentiment = source.body_neu_vader_sentiment,
        target.body_compound_vader_sentiment = source.body_compound_vader_sentiment
WHEN NOT MATCHED THEN 
    INSERT (submission_id, title, body, timestamp, upvotes, num_comments, subreddit_id, author_id, title_neg_vader_sentiment,
            title_pos_vader_sentiment, title_neu_vader_sentiment, title_compound_vader_sentiment, body_neg_vader_sentiment,
            body_pos_vader_sentiment, body_neu_vader_sentiment, body_compound_vader_sentiment) 
    VALUES (source.submission_id, source.title, source.body, source.timestamp, source.upvotes, source.num_comments,
			source.subreddit_id, source.author_id, source.title_pos_vader_sentiment, source.title_neg_vader_sentiment,
			source.title_neu_vader_sentiment, source.title_compound_vader_sentiment, source.body_pos_vader_sentiment, source.body_neg_vader_sentiment,
			source.body_neu_vader_sentiment, source.body_compound_vader_sentiment);

MERGE INTO comment_table AS target
USING (
    SELECT 
        CAST(comment_id AS NVARCHAR(50)) AS comment_id,
        CAST(body AS NVARCHAR(MAX)) AS body,
        CAST(timestamp AS DATETIME) AS timestamp,
        CAST(upvotes AS BIGINT) AS upvotes,
        CAST(subreddit_id AS NVARCHAR(50)) AS subreddit_id,
        CAST(submission_id AS NVARCHAR(50)) AS submission_id,
        CAST(author_id AS NVARCHAR(50)) AS author_id,
        CAST(pos_vader_sentiment AS FLOAT) AS pos_vader_sentiment,
        CAST(neg_vader_sentiment AS FLOAT) AS neg_vader_sentiment,
        CAST(neu_vader_sentiment AS FLOAT) AS neu_vader_sentiment,
        CAST(compound_vader_sentiment AS FLOAT) AS compound_vader_sentiment
    FROM temp_comment
) AS source
ON target.comment_id = source.comment_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.body = source.body, 
        target.timestamp = source.timestamp, 
        target.upvotes = source.upvotes, 
        target.subreddit_id = source.subreddit_id, 
        target.submission_id = source.submission_id,
        target.author_id = source.author_id, 
        target.pos_vader_sentiment = source.pos_vader_sentiment,
        target.neg_vader_sentiment = source.neg_vader_sentiment, 
        target.neu_vader_sentiment = source.neu_vader_sentiment,
        target.compound_vader_sentiment = source.compound_vader_sentiment
WHEN NOT MATCHED THEN 
    INSERT (
        comment_id, body, timestamp, upvotes, submission_id, subreddit_id, author_id,
        pos_vader_sentiment, neg_vader_sentiment, neu_vader_sentiment, compound_vader_sentiment
    ) VALUES (
        source.comment_id, source.body, source.timestamp, source.upvotes, source.submission_id, source.subreddit_id,
        source.author_id, source.pos_vader_sentiment, source.neg_vader_sentiment, source.neu_vader_sentiment,
        source.compound_vader_sentiment
    );

