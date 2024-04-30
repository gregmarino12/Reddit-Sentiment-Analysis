USE reddit
GO

CREATE TABLE author_table (
	author_id NVARCHAR(50) NOT NULL PRIMARY KEY,
	comment_karma BIGINT NULL,
	has_verified_email BIT NULL,
	is_mod BIT NULL,
	timestamp DATETIME NULL
);


CREATE TABLE subreddit_table (
	subreddit_id NVARCHAR(50) NOT NULL PRIMARY KEY,
	subreddit_name NVARCHAR(MAX) NOT NULL,
	subscribers BIGINT NOT NULL,
	timestamp DATETIME NOT NULL
);


CREATE TABLE submission_table (
	submission_id NVARCHAR(50) NOT NULL PRIMARY KEY,
	title NVARCHAR(MAX) NULL,
	body NVARCHAR(MAX) NULL,
	timestamp DATETIME NULL,
	upvotes BIGINT NULL,
	num_comments BIGINT NULL,
	subreddit_id NVARCHAR(50) NULL CONSTRAINT fk_submission_subreddit_id FOREIGN KEY (subreddit_id) REFERENCES subreddit_table(subreddit_id),
	author_id NVARCHAR(50) NULL CONSTRAINT fk_submission_author_id FOREIGN KEY (author_id) REFERENCES author_table(author_id),
	title_pos_vader_sentiment FLOAT NULL,
	title_neg_vader_sentiment FLOAT NULL,
	title_neu_vader_sentiment FLOAT NULL,
	title_compound_vader_sentiment FLOAT NULL,
	body_pos_vader_sentiment FLOAT NULL,
	body_neg_vader_sentiment FLOAT NULL,
	body_neu_vader_sentiment FLOAT NULL,
	body_compound_vader_sentiment FLOAT NULL
 );


CREATE TABLE comment_table (
	comment_id NVARCHAR(50) NOT NULL PRIMARY KEY,
	body NVARCHAR(MAX) NULL,
	timestamp DATETIME NULL,
	upvotes BIGINT NULL,
	subreddit_id NVARCHAR(50) NULL CONSTRAINT fk_comment_subreddit_id FOREIGN KEY (subreddit_id) REFERENCES subreddit_table(subreddit_id),
	submission_id NVARCHAR(50) NULL CONSTRAINT fk_comment_submission_id FOREIGN KEY (submission_id) REFERENCES submission_table(submission_id),
	author_id NVARCHAR(50) NULL CONSTRAINT  fk_comment_author_id FOREIGN KEY (author_id) REFERENCES author_table(author_id),
	pos_vader_sentiment FLOAT NULL,
	neg_vader_sentiment FLOAT NULL,
	neu_vader_sentiment FLOAT NULL,
	compound_vader_sentiment FLOAT NULL
);
