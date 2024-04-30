USE reddit
GO

DELETE ts
FROM temp_submission ts
LEFT JOIN temp_author ta ON ts.author_id = ta.author_id
WHERE ta.author_id IS NULL;

DELETE ts
FROM temp_submission ts
LEFT JOIN temp_subreddit ta ON ts.subreddit_id = ta.subreddit_id
WHERE ta.subreddit_id IS NULL;

DELETE ts
FROM temp_comment ts
LEFT JOIN temp_author ta ON ts.author_id = ta.author_id
WHERE ta.author_id IS NULL;


DELETE ts
FROM temp_comment ts
LEFT JOIN temp_subreddit ta ON ts.subreddit_id = ta.subreddit_id
WHERE ta.subreddit_id IS NULL;

DELETE ts
FROM temp_comment ts
LEFT JOIN temp_submission ta ON ts.submission_id = ta.submission_id
WHERE ta.submission_id IS NULL;
