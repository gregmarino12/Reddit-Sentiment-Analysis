/* ATTENTION: This query only runs with each line executed individually. We are working to solve this issue. Any notes on how to fix this would be greatly appreciated!*/
USE reddit
GO

DELETE FROM temp_author WHERE author_id IS NULL;
ALTER TABLE temp_author ALTER COLUMN author_id NVARCHAR(50) NOT NULL;
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_NAME = 'temp_author')
	ALTER TABLE temp_author ADD CONSTRAINT pk_author_id PRIMARY KEY (author_id);

DELETE FROM temp_subreddit WHERE subreddit_id IS NULL;
ALTER TABLE temp_subreddit ALTER COLUMN subreddit_id NVARCHAR(50) NOT NULL;
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_NAME = 'temp_subreddit')
ALTER TABLE temp_subreddit ADD CONSTRAINT pk_subreddit_id PRIMARY KEY (subreddit_id);

DELETE FROM temp_submission WHERE submission_id IS NULL;

;WITH Dupes AS (
    SELECT submission_id, COUNT(*) AS cnt
    FROM temp_submission
    GROUP BY submission_id
    HAVING COUNT(*) > 1
)
DELETE temp_submission
FROM temp_submission
JOIN Dupes ON temp_submission.submission_id = Dupes.submission_id;

ALTER TABLE temp_submission ALTER COLUMN submission_id NVARCHAR(50) NOT NULL;
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_NAME = 'temp_submission')
    ALTER TABLE temp_submission ADD CONSTRAINT pk_submission_id PRIMARY KEY (submission_id);

DELETE FROM temp_comment WHERE comment_id IS NULL;
ALTER TABLE temp_comment ALTER COLUMN comment_id NVARCHAR(50) NOT NULL;
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND TABLE_NAME = 'temp_comment')
ALTER TABLE temp_comment ADD CONSTRAINT pk_comment_id PRIMARY KEY (comment_id);


IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = 'fk_submission_author')
    ALTER TABLE temp_submission ADD CONSTRAINT fk_submission_author FOREIGN KEY (author_id) REFERENCES temp_author(author_id);
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = 'fk_submission_subreddit')
    ALTER TABLE temp_submission ADD CONSTRAINT fk_submission_subreddit FOREIGN KEY (subreddit_id) REFERENCES temp_subreddit(subreddit_id);
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = 'fk_comment_author')
    ALTER TABLE temp_comment ADD CONSTRAINT fk_comment_author FOREIGN KEY (author_id) REFERENCES temp_author(author_id);
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = 'fk_comment_subreddit')
    ALTER TABLE temp_comment ADD CONSTRAINT fk_comment_subreddit FOREIGN KEY (subreddit_id) REFERENCES temp_subreddit(subreddit_id);
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_NAME = 'fk_comment_submission')
    ALTER TABLE temp_comment ADD CONSTRAINT fk_comment_submission FOREIGN KEY (submission_id) REFERENCES temp_submission(submission_id);









