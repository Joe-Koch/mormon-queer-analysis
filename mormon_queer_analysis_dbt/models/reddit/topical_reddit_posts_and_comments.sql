WITH topical_reddit_posts AS (
    SELECT
        date,
        score,
        title || '\n' || selftext AS text,
        subreddit,
        name
    FROM
        {{ source('reddit', 'raw_reddit_posts') }}
    WHERE
        {{ var('lgbt_keywords_condition') }}
)

SELECT
    c.date,
    c.score,
    c.body AS text,
    c.subreddit
FROM
    {{ source('reddit', 'raw_reddit_comments') }} c
WHERE
    c.link_id IN (SELECT name FROM topical_reddit_posts)
    OR
    {{ var('lgbt_keywords_condition') }}

UNION ALL

SELECT
    p.date,
    p.score,
    p.text,
    p.subreddit
FROM
    topical_reddit_posts p