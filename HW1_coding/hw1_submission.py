def query_example():
  """
  This is just an example query to showcase the format of submission
  Please format the query in Bigquery console and paste it here.
  Submission starts from query_one.
  """
  return """
SELECT
 display_name,
 reputation,
 up_votes,
 down_votes
FROM
 `bigquery-public-data.stackoverflow.users`;
  """

def query_one():
    """Query one"""
    # add the formatted query between the triple quotes
    return """
SELECT
  display_name,
  reputation,
  up_votes,
  down_votes
FROM
  `bigquery-public-data.stackoverflow.users`
WHERE
  up_votes > 20000
  AND down_votes < 500
ORDER BY
  reputation DESC
LIMIT
  10
    """

def query_two():
    """Query two"""
    return """
SELECT
  location,
  COUNT(*) AS count
FROM
  `bigquery-public-data.stackoverflow.users`
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  10
    """

def query_three():
    """Query three"""
    return """
SELECT
  CASE
    WHEN location LIKE '%USA%' OR location LIKE '%United States%' THEN 'USA'
    WHEN location LIKE '%London%'OR location LIKE '%United Kingdom%' THEN 'UK'
    WHEN location LIKE '%France%' THEN 'France'
    WHEN location LIKE '%India%' THEN 'India'
    WHEN location LIKE '%Bangladesh%' THEN 'Bangladesh'
    WHEN location LIKE '%Canada%' THEN 'Canada'
    WHEN location LIKE '%Pakistan%' THEN 'Pakistan'
    WHEN location LIKE '%Germany%' THEN 'Germany'
  ELSE
  location
END
  AS country,
  COUNT(id) AS num_users
FROM
  `bigquery-public-data.stackoverflow.users`
WHERE
  location IS NOT NULL
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  20
    """

def query_four():
    """Query four"""
    return """
SELECT
  EXTRACT(YEAR
  FROM
    last_access_date) AS last_access_year,
  COUNT(*) AS num_users
FROM
  `bigquery-public-data.stackoverflow.users`
GROUP BY
  1
ORDER BY
  1 ASC

    """

def query_five():
    """Query five"""
    return """
SELECT
  id,
  display_name,
  last_access_date,
  DATE_DIFF(CAST('2023-01-07' AS DATE), CAST(last_access_date AS DATE), DAY) AS days_since_last_access,
  DATE_DIFF(CAST(last_access_date AS DATE), CAST(creation_date AS DATE), DAY) AS days_since_creation
FROM
  `bigquery-public-data.stackoverflow.users`
ORDER BY
  4 DESC,
  5 DESC,
  1 ASC
LIMIT
  10
    """

def query_six():
    """Query six"""
    return """
SELECT
  CASE
    WHEN reputation >= 0 AND reputation <= 100 THEN '0-100'
    WHEN reputation >= 101 AND reputation <= 1000 THEN '101-1000'
    WHEN reputation >= 1001 AND reputation <= 10000 THEN '1001-10000'
    WHEN reputation >= 10001 AND reputation <= 100000 THEN '10001-100000'
    WHEN reputation > 100000 THEN '>100000'
END
  AS reputation_bucket,
  ROUND(SUM(up_votes)/SUM(down_votes),2) AS upvote_ratio,
  COUNT(id) AS num_users
FROM
  `bigquery-public-data.stackoverflow.users`
GROUP BY
  1
ORDER BY
  3 DESC
    """

def query_seven():
    """Query seven"""
    return """
SELECT
  tag,
  COUNT(*) AS count
FROM
  `bigquery-public-data.stackoverflow.posts_questions`
CROSS JOIN
  UNNEST(SPLIT(tags, '|')) AS tag
WHERE
  EXTRACT(year
  FROM
    creation_date) = 2021
GROUP BY
  1
ORDER BY
  2 DESC,
  1 ASC
LIMIT
  10
    """

def query_eight():
    """Query eight"""
    return """
SELECT
  name,
  COUNT(id) AS num_users
FROM
  `bigquery-public-data.stackoverflow.badges`
WHERE
  class = 1
GROUP BY
  1
ORDER BY
  2 DESC,
  1 ASC
LIMIT
  10
    """

def query_nine():
    """Query nine"""
    return """
SELECT
  u.id,
  u.display_name,
  u.reputation,
  u.up_votes,
  u.down_votes,
  COUNT(b.id) AS num_gold_badges
FROM
  `bigquery-public-data.stackoverflow.users` AS u
LEFT JOIN
  `bigquery-public-data.stackoverflow.badges` AS b
ON
  u.id = b.user_id
WHERE
  b.class = 1
GROUP BY
  1,2,3,4,5
ORDER BY
  6 DESC,
  1 ASC
LIMIT
  10
    """

def query_ten():
    """Query ten"""
    return """
SELECT
  u.id,
  u.display_name,
  u.reputation,
  DATE_DIFF(CAST(b.date AS DATE), CAST(u.creation_date AS DATE), DAY) AS num_days
FROM
  `bigquery-public-data.stackoverflow.users` AS u
LEFT JOIN
  `bigquery-public-data.stackoverflow.badges` AS b
ON
  u.id = b.user_id
WHERE b.name = 'Illuminator'
ORDER BY
  4 ASC,
  1 ASC
LIMIT
  20
    """

def query_eleven():
    """Query eleven"""
    return """

SELECT
  CASE
    WHEN score < 0 THEN '<0'
    WHEN score >=0 AND score <= 100 THEN '0-100'
    WHEN score >=101 AND score <= 1000 THEN '101-1000'
    WHEN score >=1001 AND score <= 10000 THEN '1001-10000'
  ELSE
  '>10000'
END
  AS score_bucket,
  ROUND(AVG(view_count),2) AS avg_num_views
FROM
  `bigquery-public-data.stackoverflow.posts_questions`
GROUP BY
  1
ORDER BY
  2 asc

    """


def query_twelve():
    """Query twelve"""
    return """
SELECT
  EXTRACT(DAYOFWEEK FROM creation_date) AS day_num,
  COUNT(id) AS num_answers
FROM
  `bigquery-public-data.stackoverflow.posts_answers`
GROUP BY
  1
ORDER BY
  2 DESC,
  1 ASC


    """

def query_thirteen():
    """Query thirteen"""
    return """
SELECT
  EXTRACT(YEAR
  FROM
    creation_date) AS year,
  COUNT(*) AS num_questions,
  ROUND(SUM(CASE
        WHEN answer_count > 0 THEN 1
      ELSE
      0
    END
      )/COUNT(*)*100,2) AS percentage_answerd
FROM
  `bigquery-public-data.stackoverflow.posts_questions`
GROUP BY
  1
ORDER BY
  1 ASC
    """

def query_fourteen():
    """Query fourteen"""
    return """
SELECT
  u.id,
  u.display_name,
  u.reputation,
  COUNT(a.id) AS num_answers
FROM
  `bigquery-public-data.stackoverflow.users` AS u
LEFT JOIN
  `bigquery-public-data.stackoverflow.posts_answers` AS a
ON
  u.id = a.owner_user_id
GROUP BY
  1,
  2,
  3
HAVING
  COUNT(a.id) > 50
ORDER BY
  4 DESC,
  1 ASC
LIMIT
  20
    """

def query_fifteen():
    """Query fifteen"""
    return """

WITH temp1 AS(
  SELECT q.id, a.owner_user_id, q.tags
  FROM `bigquery-public-data.stackoverflow.posts_answers` AS a
    LEFT JOIN `bigquery-public-data.stackoverflow.posts_questions` AS q
      ON a.parent_id = q.id
  WHERE q.tags LIKE '%python%'
)

SELECT 
  u.id,
  u.display_name,
  u.reputation,
  COUNT(t.id) AS num_answers
FROM
  `bigquery-public-data.stackoverflow.users` AS u
LEFT JOIN
  temp1 AS t
ON
  u.id = t.owner_user_id
GROUP BY 1, 2, 3
HAVING COUNT(t.id) > 50
ORDER BY 4 DESC
LIMIT 20

    """

def query_sixteen():
    """Query sixteen"""
    return """

SELECT
  CASE
    WHEN q.score < 0 THEN '<0'
    WHEN q.score > 10000 THEN '>10000'
    ELSE 'else'
  END AS score,
  ROUND(AVG(q.answer_count),2) AS avg_answers,
  ROUND(AVG(q.favorite_count),2) AS avg_fav_count,
  ROUND(AVG(q.comment_count),2) AS avg_comments
FROM
  `bigquery-public-data.stackoverflow.posts_questions` AS q
WHERE q.score < 0 OR q.score > 10000
GROUP BY 1
ORDER BY 1 ASC

    """
