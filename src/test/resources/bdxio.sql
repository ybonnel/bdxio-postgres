CREATE TABLE tweets_bdxio
(
  id BIGINT PRIMARY KEY NOT NULL,
  handle TEXT NOT NULL,
  tweet JSONB NOT NULL
);

--------------------------------------------
--- C'est quoi cette table tweet_bdxio
--------------------------------------------
\d tweets_bdxio

SELECT * FROM tweets_bdxio LIMIT 1;

SELECT jsonb_pretty(tweet) FROM tweets_bdxio LIMIT 1;


--------------------------------------------
--- Combien de tweet bdxio par jour?
--------------------------------------------
SELECT
  (tweet ->> 'createdAt') :: DATE AS day,
  count(*)                        AS nb
FROM tweets_bdxio
GROUP BY day
ORDER BY day DESC;


--------------------------------------------
--- Combien de tweet bdxio par jour de la semaine?
--------------------------------------------
SELECT
  count(*)                                          AS nb,
  date_part('dow', (tweet ->> 'createdAt') :: DATE) AS dow
FROM tweets_bdxio
GROUP BY dow
ORDER BY nb DESC;


--------------------------------------------
--- Qui tweet le plus sur bdxio?
--------------------------------------------
SELECT
  count(1) AS nb_tweet,
  handle
FROM tweets_bdxio
GROUP BY handle
ORDER BY nb_tweet DESC
LIMIT 10;
-- le handle c'est sympa, mais on a pas plus d'info?
SELECT
  count(1)                   AS nb_tweet,
  handle,
  tweet -> 'user' ->> 'name' AS name
FROM tweets_bdxio
GROUP BY handle, name
ORDER BY nb_tweet DESC
LIMIT 10;


--------------------------------------------
--- Qui a le plus de followers?
--------------------------------------------
SELECT
  handle,
  tweet -> 'user' ->> 'name'                             AS name,
  max((tweet -> 'user' ->> 'followersCount') :: INTEGER) AS nbFollowers
FROM tweets_bdxio
GROUP BY handle, name
ORDER BY nbFollowers DESC
LIMIT 10;


--------------------------------------------
--- Le tweet le plus retweeté?
--------------------------------------------
SELECT
  tweet -> 'retweetedStatus' ->> 'text'                           AS text,
  tweet -> 'retweetedStatus' -> 'user' ->> 'name'                 AS name,
  MAX((tweet -> 'retweetedStatus' ->> 'retweetCount') :: INTEGER) AS retweeted
FROM tweets_bdxio
WHERE tweet ? 'retweetedStatus'
GROUP BY text, name ORDER BY retweeted DESC
LIMIT 20;


--------------------------------------------
--- Les tweets loins du lieu (44.802614, -0.588054)
--------------------------------------------
SELECT
  handle,
  tweet -> 'user' ->> 'name',
  tweet ->> 'text',
  tweet -> 'place' ->> 'name'
FROM tweets_bdxio
WHERE tweet ? 'place'
ORDER BY POINT((tweet -> 'place' -> 'boundingBoxCoordinates' -> 0 -> 0 ->> 'latitude') :: DECIMAL,
               (tweet -> 'place' -> 'boundingBoxCoordinates' -> 0 -> 0 ->> 'longitude') :: DECIMAL) <->
         '(44.802614, -0.588054)' DESC
LIMIT 5;

-- On peux indexer
CREATE INDEX TEST
  ON tweets_bdxio USING GIST (
    POINT((tweet -> 'place' -> 'boundingBoxCoordinates' -> 0 -> 0 ->> 'latitude') :: DECIMAL,
          (tweet -> 'place' -> 'boundingBoxCoordinates' -> 0 -> 0 ->> 'longitude') :: DECIMAL))
  WHERE tweet ? 'place';


--------------------------------------------
--- Cas concret : une table utilisateur
--------------------------------------------
DROP TABLE users;
CREATE TABLE users
(
  id BIGINT PRIMARY KEY NOT NULL,
  name TEXT NOT NULL,
  pref JSONB
);

SELECT * from users;

INSERT INTO users (id, name, pref)
VALUES (1, 'Yan Bonnel', '{
  "tableSize": 10
}' :: JSONB);

INSERT INTO users (id, name, pref) VALUES (2, 'Frédéric Camblor', '{
  "tableSize": 20,
  "showPub": false
}' :: JSONB);

SELECT (pref ->> 'tableSize') :: INTEGER AS table_size
FROM users
WHERE id = 2;

SELECT (pref ->> 'showPub') :: BOOLEAN AS table_size
FROM users
WHERE id = 2;

SELECT json_agg(users) FROM users;


--------------------------------------------
--- Ok avec 3 lignes, mais avec des millions?
--------------------------------------------
CREATE TABLE tweets_bdxio_big
(
  id BIGINT PRIMARY KEY NOT NULL,
  handle TEXT NOT NULL,
  tweet JSONB NOT NULL
);


CREATE INDEX tweets_bdxio_big_handle
  ON tweets_bdxio_big (handle);
CREATE INDEX created_at_idx
  ON tweets_bdxio_big ((tweet ->> 'createdAt'));
CREATE INDEX retweet_count_idx
  ON tweets_bdxio_big (((tweet ->> 'retweetCount') :: INTEGER));
CREATE INDEX user_mention_idx
  ON tweets_bdxio_big USING GIN ((tweet -> 'userMentionEntities'));

-- Combien de tweets dans la table
SELECT count(1) FROM tweets_bdxio_big;

-- Les tweets les plus anciens
select tweet ->> 'createdAt',
  id, handle,
  tweet -> 'user' ->> 'name',
  tweet ->> 'text'
FROM tweets_bdxio_big
ORDER BY tweet ->> 'createdAt' asc LIMIT 10;

-- Le tweet le plus retweeté
SELECT
  handle,
  tweet,
  ((tweet ->> 'retweetCount') :: INTEGER) rtCount
FROM tweets_bdxio_big
ORDER BY rtCount DESC
LIMIT 5;


-- Les gens qui parlent le plus de moi
SELECT
  handle,
  count(1) AS nb_mentions
FROM tweets_bdxio_big
WHERE tweet -> 'userMentionEntities' @> '[{"screenName": "ybonnel"}]'
GROUP BY handle
ORDER BY nb_mentions DESC;



