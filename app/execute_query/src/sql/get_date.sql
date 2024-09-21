SELECT
  purchase_date,
  user_id,
  count(1) AS count
FROM
  `TEST_DATA.events`
