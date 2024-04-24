------
-- Instructions:
-- 
-- Replace my-project with your project ID.
-- Replace my-dataset with your dataset ID.
-- Replace my-remote-connection with your Cloud Resource Connection name.
------


-- Step 1: Create a list of features
-- Replace the list in `DEP_CAT` in the following SQL with the generated list here.
select replace(regexp_replace(STRING_AGG(distinct CONCAT("'", department, '_', category, "'")), r'\s', ''), '&', '_')
from `bigquery-public-data.thelook_ecommerce.products`


-- Step 2: Create Customer Features Table
create or replace table my-project.my_dataset.customer_features
as (
with order_full as (
select o.order_id, o.user_id, o.status, o.gender, o.num_of_item, o.created_at,
  i.product_id, i.inventory_item_id, i.status as order_item_status, i.sale_price,
  p.department, p.category  from `bigquery-public-data.thelook_ecommerce.orders` o,
  `bigquery-public-data.thelook_ecommerce.order_items` i, `bigquery-public-data.thelook_ecommerce.products` p
  where o.order_id = i.order_id
  and i.product_id = p.id
  order by created_at desc
),
spend as (
select user_id,
CONCAT(department, '_', category) as dep_cat,
SUM(
CASE 
      WHEN created_at BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -365 DAY) AND CURRENT_TIMESTAMP()
      THEN sale_price*num_of_item
      ELSE 0
    END
) AS total_12_months,
SUM(
CASE 
      WHEN created_at BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -730 DAY) AND CURRENT_TIMESTAMP()
      THEN sale_price*num_of_item
      ELSE 0
    END
) AS total_24_months,
SUM(
CASE 
      WHEN created_at BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -1095 DAY) AND CURRENT_TIMESTAMP()
      THEN sale_price*num_of_item
      ELSE 0
    END
) AS total_36_months
from order_full
group by user_id, dep_cat
),
user_all as (
  select s.user_id, 
  s.dep_cat,
  s.total_12_months,
  s.total_24_months,
  s.total_36_months,
  u.age,
  u.gender,
  u.city,
  u.country,
  u.traffic_source
from spend s, 
`bigquery-public-data.thelook_ecommerce.users` u
where s.user_id = u.id
)
select *
from user_all
PIVOT (
  SUM(total_12_months) as total_12_months,
  SUM(total_24_months) AS total_24_months,
  SUM(total_36_months) AS total_36_months
  FOR dep_cat IN ( -- Replace this with the results from the previous SQL
   'Women_Accessories','Women_Plus','Men_Accessories','Women_Swim','Women_Active','Women_Socks_Hosiery','Men_Active','Men_Socks','Men_Swim','Women_Dresses','Women_Pants_Capris','Men_FashionHoodies_Sweatshirts','Women_Skirts','Women_Blazers_Jackets','Women_Suits','Women_Tops_Tees','Women_Sweaters','Women_FashionHoodies_Sweatshirts','Women_Shorts','Women_Jeans','Women_Maternity','Men_Shorts','Men_Sleep_Lounge','Men_Suits_SportCoats','Men_Pants','Women_Intimates','Women_Sleep_Lounge','Women_Outerwear_Coats','Men_Tops_Tees','Men_Underwear','Men_Jeans','Men_Sweaters','Women_Leggings','Women_Jumpsuits_Rompers','Men_Outerwear_Coats','Women_ClothingSets'
  )
)
);

-- Step 2: Cleanup our features, replace nulls with 0, etc.

-- Step 2a: AUtogenerate the SQL
select CONCAT("UPDATE `my-project.my-dataset.customer_features` SET ", 
STRING_AGG(CONCAT(column_name, " = ", "COALESCE(", column_name, ", 0) ")), 'WHERE true;')
 from `my-project`.my-dataset.INFORMATION_SCHEMA.COLUMNS
 where table_name = 'customer_features'
 and column_name like 'total%';


-- Step 2b: Replace the SQL below with the auto generated one from above.
UPDATE `my-project.my-dataset.customer_features` 
SET total_12_months_Women_Accessories = COALESCE(total_12_months_Women_Accessories, 0) ,total_24_months_Women_Accessories = COALESCE(total_24_months_Women_Accessories, 0) ,total_36_months_Women_Accessories = COALESCE(total_36_months_Women_Accessories, 0) ,total_12_months_Women_Plus = COALESCE(total_12_months_Women_Plus, 0) ,total_24_months_Women_Plus = COALESCE(total_24_months_Women_Plus, 0) ,total_36_months_Women_Plus = COALESCE(total_36_months_Women_Plus, 0) ,total_12_months_Men_Accessories = COALESCE(total_12_months_Men_Accessories, 0) ,total_24_months_Men_Accessories = COALESCE(total_24_months_Men_Accessories, 0) ,total_36_months_Men_Accessories = COALESCE(total_36_months_Men_Accessories, 0) ,total_12_months_Women_Swim = COALESCE(total_12_months_Women_Swim, 0) ,total_24_months_Women_Swim = COALESCE(total_24_months_Women_Swim, 0) ,total_36_months_Women_Swim = COALESCE(total_36_months_Women_Swim, 0) ,total_12_months_Women_Active = COALESCE(total_12_months_Women_Active, 0) ,total_24_months_Women_Active = COALESCE(total_24_months_Women_Active, 0) ,total_36_months_Women_Active = COALESCE(total_36_months_Women_Active, 0) ,total_12_months_Women_Socks_Hosiery = COALESCE(total_12_months_Women_Socks_Hosiery, 0) ,total_24_months_Women_Socks_Hosiery = COALESCE(total_24_months_Women_Socks_Hosiery, 0) ,total_36_months_Women_Socks_Hosiery = COALESCE(total_36_months_Women_Socks_Hosiery, 0) ,total_12_months_Men_Active = COALESCE(total_12_months_Men_Active, 0) ,total_24_months_Men_Active = COALESCE(total_24_months_Men_Active, 0) ,total_36_months_Men_Active = COALESCE(total_36_months_Men_Active, 0) ,total_12_months_Men_Socks = COALESCE(total_12_months_Men_Socks, 0) ,total_24_months_Men_Socks = COALESCE(total_24_months_Men_Socks, 0) ,total_36_months_Men_Socks = COALESCE(total_36_months_Men_Socks, 0) ,total_12_months_Men_Swim = COALESCE(total_12_months_Men_Swim, 0) ,total_24_months_Men_Swim = COALESCE(total_24_months_Men_Swim, 0) ,total_36_months_Men_Swim = COALESCE(total_36_months_Men_Swim, 0) ,total_12_months_Women_Dresses = COALESCE(total_12_months_Women_Dresses, 0) ,total_24_months_Women_Dresses = COALESCE(total_24_months_Women_Dresses, 0) ,total_36_months_Women_Dresses = COALESCE(total_36_months_Women_Dresses, 0) ,total_12_months_Women_Pants_Capris = COALESCE(total_12_months_Women_Pants_Capris, 0) ,total_24_months_Women_Pants_Capris = COALESCE(total_24_months_Women_Pants_Capris, 0) ,total_36_months_Women_Pants_Capris = COALESCE(total_36_months_Women_Pants_Capris, 0) ,total_12_months_Men_FashionHoodies_Sweatshirts = COALESCE(total_12_months_Men_FashionHoodies_Sweatshirts, 0) ,total_24_months_Men_FashionHoodies_Sweatshirts = COALESCE(total_24_months_Men_FashionHoodies_Sweatshirts, 0) ,total_36_months_Men_FashionHoodies_Sweatshirts = COALESCE(total_36_months_Men_FashionHoodies_Sweatshirts, 0) ,total_12_months_Women_Skirts = COALESCE(total_12_months_Women_Skirts, 0) ,total_24_months_Women_Skirts = COALESCE(total_24_months_Women_Skirts, 0) ,total_36_months_Women_Skirts = COALESCE(total_36_months_Women_Skirts, 0) ,total_12_months_Women_Blazers_Jackets = COALESCE(total_12_months_Women_Blazers_Jackets, 0) ,total_24_months_Women_Blazers_Jackets = COALESCE(total_24_months_Women_Blazers_Jackets, 0) ,total_36_months_Women_Blazers_Jackets = COALESCE(total_36_months_Women_Blazers_Jackets, 0) ,total_12_months_Women_Suits = COALESCE(total_12_months_Women_Suits, 0) ,total_24_months_Women_Suits = COALESCE(total_24_months_Women_Suits, 0) ,total_36_months_Women_Suits = COALESCE(total_36_months_Women_Suits, 0) ,total_12_months_Women_Tops_Tees = COALESCE(total_12_months_Women_Tops_Tees, 0) ,total_24_months_Women_Tops_Tees = COALESCE(total_24_months_Women_Tops_Tees, 0) ,total_36_months_Women_Tops_Tees = COALESCE(total_36_months_Women_Tops_Tees, 0) ,total_12_months_Women_Sweaters = COALESCE(total_12_months_Women_Sweaters, 0) ,total_24_months_Women_Sweaters = COALESCE(total_24_months_Women_Sweaters, 0) ,total_36_months_Women_Sweaters = COALESCE(total_36_months_Women_Sweaters, 0) ,total_12_months_Women_FashionHoodies_Sweatshirts = COALESCE(total_12_months_Women_FashionHoodies_Sweatshirts, 0) ,total_24_months_Women_FashionHoodies_Sweatshirts = COALESCE(total_24_months_Women_FashionHoodies_Sweatshirts, 0) ,total_36_months_Women_FashionHoodies_Sweatshirts = COALESCE(total_36_months_Women_FashionHoodies_Sweatshirts, 0) ,total_12_months_Women_Shorts = COALESCE(total_12_months_Women_Shorts, 0) ,total_24_months_Women_Shorts = COALESCE(total_24_months_Women_Shorts, 0) ,total_36_months_Women_Shorts = COALESCE(total_36_months_Women_Shorts, 0) ,total_12_months_Women_Jeans = COALESCE(total_12_months_Women_Jeans, 0) ,total_24_months_Women_Jeans = COALESCE(total_24_months_Women_Jeans, 0) ,total_36_months_Women_Jeans = COALESCE(total_36_months_Women_Jeans, 0) ,total_12_months_Women_Maternity = COALESCE(total_12_months_Women_Maternity, 0) ,total_24_months_Women_Maternity = COALESCE(total_24_months_Women_Maternity, 0) ,total_36_months_Women_Maternity = COALESCE(total_36_months_Women_Maternity, 0) ,total_12_months_Men_Shorts = COALESCE(total_12_months_Men_Shorts, 0) ,total_24_months_Men_Shorts = COALESCE(total_24_months_Men_Shorts, 0) ,total_36_months_Men_Shorts = COALESCE(total_36_months_Men_Shorts, 0) ,total_12_months_Men_Sleep_Lounge = COALESCE(total_12_months_Men_Sleep_Lounge, 0) ,total_24_months_Men_Sleep_Lounge = COALESCE(total_24_months_Men_Sleep_Lounge, 0) ,total_36_months_Men_Sleep_Lounge = COALESCE(total_36_months_Men_Sleep_Lounge, 0) ,total_12_months_Men_Suits_SportCoats = COALESCE(total_12_months_Men_Suits_SportCoats, 0) ,total_24_months_Men_Suits_SportCoats = COALESCE(total_24_months_Men_Suits_SportCoats, 0) ,total_36_months_Men_Suits_SportCoats = COALESCE(total_36_months_Men_Suits_SportCoats, 0) ,total_12_months_Men_Pants = COALESCE(total_12_months_Men_Pants, 0) ,total_24_months_Men_Pants = COALESCE(total_24_months_Men_Pants, 0) ,total_36_months_Men_Pants = COALESCE(total_36_months_Men_Pants, 0) ,total_12_months_Women_Intimates = COALESCE(total_12_months_Women_Intimates, 0) ,total_24_months_Women_Intimates = COALESCE(total_24_months_Women_Intimates, 0) ,total_36_months_Women_Intimates = COALESCE(total_36_months_Women_Intimates, 0) ,total_12_months_Women_Sleep_Lounge = COALESCE(total_12_months_Women_Sleep_Lounge, 0) ,total_24_months_Women_Sleep_Lounge = COALESCE(total_24_months_Women_Sleep_Lounge, 0) ,total_36_months_Women_Sleep_Lounge = COALESCE(total_36_months_Women_Sleep_Lounge, 0) ,total_12_months_Women_Outerwear_Coats = COALESCE(total_12_months_Women_Outerwear_Coats, 0) ,total_24_months_Women_Outerwear_Coats = COALESCE(total_24_months_Women_Outerwear_Coats, 0) ,total_36_months_Women_Outerwear_Coats = COALESCE(total_36_months_Women_Outerwear_Coats, 0) ,total_12_months_Men_Tops_Tees = COALESCE(total_12_months_Men_Tops_Tees, 0) ,total_24_months_Men_Tops_Tees = COALESCE(total_24_months_Men_Tops_Tees, 0) ,total_36_months_Men_Tops_Tees = COALESCE(total_36_months_Men_Tops_Tees, 0) ,total_12_months_Men_Underwear = COALESCE(total_12_months_Men_Underwear, 0) ,total_24_months_Men_Underwear = COALESCE(total_24_months_Men_Underwear, 0) ,total_36_months_Men_Underwear = COALESCE(total_36_months_Men_Underwear, 0) ,total_12_months_Men_Jeans = COALESCE(total_12_months_Men_Jeans, 0) ,total_24_months_Men_Jeans = COALESCE(total_24_months_Men_Jeans, 0) ,total_36_months_Men_Jeans = COALESCE(total_36_months_Men_Jeans, 0) ,total_12_months_Men_Sweaters = COALESCE(total_12_months_Men_Sweaters, 0) ,total_24_months_Men_Sweaters = COALESCE(total_24_months_Men_Sweaters, 0) ,total_36_months_Men_Sweaters = COALESCE(total_36_months_Men_Sweaters, 0) ,total_12_months_Women_Leggings = COALESCE(total_12_months_Women_Leggings, 0) ,total_24_months_Women_Leggings = COALESCE(total_24_months_Women_Leggings, 0) ,total_36_months_Women_Leggings = COALESCE(total_36_months_Women_Leggings, 0) ,total_12_months_Women_Jumpsuits_Rompers = COALESCE(total_12_months_Women_Jumpsuits_Rompers, 0) ,total_24_months_Women_Jumpsuits_Rompers = COALESCE(total_24_months_Women_Jumpsuits_Rompers, 0) ,total_36_months_Women_Jumpsuits_Rompers = COALESCE(total_36_months_Women_Jumpsuits_Rompers, 0) ,total_12_months_Men_Outerwear_Coats = COALESCE(total_12_months_Men_Outerwear_Coats, 0) ,total_24_months_Men_Outerwear_Coats = COALESCE(total_24_months_Men_Outerwear_Coats, 0) ,total_36_months_Men_Outerwear_Coats = COALESCE(total_36_months_Men_Outerwear_Coats, 0) ,total_12_months_Women_ClothingSets = COALESCE(total_12_months_Women_ClothingSets, 0) ,total_24_months_Women_ClothingSets = COALESCE(total_24_months_Women_ClothingSets, 0) ,total_36_months_Women_ClothingSets = COALESCE(total_36_months_Women_ClothingSets, 0) WHERE true;


-- Step 3: Create the k-means model

CREATE OR REPLACE MODEL
  `my-project.my-dataset.customer_segments_model`
OPTIONS
  ( MODEL_TYPE='KMEANS',
    NUM_CLUSTERS=HPARAM_RANGE(3, 5),
    KMEANS_INIT_METHOD='RANDOM',
    NUM_TRIALS=10
  ) AS
SELECT
  *
FROM
  `my-project.my-dataset.customer_features`



-- Step 4: Create the LLM model pointer
CREATE MODEL `my-project.my-dataset.gemini-llm`
 REMOTE WITH CONNECTION `my-project.us.my-remote-connection`
 OPTIONS(ENDPOINT = 'gemini-pro')

-- Run the model and generate customer segments
create or replace table `my-project.my-dataset.customer_segments` AS
SELECT
  * EXCEPT (nearest_centroids_distance)
FROM
  ML.PREDICT( MODEL `my-project.my-dataset.customer_segments_model`,
    (
    SELECT
      *
    FROM `my-project.my-dataset.customer_features`
    )
  ) 

-- Example prompt to generate flavortext using BQML
WITH sample_1 as (
  SELECT * EXCEPT (CENTROID_ID) FROM my-project.my-dataset.customer_segments TABLESAMPLE SYSTEM (0.0001 PERCENT)
  where CENTROID_ID = 4 limit 5
),
json_1 as (
  select string_agg(to_json_string(t, true)) as segment_1 from sample_1 t
)
select * from ML.GENERATE_TEXT(
    MODEL `my-project.my-dataset.gemini-llm`,
      (SELECT 'The following json represents a sample of customers that have been clustered together as part of a customer segment. Generate a short heading and description for this customer segment. Results MUST be in JSON with the following keys - heading, description. Do not return results in any other format. Do not add any additional keys or use any other key names.' || segment_1 AS prompt FROM json_1),
      STRUCT(
        0.4 AS temperature, 
        750 AS max_output_tokens
      )
  );




-- Top 5 per segment by LTV
with aggs as (
select o.user_id, min(o.created_at) first_order,
sum(i.sale_price * o.num_of_item) total_ltv,
sum(
  CASE WHEN o.created_at BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -365 DAY) AND CURRENT_TIMESTAMP()
      THEN sale_price*num_of_item
      ELSE 0
  END
) as last_365_spend
from
`bigquery-public-data.thelook_ecommerce.orders` o,
  `bigquery-public-data.thelook_ecommerce.order_items` i, `bigquery-public-data.thelook_ecommerce.products` p
WHERE
 o.order_id = i.order_id
 AND i.product_id = p.id
 GROUP BY o.user_id
),
user_aug as (
select u.id, u.first_name || ' ' || u.last_name,
u.gender,
u.age,
u.city,
u.country,
FORMAT_TIMESTAMP("%Y %b", a.first_order) as first_order,
a.total_ltv,
a.last_365_spend
 from aggs a, `bigquery-public-data.thelook_ecommerce.users` u
where 
u.id = a.user_id
),
top_spender_by_segment_last_365 as (
  select 
  cs.CENTROID_ID segment, ua.*,
  RANK() over (PARTITION BY cs.CENTROID_ID ORDER BY ua.total_ltv desc) rnk
  FROM my-project.my-dataset.customer_segments cs, user_aug ua
  WHERE ua.id = cs.user_id
)
select * from top_spender_by_segment_last_365
where rnk < 5

-- Top 5 counteries per segment by spend

with aggs as (
select o.user_id, min(o.created_at) first_order,
sum(i.sale_price * o.num_of_item) total_ltv,
sum(
  CASE WHEN o.created_at BETWEEN TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -365 DAY) AND CURRENT_TIMESTAMP()
      THEN sale_price*num_of_item
      ELSE 0
  END
) as last_365_spend
from
`bigquery-public-data.thelook_ecommerce.orders` o,
  `bigquery-public-data.thelook_ecommerce.order_items` i, `bigquery-public-data.thelook_ecommerce.products` p
WHERE
 o.order_id = i.order_id
 AND i.product_id = p.id
 GROUP BY o.user_id
),
user_aug as (
select u.id, u.country,
a.last_365_spend
 from aggs a, `bigquery-public-data.thelook_ecommerce.users` u
where 
u.id = a.user_id
),
top_spending_countries_last_365 as (
  select 
  cs.CENTROID_ID segment, ua.country, sum(ua.last_365_spend) last_365_spend
  FROM my-project.my-dataset.customer_segments cs, user_aug ua
  WHERE ua.id = cs.user_id
  group by cs.CENTROID_ID, ua.country
),
ranked as (
select rank() over (partition by segment order by last_365_spend desc) as rnk, 
  tsc.*
 from top_spending_countries_last_365 tsc
)
select * from ranked where rnk <= 5
