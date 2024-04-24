# streamlit_app.py

import streamlit as st
from google.oauth2 import service_account
from google.cloud import bigquery
import json

st.set_page_config(layout="wide")

# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

st.title("Customer Segmentation Demo")
body_text = """
The 4 step process to Customer Segmentation using BQML!  

We're using the `bigquery-public-data.thelook_ecommerce` dataset for this.

Step 1: We use the `USERS`, `PRODUCT`, `ORDER` and `ORDER_DETAIL` tables to generate features for our K-Means Model   
Step 2: We train a K-Means model using BigQueryML  
Step 3: We use the model to segment our customers into 4 segments  
Step 4: We sample the data for each segment, send it to Gemini and generate a short heading and description for each segment  
"""
st.write(body_text)


def generate_segment_flavortext(centroid_number, temp=0.2, max_output_tokens=750, limit=5, prompt=None, sample_percent=0.001):
    if not prompt:
        prompt = """The following json represents a sample of customers that have been clustered 
        together. 
        Generate a short heading and description for this customer segment. 
        Results MUST BE FORMATTED AS RAW JSON with the following keys - heading, description. 
        Do not return results in any other format. Do not return results within a markdown block. 
        Do not add any additional keys or use any other key names.
        ---
        """

    sql_str = """
    WITH sample_1 as (
        SELECT * EXCEPT (CENTROID_ID) FROM my-project.my-dataset.customer_segments 
    """  \
    + f"TABLESAMPLE SYSTEM ({sample_percent} PERCENT)" \
    + f"where CENTROID_ID = {centroid_number} limit {limit}" \
    + """
    ),
    json_1 as (
        select string_agg(to_json_string(t, true)) as segment_1 from sample_1 t
    )
    select * from ML.GENERATE_TEXT(
        MODEL `my-project.my-dataset.gemini-llm`,
        (
            SELECT """ \
                + f"'{prompt}' || segment_1 AS prompt FROM json_1)," \
                + """
                    STRUCT(
                    """ \
                    + f"{temp} AS temperature, {max_output_tokens} AS max_output_tokens" \
                    + """
                    )
    );
    """

    return sql_str


def top_n_ltv_by_segment(n):
    sql_str = """
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
        select u.id, u.first_name || ' ' || u.last_name customer_name,
        u.gender,
        u.age,
        u.city,
        u.country,
        FORMAT_TIMESTAMP("%Y %b", a.first_order) as first_order_date,
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
        """ \
        + f"where rnk <= {n}"
    return sql_str

def top_n_countries_by_segment(n):
    sql_str = """
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
        #RANK() over (PARTITION BY cs.CENTROID_ID ORDER BY sum(ua.last_365_spend) desc) rnk
        FROM my-project.my-dataset.customer_segments cs, user_aug ua
        WHERE ua.id = cs.user_id
        group by cs.CENTROID_ID, ua.country
        ),
        ranked as (
        select rank() over (partition by segment order by last_365_spend desc) as rnk, tsc.*
        from top_spending_countries_last_365 tsc
        )""" \
        + f"select * from ranked where rnk <= {n} order by segment, rnk"
    return sql_str

# Perform query.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=1200)
def run_query(query):
    query_job = client.query(query)
    rows_raw = query_job.result()
    # Convert to list of dicts. Required for st.cache_data to hash the return value.
    rows = [dict(row) for row in rows_raw]
    return rows

@st.cache_data(ttl=600)
def run_query_df(query):
    query_job = client.query(query)
    df = query_job.result().to_dataframe()
    return df


rows = run_query("SELECT word FROM `bigquery-public-data.samples.shakespeare` LIMIT 10")

llm_prompt = "The following json represents a sample of customers that have been clustered together as part of a customer segment. Generate a short heading and description for this customer segment. Results MUST be in JSON with the following keys - heading, description. Do not return results in any other format. Do not add any additional keys or use any other key names."

temp = 0.2
max_output_tokens=1024
sample_perc = 0.01
limit = 5

with st.expander("Query and LLM Parameters"):
    llm_prompt = st.text_area(
    "Prompt used with the data for each segment.",
    llm_prompt,
    height=200,
    )

    temp = st.slider(
        "Temperature",
        min_value=0.0,
        max_value=1.0,
        value=0.2,
        step=0.05,
    )

    max_output_tokens = st.slider(
        "Max Output Tokens",
        min_value=256,
        max_value=2048,
        value=1024,
        step=12,
    )

    sample_perc = st.slider(
        "SQL Sampling Percentage",
        min_value=0.01,
        max_value=100.00,
        value=0.01,
        step=0.01
    )

    limit = st.slider(
        "SQL Sampling Query Limit",
        min_value=1,
        max_value=20,
        value=5,
        step=1,
    )



# Get segment titles and descriptions
with st.expander("SQL Generated"):
    st.code(
        generate_segment_flavortext(1, temp, max_output_tokens, limit, llm_prompt, sample_perc),
        language='SQL',
    )

# Print results.
seg_results = []


with st.expander(f"Raw Results"):
    for seg in range(1,5):
        rows = run_query(generate_segment_flavortext(seg, temp, max_output_tokens, limit, llm_prompt, sample_perc))
        st.subheader(f"Segment {seg}")
        st.json(rows)
        try:
            result = rows[0]['ml_generate_text_result']['candidates'][0]['content']['parts'][0]['text']
            st.json(result)    
            result_dict = json.loads(result)
        except json.JSONDecodeError as e:
            st.write("JSON badly formatted. Checking if in markdown block.")
            result_lines = result.splitlines()
            if result_lines[0].strip() == "```json" or result_lines[0].strip() == "```JSON":
                st.write("Markdown block detected. Cleaning up.")
                result_lines = result_lines[1:]
            if result_lines[-1].strip() == "```":
                result_lines = result_lines[:-1]
            result = "\n".join(result_lines)
            st.write(f"New result object is {result}")
            result_dict = json.loads(result)
        heading=result_dict['heading']
        description=result_dict['description']
        #st.write(f"Heading is {heading}")
        #st.write(f"Description is {description}")
        seg_results.append({"heading": heading, "description": description})



#st.header("Customer Segments")
st.write(f"We have {len(seg_results)} valid segments.")

top_n_by_ltv = run_query_df(top_n_ltv_by_segment(5))
top_n_countries_by_365 = run_query_df(top_n_countries_by_segment(5))

#st.write(seg_results)
for idx, val in enumerate(seg_results):
    st.header(f"Segment {idx+1}: {val['heading']}", divider="red")
    st.write(val['description'])
    st.subheader("Top 5 Spenders by Total LTV")
    st.dataframe(top_n_by_ltv[top_n_by_ltv['segment']==idx+1][['customer_name', 'gender', 'age', 'city', 'country', 'first_order_date', 'last_365_spend', 'total_ltv']], hide_index=True)
    st.subheader("Top 5 Countries by Spend in Last 365 Days")
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(top_n_countries_by_365[top_n_countries_by_365['segment']==idx+1][['country','last_365_spend']])
    with col2:
        st.bar_chart(top_n_countries_by_365[top_n_countries_by_365['segment']==idx+1][['country','last_365_spend']], x = "country", y="last_365_spend")
    
    #st.write(val)




#st.write(seg1)
        
