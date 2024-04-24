# Simple ML Customer Segmentation - all in BigQuery!

This repo contains code that demonstrates how you can do ML based customer segmentation 
very easily using only BigQuery ML. To make things exciting, we'll use BigQuery's ability
to call Vertex AI LLMs (like Gemini) directly through SQL to generate human-readable
headlines and summaries of each segment by sending it sample data from the generated
segments.

## Prerequisites
You need a GCP project and access to the BigQuery public dataset, specifically `bigquery-public-data.thelook_ecommerce`.
You should also create a BigQuery dataset in the US region within your project to hold the model we'll create, as well as other 
artifacts.

You should have a Cloud Resource connection setup in your project so that BigQuery can call Vertex AI.
The documentation page [here](https://cloud.google.com/bigquery/docs/create-cloud-resource-connection) walks you through this process.

The streamlit app uses service account credentials that you need to specify in `.streamlit/secrets.toml`. To do this, 
create a Service Account with appropriate permissions to query BigQuery and download the credentials JSON file. Rename `.streamlit/secrets.toml.template`
to `.streamlit/secrets.toml` and replace the empty strings in this file with the information in the downloaded service account
credentials JSON file.

You should specify your Project ID and Dataset ID. To do this, open the `bigquery_sql/BigQuery.sql` file and find and replace all occurances of `my-project` with your project id and `my-dataset` with your dataset id. Do this in `app.py` as well.

Setup python prerequisites with
```bash
pip install -r requirements.txt
```

## To Run the Streamlit app
```bash
streamlit run app.py
```

Enjoy!
