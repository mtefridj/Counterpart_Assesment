import pandas as pd
import sqlite3
import json

conn = sqlite3.connect('mock.db')

# modeling data into renaming the columns

query_policies = """
select
    "Policy ID" as policy_id,
    "Quote ID" as quote_id,
    "Policy Created At Date" as policy_created_at_date
from policies

"""
policies = pd.read_sql(query_policies, conn)

print(policies.head())

policies.to_sql('policies', conn, if_exists='replace', index=False)

policies_new = pd.read_sql('select * from policies', conn)


query_quotes = """
select
    "Quote ID" as quote_id,
    "Application ID" as application_id,
    "Quoted Date Date" as quote_created_at_date
from quotes
"""

quotes = pd.read_sql(query_quotes, conn)
quotes.to_sql('quotes', conn, if_exists='replace', index=False)

quotes_new = pd.read_sql('select * from quotes', conn)
print(quotes_new.head())

query_submissions = """
select
    "Application ID" as application_id,
    "State" as state,
    "Industry" as industry,
    "Submission ID" as submission_id,
    "Sample JSON" as sample_json
    from submissions
"""

submissions = pd.read_sql(query_submissions, conn)

submissions.to_sql('submissions', conn, if_exists='replace', index=False)

submissions_new = pd.read_sql('select * from submissions', conn)

for i, row in submissions_new.iterrows():
    if pd.notnull(row['sample_json']):
        json_data = json.loads(row['sample_json'])
        for key, value in json_data.items():
            submissions_new.at[i, key] = value

# In the case of flattening out and parsing json columns within snowflake, we can use the following code to parse the json data
            # select
            #     s.*,
            #     f.value:"BKC006"::INT AS BKC006,
            #     f.value:"CLC010"::INT AS CLC010,
            #     f.value:"DMO013"::STRING AS DMO013,
            #     f.value:"JDC010"::INT AS JDC010,
            #     f.value:"RTB031"::INT AS RTB031,
            #     f.value:"RTD059"::INT AS RTD059,
            #     f.value:"RTD060"::INT AS RTD060,
            #     f.value:"TTB020"::INT AS TTB020,
            #     f.value:"TTC038"::INT AS TTC038,
            #     f.value:"TTC051"::INT AS TTC051,
            #     f.value:"TXC010"::INT AS TXC010,
            #     f.value:"UCC002"::INT AS UCC002
            # from submissions s,
            # lateral flatten(input => s.sample_json) f



submissions_new.to_sql('submissions', conn, if_exists='replace', index=False)

print(submissions_new.head())

query_fact = """
select
    p.policy_id,
    q.quote_id,
    s.application_id,
    p.policy_created_at_date,
    q.quote_created_at_date
from policies p
left join quotes q on q.quote_id = p.quote_id
left join submissions s on s.application_id = q.application_id
"""

fact_quotes_submission = pd.read_sql(query_fact, conn)

fact_quotes_submission.to_sql('fact_submissions', conn, if_exists='replace', index=False)

fact_quotes_submission_new = pd.read_sql('select * from fact_submissions', conn)

print(fact_quotes_submission_new.head())

conn.close()

