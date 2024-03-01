# Counterpart_Assesment
Assessment for Counterpart

## Data Processing

This project uses SQL for data processing. Here's an overview of the steps involved:

1. **Data Extraction**: The data is extracted from CSV files and loaded into a SQLite database using pandas' `read_csv` and `to_sql` functions in `mock_db.py`.

2. **Data Cleaning**: The data is cleaned by renaming columns for consistency across tables. This is done using SQL `SELECT` statements with column aliasing in `data_join.py`.

    ```sql
    select
        "Policy ID" as policy_id,
        "Quote ID" as quote_id,
        "Policy Created At Date" as policy_created_at_date
    from policies
    ```

    ```sql
    select
        "Quote ID" as quote_id,
        "Application ID" as application_id,
        "Quoted Date Date" as quote_created_at_date
    from quotes
    ```

    ```sql
    select
        "Application ID" as application_id,
        "State" as state,
        "Industry" as industry,
        "Submission ID" as submission_id,
        "Sample JSON" as sample_json
    from submissions
    ```
    Although this repo is working out of python, I would flatten the json out in Snowflake with this query

    ```sql
     select
                s.*,
                f.value:"BKC006"::INT AS BKC006,
                f.value:"CLC010"::INT AS CLC010,
                f.value:"DMO013"::STRING AS DMO013,
                f.value:"JDC010"::INT AS JDC010,
                f.value:"RTB031"::INT AS RTB031,
                f.value:"RTD059"::INT AS RTD059,
                f.value:"RTD060"::INT AS RTD060,
                f.value:"TTB020"::INT AS TTB020,
                f.value:"TTC038"::INT AS TTC038,
                f.value:"TTC051"::INT AS TTC051,
                f.value:"TXC010"::INT AS TXC010,
                f.value:"UCC002"::INT AS UCC002
            from submissions s,
            lateral flatten(input => s.sample_json) f
    ```
    I would utilize this query if the keys were static and consistent throughout the data. If the json varied, or the keys grew to a new legnth, I would then utilize a query similar to this:

    ```sql
        select
            s.*,
            f.key AS json_key,
            f.value::STRING AS json_value
        from  submissions s,
        LATERAL FLATTEN(input => PARSE_JSON(s.sample_json)) f;
    ```
    Although I would keep in mind that this would change the table to fan out, I would only implement if the JSON was needed to be seen with each id.

3. **Data Transformation**: The data is transformed to prepare it for analysis. This includes parsing JSON data in the `submissions` table and creating a new fact table that joins data from the `policies`, `quotes`, and `submissions` tables. This is done using SQL `SELECT` statements and pandas' `to_sql` function in `data_join.py`.

    ```sql
    select
        p.policy_id,
        q.quote_id,
        s.application_id,
        p.policy_created_at_date,
        q.quote_created_at_date
    from policies p
    left join quotes q on q.quote_id = p.quote_id
    left join submissions s on s.application_id = q.application_id
    ```
    This query is then saved as fact_submission
    ```python
    fact_quotes_submission.to_sql('fact_policy', conn, if_exists='replace', index=False)
    ```



4. **Data Analysis**: The processed data is analyzed to calculate the hit rate for each state and industry. This is done using SQL `WITH` statements and `SELECT` statements in `data_quality.py`.

    ```sql
    with state_counts as (
    select
        s.state,
        count(p.policy_id) as policy_count,
        count(q.quote_id) as quote_count
    from submissions s
    left join quotes q on q.application_id = s.application_id
    left join policies p on p.quote_id = q.quote_id
    group by s.state
    ),

    state_hit_rate as (
    select
        state,
        policy_count,
        quote_count,
        cast(policy_count as real) / nullif(quote_count,0) as hit_rate
    from state_counts
    group by state
    )

    select
        *
    from state_hit_rate
    where hit_rate is not null
    ```

    ```sql
    with industry_counts as (
        select
            s.industry,
            count(p.policy_id) as policy_count,
            count(q.quote_id) as quote_count
        from submissions s
        left join quotes q on q.application_id = s.application_id
        left join policies p on p.quote_id = q.quote_id
        group by s.industry
        ),
        industry_hit_rate as (
        select
            industry,
            policy_count,
            quote_count,
            cast(policy_count as real) / nullif(quote_count,0) as hit_rate
        from industry_counts
        group by industry
        )
        select
            *
        from industry_hit_rate
        where hit_rate is not null
    ```