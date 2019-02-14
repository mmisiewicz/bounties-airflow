## Bounties Airflow Dags

These dags are responsible for moving around data and manipulating it for fun and profit.

## Structure

```
.
├── README.md
├── aggs
│   ├── agg_bounty_snapshot.py
│   └── agg_visitor_previous_visit.py
└── etl
    ├── generate_bounty_view_board.py
    ├── generate_user_page_log.py
    ├── load_data_from_s3.py
    └── load_data_from_s3_backfill.py

```
### `etl` dir

The Exchange, Transform, Load ("etl") DAGs are responsible for moving data from point A to point B. The most important DAG is `load_data_from_s3`, which does a few things. First, it looks on S3 for logs from the Bounties explorer. Next, it parses each line using a complex REGEX pattern. It creates a new Postgres 10 table partition, and then attaches it to a parent "raw_s3_logs" which is used in subsequent transformations. Lastly it attaches the newly created partition, and takes care of moving rows that straddle hour boundaries to their appropriate partitions (this happens when processing the files produced around midnight UTC). One other important detail is that the `load_data_from_s3` DAG keeps logs about data quality; two tables `files_processed` and `raw_regex_failure_lines` provide statistics on bad data and examples of raw bad data.

Additionally, two other DAGs `generate_user_page_log` and `generate_bounty_view_board` both depend on the `raw_s3_log` table being filled. They sense for the completion of the S3 ETL dag, and then generate repsectively the webserver access log, and log of which bounties have been viewed by which users. 

### `Aggs` dir

This directory contains all aggregations of data ("aggs"). It currently contains two DAGs, one for daily analysis of marketplace value, and another for determining a user's return status ("when did they first visit?").
