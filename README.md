---
# EPL Data Engineering Pipeline
---

This project builds an end-to-end data pipeline that ingests English Premier League (EPL) team and player data via API, stores raw JSON files in AWS s3, applies data cleaning and transformation logic using Pandas, and loads cleaned data into a PostgreSQL database.


## Project Structure 
```
.
├── main.py                 # Entry point: Orchestrates the pipeline
├── clean_players.py        # Clean players data
├── clean_teams.py          # Clean teams data
├── docker-compose.yaml     # Optional: Local Postgres setup
├── ingest_players.py       # Fetches players from API and uploads raw to s3
├── ingest_teams.py         # Fetches teams from API and uploads raw data to s3
├── pg_upload.py            # Uploads cleaned data to Postgres
├── requirements.txt        # Libraries used for this project
├── src                     # Core pipeline components
│   ├── __init__.py         
│   ├── api_ingestor.py     # Handles requests to the balldontlie.io API   
│   ├── data_cleaner.py     # Modular class for data cleaning (drop nulls, cast, etc.)
│   ├── postgres_wrapper.py # Manages Postgres connection + COPY logic
│   ├── qa_checker.py        # Data quality assertions (set up for next phase of pipeline)
│   └── s3_wrapper.py        # Handles raw JSON uploads to S3
└── utilities                # Shared utility functions
    ├── __init__.py
    └── logger.py            # Centralized logging

```

## Pipeline Overview

### 1. Data Ingestion

    Source: https://api.balldontlie.io/

    Retrieves EPL teams and associated players

    Raw responses are stored as JSON in S3 (raw/json/epl/teams, players by team ID)

### 2. Data Cleaning

    Null removal

    Lowercase column normalization

    Type casting (e.g., height, weight as integers)

    Error handling for malformed data (e.g., string age formats)

### 3. Data Loading

    Cleaned data loaded into:

        epl_datapipeline.epl_teams

        epl_datapipeline.epl_team_players

    PostgreSQL upload handled using COPY FROM STDIN

## Schema Summary

### epl_datapipeline.epl_teams
| Column Name | Data Type         | Is Nullable |
| ----------- | ----------------- | ----------- |
| id          | integer           | NO          |
| stadium     | character varying | YES         |
| name        | character varying | YES         |
| short\_name | character varying | YES         |
| abbr        | character varying | YES         |
| city        | character varying | YES         |

### epl_datapipeline.epl_team_players
| Column Name    | Data Type         | Is Nullable |
| -------------- | ----------------- | ----------- |
| name           | character varying | YES         |
| position       | character varying | YES         |
| national\_team | character varying | YES         |
| first\_name    | character varying | YES         |
| last\_name     | character varying | YES         |
| birth\_date    | character varying | YES         |
| id             | integer           | NO          |
| height         | integer           | YES         |
| weight         | integer           | YES         |
| team\_id       | integer           | NO          |
| age            | character varying | YES         |

## Diagram
```mermaid
%%{ init : {
  "theme" : "default",
  "themeVariables": {
    "primaryColor": "#1e88e5",
    "secondaryColor": "#90caf9",
    "primaryTextColor": "#ffffff",
    "secondaryTextColor": "#000000",
    "lineColor": "#e3f2fd",
    "tertiaryColor": "#e0f7fa"
  }
}}%%
flowchart LR
    
    A -->|Raw JSON stored| B2[/Upload to S3/]

    A([API Source: balldontlie.io]) -->|Ingest with Python| B[Raw DataFrame: teams & players]
    
    B --> C1
        subgraph Transform Data
        C1[Drop Nulls]
        C2[Standardize Column Case]
        C3[Fill Nulls]
        C4[Cast Data Types]
        C1-->C2-->C3-->C4
    end

    C4 --> D[(Postgres: epl_teams)]
    C4 --> E[(Postgres: epl_team_players)]

    style D fill:#f9f,stroke:#333,stroke-width:1px
    style E fill:#f9f,stroke:#333,stroke-width:1px
```
## Getting started

### Clone This Repository

```bash
git clone https://github.com/YOUR_USERNAME/epl_data_pipeline.git
cd epl_data_pipeline
```

### Install Requirements 
**It's recommended to use a virtual environment:**
```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Environment Variables
**Create a .env file in the project root with the following variables**

```
API_KEY=your_api_key_here
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=your_s3_bucket_name
POSTGRES_URL=postgresql://user:password@localhost:5432/epl_db
```
**Make sure you never commit your .env file. Add it to your .gitignore**

### Run The Pipeline 
You can run the end-to-end pipeline by executing:
```
python main.py
```

## Sample Data Preview

### 'epl_teams' - sample rows

| id | name                     | short\_name | abbr | city        | stadium                  |
| -- | ------------------------ | ----------- | ---- | ----------- | ------------------------ |
| 1  | Arsenal                  | Arsenal     | ARS  | London      | Emirates Stadium         |
| 2  | Aston Villa              | Aston Villa | AVL  | Birmingham  | Villa Park               |
| 8  | Bournemouth              | Bournemouth | BOU  | Bournemouth | Vitality Stadium         |
| 10 | Brentford                | Brentford   | BRE  | Brentford   | Gtech Community Stadium  |
| 11 | Brighton and Hove Albion | Brighton    | BHA  | Falmer      | American Express Stadium |


### 'epl_team_players' - sample rows

| id    | position                      | national\_team | height | weight | birth\_date              | age               | name                      | first\_name     | last\_name       | team\_id |
| ----- | ----------------------------- | -------------- | ------ | ------ | ------------------------ | ----------------- | ------------------------- | --------------- | ---------------- | -------- |
| 74    | Centre Defensive Midfielder   | Ghana          | 185    | 77     | 1993-06-13T00:00:00.000Z | 31 years 348 days | Thomas Partey             | Thomas          | Partey           | 1        |
| 91    | Centre Central Midfielder     | Netherlands    | 177    | 70     | 2003-08-20T00:00:00.000Z | 21 years 280 days | Salah-Eddine Oulad M'hand | Salah-Eddine    | Oulad M'hand     | 1        |
| 25446 | Right Winger                  | Brazil         | 175    | 85     | 2003-04-07T00:00:00.000Z | 21 years 278 days | Marquinhos                | Marcus Vinicius | Oliveira Alencar | 1        |
| 68    | Centre/Right Central Defender | England        | 186    | 76     | 1997-10-08T00:00:00.000Z | 27 years 231 days | Ben White                 | Ben             | White            | 1        |
| 100   | Centre Central Defender       | Jamaica        | 185    | 75     | 2003-10-16T00:00:00.000Z | 21 years 149 days | Zane Monlouis             | Zane            | Monlouis         | 1        |

## Data Quality

    Built-in logger traces ingestion, cleaning, and load stages

    Handles:

        API 429 rate limits via time.sleep()

        Duplicate team-player relationships

        Type mismatches during load

## Future Enhancements

    DBT model layer for semantic modeling

    Unit test coverage for all components

    Schedule via Airflow or Prefect

    Docker containerization

## Learning Focus

This project is built to reflect real-world data engineering tasks:

    Modular architecture

    S3 + Postgres integration

    Reusable wrappers for API, DB, and S3

    End-to-end logging and QA readiness

## Tech Stack

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-blue?logo=postgresql&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange?logo=amazonaws&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Cleaning-black?logo=pandas&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Optional-lightblue?logo=docker&logoColor=white)
![v2-in-progress](https://img.shields.io/badge/version-v2--in--progress-yellow?style=flat-square)
