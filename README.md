# Ingestion Script

![CI CD](https://github.com/jyablonski/python_docker/actions/workflows/ci_cd.yml/badge.svg) [![Coverage Status](https://coveralls.io/repos/github/jyablonski/python_docker/badge.svg?branch=master)](https://coveralls.io/github/jyablonski/python_docker?branch=master)

Version: 1.13.7

## Ingestion Script

The Ingestion Script scrapes from the following sources to extract data and load it to PostgreSQL + S3:
- basketball-reference
- DraftKings
- Reddit Comments
- Twitter Tweets (RIP Q3 2023)

You'll need to configure your own Database credentials, S3 Bucket, and Reddit API Credentials in order for some of the functionality to work.

## Tests
To run tests locally, run `make test`.

The same Test Suite is ran after every commit on a PR via GitHub Actions.

## Project

![nba_pipeline_diagram](https://github.com/jyablonski/python_docker/assets/16946556/8c24a546-dca0-4785-acc6-7b6d3dad7195)

1. Links to other Repos providing infrastructure for this Project
    * [Dash Server](https://github.com/jyablonski/nba_elt_dashboard)
    * [dbt](https://github.com/jyablonski/nba_elt_dbt)
    * [Terraform](https://github.com/jyablonski/aws_terraform)
    * [ML Pipeline](https://github.com/jyablonski/nba_elt_mlflow)
    * [REST API](https://github.com/jyablonski/nba_elt_rest_api)
