# Flight Data Analysis Project

## Project Overview

The project involves exploring and analyzing U.S. flight data, which is stored in Parquet files on a Spark cluster hosted within the Databricks environment.

## Dataset Description

The dataset contains detailed information about flights, including various attributes such as flight date, carrier code, departure and arrival times, delays, and more. Here's a brief description of the dataset columns:

| Variable              | Meaning                                                                 |
|-----------------------|-------------------------------------------------------------------------|
| FL_DATE               | Date of flight (YYYY-mm-dd)                                             |
| OP_CARRIER            | Airline code assigned by the International Air Transport Association   |
| OP_CARRIER_FL_NUM     | Flight number assigned by the airline                                    |
| ORIGIN                | IATA code for the airport of departure                                   |
| DEST                  | IATA code for the destination airport                                   |
| ...                   | (Other columns omitted for brevity)                                      |


## Milestones

### Part 1: [Basic Data Exploration]("file:///C:/Users/tilin/Downloads/Part%201_%20Basic%20Data%20Exploration%20(1).html")

Tasks for this part of the project:

- Prepare data: Clean, format, and consolidate into a Spark DataFrame.
- Analyze data: Calculate aggregates including flight counts, delay percentages, and statistics by carrier.

## Getting Started

To get started with the project, follow these steps:

1. Clone this repository to your local machine.

```bash
git clone <repository_url>
```

2. Set up your Databricks workspace and connect it to your Azure Data Lake Storage account.

```bash
# Install the Databricks CLI
pip install databricks-cli

# Configure the Databricks CLI with your access token
databricks configure --token

# Connect to your Databricks workspace
databricks workspace configure --url <workspace_url> --token

# Mount your Azure Data Lake Storage account
databricks fs mount --source adl://<storage_account_name>.azuredatalakestore.net/ --mount-point /mnt/<mount_name>
```

3. Use the provided notebooks to perform data exploration and analysis.

