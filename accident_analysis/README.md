# Accident Analytics

## Description

This project performs various data analyses on vehicle crash data using PySpark. The analyses include identifying crash trends, vehicle makes, and demographics involved in crashes.

## Structure

- `config/`: Configuration files.
- `data/`: Input and output data directories.
- `src/`: Source code for data reading, writing, and analysis.

## Setup

1. Install the required dependencies.
2. Place input data in the `data/input_data/` directory.
3. Update paths in `config/config.yaml`.

## Execution

Run the application with the following command:

```bash
spark-submit src/main.py
