# Sales Analytics System

A Python-based system designed to ingest raw, messy sales data, clean it, and generate actionable business intelligence reports.

## Project Structure

- `data/`: Contains raw input files (e.g., `sales_data.txt`).
- `output/`: Stores generated reports.
- `utils/`: Helper modules for file I/O, data processing, and API simulation.
- `main.py`: The entry point for the application.

## Features

1. **Data Cleaning**: Handles missing values, removes negative prices, and corrects formatting (e.g., commas in numbers).
2. **Analysis**: Calculates total revenue, regional breakdown, and top products.
3. **Modular Design**: Separates logic into `file_handler`, `data_processor`, and `api_handler`.

## How to Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt