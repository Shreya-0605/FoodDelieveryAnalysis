# Food Delivery Analytics

A simple food delivery analytics project using PySpark to analyze the dataset
and Streamlit to display the results in a dashboard.

## Project flow

```text
data/food_delivery_analysis_dataset.csv
              |
              v
src/food_delivery_analysis.py
              |
              v
outputs/*.csv
              |
              v
dashboard.py
```

## Requirements

- Python
- Java, required by PySpark
- Packages listed in `requirements.txt`

## Install

Run these commands from the project root in PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If the virtual environment already exists, activate it and install or update
the packages with:

```powershell
.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Run the analysis

Run the analysis from the project root so its relative dataset path resolves:

```powershell
python src\food_delivery_analysis.py
```

The script cleans and analyzes the dataset with PySpark, then creates the
`outputs` folder and writes:

- `orders_by_date.csv`
- `orders_by_hour.csv`
- `order_status.csv`
- `top_restaurants.csv`
- `customer_summary.csv`

## Run the dashboard

After the analysis has completed and generated the CSV files, run:

```powershell
python -m streamlit run dashboard.py
```

The dashboard displays order, customer, and restaurant summaries with an
Order Status filter. If it reports missing CSV files, run the PySpark
analysis first and wait for it to finish.
