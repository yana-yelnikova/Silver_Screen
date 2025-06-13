# Silver Screen Cinema Efficiency Analysis (dbt Project)

This project is an ELT pipeline built on dbt to analyze data from the 'Silver Screen' cinema chain. The project's goal is to clean and unify data from various sources and create a single analytical data mart to assess the profitability of movies across different locations.

## Tech Stack
* **dbt Core**: For data transformation.
* **Snowflake**: As the data warehouse.
* **Git & GitHub**: For version control.

## Data Sources
This project uses 5 raw data sources provided by 'Silver Screen':

| Source Name       | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `movie_catalogue` | Contains detailed information about movies released in 2024.                  |
| `invoices`        | Invoices issued for showing specific movies at various theater locations.     |
| `nj_001`          | Detailed transactional data for all ticket sales from location NJ_001.        |
| `nj_002`          | Daily aggregated sales information from location NJ_002.                      |
| `nj_003`          | Transactional data for all product types (tickets, snacks, etc.) from location NJ_003. |

## Project Structure
This project uses a layered architecture to organize models, which is a dbt best practice:

* `models/staging`: Models for basic cleaning and standardization of raw data from the 5 sources. Each model in this layer corresponds to one source.
* `models/intermediate`: Intermediate models for unifying data from different sources. This is where the main sales aggregation logic resides.
* `models/marts`: The final data marts, ready for analysis and connection to BI tools. The key model is `fct_monthly_movie_performance`.
* `tests/`: Custom (singular) tests to check for complex business rules not covered by standard generic tests.

## How to Run the Project

1.  **Build Models and Run Tests:**
    To sequentially build all models and run all tests, use the command:
    ```bash
    dbt build
    ```
    Alternatively, you can run the commands separately:
    ```bash
    dbt run   # To build all models (tables/views)
    dbt test  # To run all data quality tests
    ```

2.  **Generate and View Documentation:**
    To generate the documentation site and view the dependency graph (DAG):
    ```bash
    dbt docs generate
    dbt docs serve
    ```

## Data Models Overview
The data pipeline executes the following steps:
1.  **Source Cleansing:** Data from `NJ_001`, `NJ_002`, `NJ_003`, `INVOICES`, and `MOVIE_CATALOGUE` is processed through `staging` models.
2.  **Sales Unification:** Cleansed sales data is unified and aggregated by month in the `int_monthly_sales` model.
3.  **Mart Creation:** The final model, `fct_monthly_movie_performance`, `JOIN`s aggregated sales, costs, and movie details, creating a single table for analysis.

### Key Features & Fixes
During development, the following data quality tasks were resolved:
* **Unreliable Invoice Data:** The `invoice_id` was found to be non-unique. The logic was changed to aggregate costs by a true business key (`month`, `location`, `movie`) to ensure correct cost calculation.
* **Custom Testing:** A custom SQL test was developed to verify the uniqueness of the column combination in the intermediate model, guaranteeing the correctness of the aggregation logic.
