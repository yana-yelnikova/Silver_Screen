# Silver Screen Cinema Efficiency Analysis (dbt Project)

## 🎯 Business Problem
Entertainment Company "Silver Screen" acquired a chain of three movie theaters in New Jersey and needs to analyze the efficiency of these locations. The key challenge is that data comes from disparate, poorly formatted sources, making it impossible to get a unified view of performance.

This project is an ELT pipeline built on dbt to analyze data from the 'Silver Screen' cinema chain. The project's goal is to clean and unify data from various sources and create a single analytical data mart to assess the profitability of movies across different locations.

## Tech Stack
* **dbt Core**: For data transformation.
* **Snowflake**: As the data warehouse.
* **Git & GitHub**: For version control.

## Data Sources
This project is based on 5 raw data files (CSVs) provided by 'Silver Screen'. The source files are located in the `/seeds` directory of this repository and are loaded into the data warehouse using the `dbt seed` command.

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

## Data Lineage (DAG)

The project follows a layered architecture to ensure modularity and maintainability. The complete dependency graph (DAG) below visualizes the flow of data from the raw sources (`seeds`) on the left, through the staging and intermediate models, to the final analytical data mart on the right.

This structure makes the pipeline easy to debug, test, and scale.

![Project Data Lineage](screenshots/Lineage%20of%20the%20project.png)

## Final Output: The Analytical Data Mart

The final output of this ELT pipeline is the `fct_monthly_movie_performance` table. This model serves as a single source of truth, joining cleaned movie details, unified sales data, and monthly rental costs.

This mart is ready for direct connection to any BI tool (like Tableau, Power BI, or Looker) to build dashboards and analyze the profitability of each movie across all locations.

**Example Rows:**

| MOVIE_ID | MOVIE_TITLE | GENRE | STUDIO | MONTH | LOCATION | RENTAL_COST | TICKETS_SOLD | REVENUE |
|:---|:---|:---|:---|:---|:---|---:|---:|---:|
| mov_001 | Cosmic Explorers | Sci-Fi | Galaxy Studios | 2024-01-01 | nj_001 | 5000.00 | 1250 | 15000.00 |
| mov_002 | Midnight Shadows | Thriller | Dark Horse Pictures| 2024-01-01 | nj_001 | 4500.00 | 1100 | 13200.00 |
| mov_001 | Cosmic Explorers | Sci-Fi | Galaxy Studios | 2024-01-01 | nj_002 | 5200.00 | 1300 | 15600.00 |
| mov_003 | The Last Laugh | Comedy | FunTime Films | 2024-02-01 | nj_003 | 3800.00 | 950 | 11400.00 |

## How to Run the Project

1.  **Load Seed Data:**
    This command loads the raw data from the CSV files located in the `/seeds` directory into your data warehouse.
    ```bash
    dbt seed
    ```

2.  **Build Models and Run Tests:**
    To sequentially build all models and run all tests, use the command:
    ```bash
    dbt build
    ```
    Alternatively, you can run the commands separately:
    ```bash
    dbt run   # To build all models (tables/views)
    dbt test  # To run all data quality tests
    ```

3.  **Generate and View Documentation:**
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

## Data Quality & Key Challenges
Data from the sources was inconsistent and required several key transformations and quality checks:

* **Unifying Disparate Sources:** Developed logic to merge three different sales data structures (transactional `nj_001`/`nj_003` and aggregated `nj_002`) into a single, standardized format.
* **Handling Unreliable Invoice Keys:** The `invoice_id` was non-unique, making simple joins impossible. The logic was rebuilt to aggregate costs based on a true business key (`month`, `location`, `movie_id`), ensuring accurate profit calculation.
* **Robust Testing Strategy:** Implemented a combination of tests to guarantee data integrity:
    * **Generic Tests:** Used `not_null` and `unique` tests on primary keys across all models.
    * **Custom Test:** Developed a singular test (`assert_int_monthly_sales_is_unique.sql`) to verify that the combination of `month`, `location`, and `movie_id` is unique in the final model, preventing incorrect aggregations.

