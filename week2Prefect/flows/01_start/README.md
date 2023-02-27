## Ingesting NYC Taxi Trips Data
This script was made during my DE zoomcamp learning, it downloads and processes New York City yellow taxi trips data for January 2021, and ingests the resulting data into a PostgreSQL database using Prefect.

#### Requirements
- Python 3.x
- Prefect
- Pandas
- SQLAlchemy
- prefect-sqlalchemy

### Workflow
  **1.** The script downloads the taxi trips data CSV file from a GitHub repository, and unzips it.
  
  **2.** The extract_data task reads the CSV file using Pandas, and returns a DataFrame.

  **3.** The transform_data task filters out rows with missing passenger counts.

  **4.** The ingest_data task ingests the filtered data into a PostgreSQL database using SQLAlchemy.
  
  **5.** The main_flow function defines the workflow, which consists of the three tasks.
  
  **6.** The workflow is executed using Prefect.
  
