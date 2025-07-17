# Data Engineering Assessment

Welcome!  
This exercise evaluates your core **data-engineering** skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | relational modelling, normalisation, DDL/DML scripting        |
| Python ETL | data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0 Prerequisites & Setup

> **Allowed technologies**

- **Python ≥ 3.8** – all ETL / data-processing code
- **MySQL 8** – the target relational database
- **Lightweight helper libraries only** (e.g. `pandas`, `mysql-connector-python`).  
  List every dependency in **`requirements.txt`** and justify anything unusual.
- **No ORMs / auto-migration tools** – write plain SQL by hand.

---

## 1 Clone the skeleton repo

```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git
```

✏️ Note: Rename the repo after cloning and add your full name.

**Start the MySQL database in Docker:**

```
docker-compose -f docker-compose.initial.yml up --build -d
```

- Database is available on `localhost:3306`
- Credentials/configuration are in the Docker Compose file
- **Do not change** database name or credentials

For MySQL Docker image reference:
[MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

- You are provided with a raw JSON file containing property records is located in data/
- Each row relates to a property. Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.).
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.
- Use the supplied Field Config.xlsx (in data/) to understand business semantics.

### Task

- **Normalize the data:**

  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place your scripts in `sql/` and `scripts/`
  - The scripts should take the initial json to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

**Tech Stack:**

- Python (include a `requirements.txt`)
  Use **MySQL** and SQL for all database work
- You may use any CLI or GUI for development, but the final changes must be submitted as python/ SQL scripts
- **Do not** use ORM migrations—write all SQL by hand

---

## Submission Guidelines

- Edit the section to the bottom of this README with your solutions and instructions for each section at the bottom.
- Place all scripts/code in their respective folders (`sql/`, `scripts/`, etc.)
- Ensure all steps are fully **reproducible** using your documentation
- Create a new private repo and invite the reviewer https://github.com/mantreshjain

---

**Good luck! We look forward to your submission.**

## Solutions and Instructions (Filed by Candidate)

**Document your database design and solution here:**

- Explain your schema and any design decisions
- Give clear instructions on how to run and test your script

**Document your ETL logic here:**

- Outline your approach and design
- Provide instructions and code snippets for running the ETL
- List any requirements


### Schema Overview

The schema is designed to normalize JSON property data into a relational structure.

###  Tables

- **`properties`**  
  Core property information (one row per property).

- **`valuations`**  
  List price, rent, and other valuation metrics linked to a property.

- **`hoa_fees`**  
  HOA fee details per property.

- **`rehab_estimates`**  
  Rehab-related estimates for each property.

---

##  Design Decisions

- **Relational Integrity**  
  Foreign keys link child tables (`valuations`, `hoa_fees`, `rehab_estimates`) to `properties.id`.

- **Deduplication**  
  Composite deduplication on `(address, city, state)` ensures uniqueness in the `properties` table.

- **Child Table Deduplication**  
  Full-row comparison is used to avoid inserting duplicate rows in child tables.

- **Performance Optimization**  
  Data is inserted using chunking (batch inserts) for scalability and speed.

  ## How to run

  - The creation tables script in the /sql folder to be used to create all the tables required
  - Use the python ETL Code to load the tables
  - Use validation_queries.sql in the /sql folder to verify the insertion

  ##  ETL Logic Documentation

### 1. Approach & Design

- The script loads property data from a `JSON` file and inserts it into a `MySQL` database using Python.
- Deduplication is enforced:
  - For `properties`, using a composite key of `(address, city, state)`.
  - For child tables (`valuations`, `hoa_fees`, `rehab_estimates`), duplicates are avoided via full-row comparison.
- Batch processing with a chunk size of 1000 rows improves performance on large datasets.
- All inserts are wrapped in transactions to maintain data integrity and enable rollback on failure.

---

### 2. Running the ETL

####  Preparation

- Ensure your MySQL server is running and the schema (tables) is already created.
- Update the `DB_CONFIG` section in the script if your connection settings differ.
- Place the `fake_property_data.json` file in the same folder as the script.

####  Install Python Dependencies

```bash
pip install mysql-connector-python

```
### How to run


```bash
python etl_script.py 
```

***The etl script is available in /scripts folder***




