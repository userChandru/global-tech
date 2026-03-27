# Global Tech Data Platform - Technical Documentation

## Document Information

**Project Name:** Global Tech Data Platform  
**Version:** 1.0  
**Last Updated:** March 27, 2026  
**Owner:** Data Engineering Team  
**Platform:** Databricks on AWS  

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Data Pipeline Layers](#data-pipeline-layers)
4. [Schema Definitions](#schema-definitions)
5. [Data Flow and Transformations](#data-flow-and-transformations)
6. [Key Metrics and KPIs](#key-metrics-and-kpis)
7. [Technology Stack](#technology-stack)
8. [Data Lineage](#data-lineage)
9. [Deployment and Operations](#deployment-and-operations)
10. [Best Practices and Conventions](#best-practices-and-conventions)

---

## 1. Executive Summary

The Global Tech Data Platform is a comprehensive enterprise data warehouse solution built on Databricks using the Medallion Architecture (Bronze-Silver-Gold). The platform ingests, processes, and analyzes financial and HR data for TechNova Industries and Nova AI, providing actionable insights through data cubes and KPI dashboards.

### Key Capabilities

* **Automated Data Ingestion**: Batch loading from S3-based CSV files
* **Multi-Layer Architecture**: Bronze (raw), Silver (cleansed), and Gold (analytical) layers
* **Dimensional Modeling**: Star schema with fact and dimension tables
* **Unified Data Cubes**: Combined financial and payroll analytics
* **Business Intelligence**: Pre-calculated KPIs for revenue, costs, margins, and payroll metrics

### Data Domains

* **Financial Data**: General ledger transactions, accounts, chart of accounts
* **HR & Payroll**: Employee information, compensation, benefits, deductions
* **Master Data**: Companies, departments, organizational hierarchy
* **Time Intelligence**: Comprehensive date dimension with fiscal calendars

---

## 2. Architecture Overview

### 2.1 Medallion Architecture

The platform follows the Medallion Architecture pattern with three distinct layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAW DATA SOURCES                        │
│                    (S3: global-tech-raw)                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (01_bronze)                   │
│                   Schema: 01_global_tech_bronze                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Raw Tables: Ingested as-is from source                  │  │
│  │  - accounts_raw                                          │  │
│  │  - company_raw                                           │  │
│  │  - departments_raw                                       │  │
│  │  - employee_raw                                          │  │
│  │  - general_ledgers_raw                                   │  │
│  │  - payroll_raw                                           │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (02_silver)                    │
│                  Schema: 02_global_tech_silver                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Transformed Tables: Cleansed and standardized           │  │
│  │  - transformed_accounts                                  │  │
│  │  - transformed_company                                   │  │
│  │  - transformed_departments                               │  │
│  │  - transformed_employee                                  │  │
│  │  - transformed_general_ledgers                           │  │
│  │  - transformed_payroll                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER (03_gold)                      │
│                   Schema: 03_global_tech_gold                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Dimension Tables (dims_tables)                          │  │
│  │  - dim_date                                              │  │
│  │  - dim_company                                           │  │
│  │  - dim_department                                        │  │
│  │  - dim_employee                                          │  │
│  │  - dim_account                                           │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Fact Tables (facts_tables)                              │  │
│  │  - fact_general_ledger                                   │  │
│  │  - fact_payroll                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Data Cubes (data_cube)                                  │  │
│  │  - gl_data_cube (General Ledger enriched)               │  │
│  │  - payroll_data_cube (Payroll enriched)                 │  │
│  │  - unified_data_cube (Combined financial + payroll)     │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   BI & Analytics│
                    │   Dashboards    │
                    │   KPIs          │
                    └─────────────────┘
```

### 2.2 Catalog and Schema Organization

**Bronze Catalog:** `01_global_tech_bronze`
* Schema: `raw_tables`

**Silver Catalog:** `02_global_tech_silver`
* Schema: `transformed_tables`

**Gold Catalog:** `03_global_tech_gold`
* Schemas:
  * `dims_tables` - Dimension tables
  * `facts_tables` - Fact tables
  * `data_cube` - Analytical data cubes

---

## 3. Data Pipeline Layers

### 3.1 Bronze Layer (Raw Data Ingestion)

**Notebook:** `nb_01_bronze.ipynb`  
**Purpose:** Ingest raw data from S3 and store in Delta format without transformations

#### Process Flow

1. **Source Location**: S3 bucket `s3://global-tech-raw/raw_files/`
2. **File Format**: CSV with headers
3. **Ingestion Method**: 
   ```python
   spark.read.format("csv")
     .options(header=True, inferSchema=True)
     .load(f"s3://global-tech-raw/raw_files/{file}")
   ```
4. **Storage Format**: Delta Lake
5. **Write Mode**: Overwrite

#### Source Files

* `accounts.csv` → `accounts_raw`
* `company.csv` → `company_raw`
* `departments.csv` → `departments_raw`
* `employee.csv` → `employee_raw`
* `general ledgers.csv` → `general_ledgers_raw`
* `payroll.csv` → `payroll_raw`

#### Key Characteristics

* **No Data Transformation**: Data stored exactly as received
* **Schema Inference**: Automatic type detection
* **Audit Trail**: Preserves original data for compliance
* **Full Refresh**: Complete replacement on each load

---

### 3.2 Silver Layer (Cleansed and Standardized)

**Purpose:** Data quality, standardization, and business logic application

#### Common Transformation Patterns

1. **Data Cleansing**
   * Trim whitespace from string columns
   * Handle null values with default values
   * Standardize date formats
   * Apply data type conversions

2. **Surrogate Key Generation**
   * Add `_sk` suffix columns using `monotonically_increasing_id()`
   * Example: `company_sk`, `employee_sk`, `department_sk`

3. **Column Renaming**
   * Resolve naming conflicts (e.g., `is_active` → `company_is_active`)
   * Standardize naming conventions

4. **Data Enrichment**
   * Calculate derived fields
   * Add business logic computations

#### Individual Transformation Notebooks

##### nb_trn_company.ipynb
```python
# Key Transformations:
- Trim whitespace: company_name, industry, country
- Date conversion: established_date (to date type)
- Null handling: Replace nulls with 'Unknown' or 0
- Add surrogate key: company_sk
- Rename: is_active → company_is_active
```

##### nb_trn_departments.ipynb
```python
# Key Transformations:
- Standardize department codes
- Add department_sk surrogate key
- Cleanse department_name and cost_center
```

##### nb_trn_employee.ipynb
```python
# Key Transformations:
- Date conversions: hire_date, termination_date
- Calculate tenure_days
- Standardize email format
- Add employee_sk
- Rename: is_active → employee_is_active
```

##### nb_trn_accounts.ipynb
```python
# Key Transformations:
- Standardize account codes
- Categorize accounts (Revenue, COGS, Operating Expense, etc.)
- Add account_sk
- Cleanse account_name
```

##### nb_trn_general_ledgers.ipynb
```python
# Key Transformations:
- Date parsing: entry_date, posting_date (format: 'dd-MM-yyyy HH:mm')
- Add gl_sk surrogate key
- Rename: status → gl_status
- Preserve amounts without modification
```

##### nb_trn_payroll.ipynb
```python
# Key Transformations:
- Date conversions: pay_date, pay_period_start, pay_period_end
- Calculate total_compensation and total_deduction
- Add payroll_sk
- Standardize payment_method values
```

---

### 3.3 Gold Layer (Analytical Models)

**Purpose:** Dimensional modeling, data cubes, and business metrics

#### 3.3.1 Dimension Tables (`nb_gold_dims.ipynb`)

Dimension tables provide descriptive context for fact tables.

##### dim_date

**Generation Method:** Dynamic date range based on fact table min/max dates

```python
# Key Attributes:
- date_key (PK): Integer format YYYYMMDD
- date: Full date
- year, quarter, month
- month_name: Full month name (January, February, etc.)
- year_month: Format YYYY-MM
- day_of_month, day_of_week, week_of_year
```

**Date Range Logic:**
* Finds min/max dates from both general ledger and payroll
* Generates continuous date sequence
* Supports fiscal period analysis

##### dim_company

```python
# Attributes:
- company_key (PK): Surrogate key from silver layer
- company_id (Business Key)
- company_name
- industry
- country
- established_date
- company_is_active: Boolean flag
```

##### dim_department

```python
# Attributes:
- department_key (PK)
- department_id (Business Key)
- department_code
- department_name
- cost_center
```

##### dim_employee

```python
# Attributes:
- employee_key (PK)
- employee_id (Business Key)
- employee_code
- first_name, last_name
- email
- department_key (FK)
- company_key (FK)
- position
- hire_date, termination_date
- employee_is_active
- tenure_days
```

**Joins:** Pre-joined with department and company for denormalization

##### dim_account

```python
# Attributes:
- account_key (PK)
- account_id (Business Key)
- account_code
- account_name
- account_type: Asset, Liability, Equity, Revenue, Expense
- category: Revenue, COGS, Operating Expense, etc.
- account_is_active
```

#### 3.3.2 Fact Tables (`nb_gold_facts.ipynb`)

##### fact_general_ledger

**Grain:** One row per general ledger transaction

```python
# Foreign Keys:
- entry_date_key: Links to dim_date
- posting_date_key: Links to dim_date
- company_key: Links to dim_company
- department_key: Links to dim_department
- account_key: Links to dim_account

# Measures:
- debit_amount: Debit transactions
- credit_amount: Credit transactions

# Descriptive Attributes:
- gl_id: Business identifier
- transaction_type: Type of GL entry
- reference_number: External reference
- description: Transaction description
- fiscal_year, fiscal_period
- created_by, approved_by: Audit fields
- gl_status: Transaction status
- account_category: Denormalized from account dimension
```

##### fact_payroll

**Grain:** One row per employee per pay period

```python
# Foreign Keys:
- pay_date_key: Links to dim_date
- pay_period_start_date_key: Links to dim_date
- pay_period_end_date_key: Links to dim_date
- company_key: Links to dim_company
- department_key: Links to dim_department
- employee_key: Links to dim_employee

# Compensation Measures:
- bonus
- overtime_pay
- commission
- allowances
- total_compensation

# Deduction Measures:
- tax_deduction
- social_security
- health_insurance
- retirement_contribution
- other_deductions
- total_deduction

# Net Measure:
- net_salary: total_compensation - total_deduction

# Descriptive Attributes:
- payroll_id: Business identifier
- pay_date, pay_period_start, pay_period_end
- year_month: Pay period identifier
- payment_method: Check, Direct Deposit, etc.
- payroll_status: Paid, Pending, etc.
```

#### 3.3.3 Data Cubes (`nb_gold_data_cube.ipynb`)

Data cubes are denormalized analytical tables optimized for query performance.

##### gl_data_cube

**Purpose:** Flattened view of general ledger with all dimensional attributes

```python
# Structure: Fact + All Dimensions
- GL transaction identifiers and measures
- Entry date attributes (year, quarter, month, month_name, year_month)
- Posting date attributes (year, quarter, month, month_name, year_month)
- Company attributes (id, name, industry, country, active flag)
- Department attributes (id, code, name, cost center)
- Account attributes (id, code, name, type, category, active flag)
- Calculated: net_amount = debit_amount - credit_amount
```

**Query Optimization:** All common filter and group-by columns pre-joined

##### payroll_data_cube

**Purpose:** Flattened view of payroll with all dimensional attributes

```python
# Structure: Fact + All Dimensions
- Payroll identifiers and measures
- Pay date attributes (year, quarter, month, month_name, year_month)
- Pay period dates
- Company attributes
- Department attributes
- Employee attributes (id, code, full name, email, position, hire date, active, tenure)
- All compensation components
- All deduction components
- Net salary
```

**Special Feature:** `concat_ws()` combines first_name and last_name into `employee_name`

##### unified_data_cube

**Purpose:** Combined financial and payroll metrics for holistic analysis

**Aggregation Level:** Company + Department + Month + Account Category

```python
# Grain: One row per company, department, month, and account category combination

# Dimensional Attributes:
- company_id, company_name, industry, country
- department_id, department_name, cost_center
- year_month, year, quarter, month
- account_category (from GL only)

# GL Metrics:
- total_debit: Sum of debit amounts
- total_credit: Sum of credit amounts
- gl_net_amount: Sum of net amounts
- transaction_count: Count of GL transactions

# Payroll Metrics:
- total_compensation: Sum of all compensation
- total_deduction: Sum of all deductions
- total_net_salary: Sum of net salaries
- employee_count: Count of unique employees
- avg_net_salary: Average net salary

# Join Logic: FULL OUTER JOIN on company, department, and month
# Null Handling: COALESCE with 0 for missing values
```

**Use Cases:**
* Compare payroll costs against revenue by department
* Analyze labor cost as percentage of revenue
* Track headcount changes alongside financial performance
* Calculate fully-loaded departmental costs

---

## 4. Schema Definitions

### 4.1 Bronze Layer Tables

#### accounts_raw
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| account_id | Integer | Unique account identifier |
| account_code | String | Chart of accounts code |
| account_name | String | Account description |
| account_type | String | Asset, Liability, Revenue, Expense |
| category | String | Subcategory classification |
| is_active | Boolean | Account active status |

#### company_raw
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| company_id | Integer | Unique company identifier |
| company_name | String | Legal company name |
| industry | String | Industry classification |
| country | String | Country of incorporation |
| established_date | String | Company founding date |
| employee_count | Integer | Total employee count |
| revenue | Double | Annual revenue |
| is_active | Boolean | Company active status |

#### departments_raw
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| department_id | Integer | Unique department identifier |
| department_code | String | Department code |
| department_name | String | Department name |
| company_id | Integer | Parent company |
| cost_center | String | Cost center code |

#### employee_raw
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| employee_id | Integer | Unique employee identifier |
| employee_code | String | Employee code |
| first_name | String | First name |
| last_name | String | Last name |
| email | String | Email address |
| company_id | Integer | Employer company |
| department_id | Integer | Assigned department |
| position | String | Job title |
| hire_date | String | Employment start date |
| termination_date | String | Employment end date (nullable) |
| is_active | Boolean | Employment active status |

#### general_ledgers_raw
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| gl_id | Integer | Unique GL transaction identifier |
| company_id | Integer | Company identifier |
| account_id | Integer | Account identifier |
| department_id | Integer | Department identifier |
| entry_date | String | Transaction entry date |
| posting_date | String | Transaction posting date |
| fiscal_year | Integer | Fiscal year |
| fiscal_period | Integer | Fiscal period |
| transaction_type | String | Type of transaction |
| reference_number | String | External reference |
| description | String | Transaction description |
| debit_amount | Double | Debit amount |
| credit_amount | Double | Credit amount |
| created_by | String | Creator user |
| approved_by | String | Approver user |
| status | String | Transaction status |

#### payroll_raw
| Column Name | Data Type | Description |
|------------|-----------|-------------|
| payroll_id | Integer | Unique payroll transaction identifier |
| employee_id | Integer | Employee identifier |
| company_id | Integer | Company identifier |
| department_id | Integer | Department identifier |
| pay_date | String | Pay date |
| pay_period_start | String | Period start date |
| pay_period_end | String | Period end date |
| bonus | Double | Bonus amount |
| overtime_pay | Double | Overtime amount |
| commission | Double | Commission amount |
| allowances | Double | Allowances amount |
| tax_deduction | Double | Tax deduction |
| social_security | Double | Social security deduction |
| health_insurance | Double | Health insurance deduction |
| retirement_contribution | Double | Retirement deduction |
| other_deductions | Double | Other deductions |
| payment_method | String | Payment method |
| status | String | Payroll status |

### 4.2 Silver Layer Tables

Silver layer tables follow the same structure as bronze with these enhancements:

* Surrogate keys added (`_sk` suffix)
* Date columns converted to proper date types
* Null values handled
* Column names standardized (e.g., `is_active` → `{entity}_is_active`)
* Calculated columns added (e.g., `tenure_days`, `total_compensation`, `total_deduction`, `net_salary`)

### 4.3 Gold Layer Tables

Refer to Section 3.3 for detailed gold layer schemas.

---

## 5. Data Flow and Transformations

### 5.1 End-to-End Data Flow

```
S3 CSV Files
    │
    ├─► Bronze: Raw ingestion (no transformation)
    │
    ├─► Silver: Cleansing & Standardization
    │      │
    │      ├─► Data type conversion
    │      ├─► Null handling
    │      ├─► Surrogate key generation
    │      ├─► Column renaming
    │      └─► Business logic application
    │
    └─► Gold: Dimensional Modeling
           │
           ├─► Dimensions: Date, Company, Department, Employee, Account
           │
           ├─► Facts: General Ledger, Payroll
           │
           └─► Data Cubes: GL Cube, Payroll Cube, Unified Cube
                  │
                  └─► BI & Analytics: KPIs, Dashboards, Reports
```

### 5.2 Dependency Chain

**Level 0:** Bronze raw tables (independent)

**Level 1:** Silver transformed tables (depend on bronze)
* transformed_accounts ← accounts_raw
* transformed_company ← company_raw
* transformed_departments ← departments_raw
* transformed_employee ← employee_raw
* transformed_general_ledgers ← general_ledgers_raw
* transformed_payroll ← payroll_raw

**Level 2:** Gold dimension tables (depend on silver)
* dim_date ← transformed_general_ledgers + transformed_payroll
* dim_company ← transformed_company
* dim_department ← transformed_departments
* dim_employee ← transformed_employee + transformed_departments + transformed_company
* dim_account ← transformed_accounts

**Level 3:** Gold fact tables (depend on silver + gold dimensions)
* fact_general_ledger ← transformed_general_ledgers + all dimensions
* fact_payroll ← transformed_payroll + all dimensions

**Level 4:** Gold data cubes (depend on facts + dimensions)
* gl_data_cube ← fact_general_ledger + all dimensions
* payroll_data_cube ← fact_payroll + all dimensions
* unified_data_cube ← gl_data_cube + payroll_data_cube (aggregated)

**Level 5:** KPIs and analytics (depend on data cubes)

### 5.3 Join Strategies

**Dimension Enrichment Joins:**
* Type: LEFT JOIN
* Purpose: Preserve all fact records even if dimension lookup fails
* Example: `fact_general_ledger LEFT JOIN dim_company ON company_key`

**Data Cube Full Outer Join:**
* Type: FULL OUTER JOIN
* Purpose: Capture all combinations of company, department, and month from both GL and payroll
* Null Handling: `COALESCE()` with dimension attributes and `lit(0)` for measures

---

## 6. Key Metrics and KPIs

**Notebook:** `nb_gold_kpis.ipynb`

### 6.1 Financial KPIs

#### Monthly Consolidated Revenue
```sql
Metric: Sum of credit amounts for Revenue category accounts
Grain: Month
Calculation: SUM(credit_amount) WHERE account_category = 'Revenue'
Growth: Month-over-month percentage change using LAG() window function
```

#### Cost of Sales by Month
```sql
Metric: Net amount for Cost of Sales category
Grain: Company, Month
Calculation: SUM(net_amount) WHERE account_category = 'Cost of Sales'
```

#### Monthly Gross Profit Margin
```sql
Metrics:
- Revenue: SUM(net_amount) WHERE account_category = 'Revenue'
- COGS: SUM(net_amount) WHERE account_category = 'Cost of Sales'
- Gross Profit: Revenue - COGS
- Gross Margin %: (Gross Profit / Revenue) * 100
Grain: Month
```

#### Operating Expense Breakdown
```sql
Metric: Total expenses by company, department, and account
Grain: Company, Department, Account
Calculation: SUM(net_amount) WHERE account_category = 'Operating Expense'
Sorting: Descending by total expense
```

#### Net Profit by Month
```sql
Metrics:
- Revenue: SUM(net_amount) WHERE account_category = 'Revenue'
- COGS: SUM(net_amount) WHERE account_category = 'Cost of Sales'
- OpEx: SUM(net_amount) WHERE account_category = 'Operating Expense'
- Net Profit: Revenue - COGS - OpEx
Grain: Month
```

### 6.2 Payroll KPIs

#### Average Compensation by Position
```sql
Metrics:
- Average total compensation
- Average bonus
- Average overtime pay
- Average commission
- Employee count
Grain: Position, Company
Grouping: Position and company
```

#### Overtime and Bonus Analysis
```sql
Metrics:
- Total overtime
- Total bonus
- Total commission
- Overtime-to-base ratio
- Employee count
Grain: Department
Calculation: Total overtime / (Total comp - overtime - bonus - commission) * 100
```

#### Payroll Cost as % of Company Revenue
```sql
Metrics:
- Monthly revenue (from GL cube)
- Total payroll cost (from payroll cube)
- Payroll as % of revenue
Grain: Month, Company
Join: Inner join GL and payroll cubes on month and company
```

#### Cost per Department
```sql
Metrics:
- Total payroll cost
- Employee count
- Average cost per employee
Grain: Department, Company
Sorting: Descending by total cost
```

#### Headcount Distribution by Department
```sql
Metric: Count of active employees
Grain: Department, Month, Company
Filter: WHERE employee_is_active = TRUE
```

### 6.3 Unified KPIs (Cross-Domain)

#### Labor Cost Efficiency
```sql
Metric: Payroll cost / Revenue
Source: unified_data_cube
Insight: Measure labor cost as percentage of revenue generation
```

#### Department Profitability
```sql
Metrics:
- Department revenue (from GL)
- Department payroll cost (from payroll)
- Net contribution: Revenue - Payroll
Source: unified_data_cube
Grain: Department, Month
```

---

## 7. Technology Stack

### 7.1 Platform

* **Cloud Provider:** AWS
* **Data Platform:** Databricks
* **Compute:** Serverless Interactive Cluster
* **Storage:** Delta Lake on S3
* **Catalog:** Unity Catalog

### 7.2 Languages and Frameworks

* **Python:** Data transformation and orchestration
* **PySpark:** Distributed data processing
* **SQL:** Analytical queries and KPIs
* **Delta Lake:** ACID transactions, time travel, schema evolution

### 7.3 Key Libraries

```python
from pyspark.sql.functions import (
    col, lit, coalesce, when, concat_ws,
    trim, to_date, upper, date_format,
    year, month, quarter, dayofmonth, dayofweek, weekofyear,
    sum as _sum, count, avg,
    sequence, explode,
    monotonically_increasing_id
)
```

### 7.4 Data Formats

* **Bronze:** Delta (from CSV)
* **Silver:** Delta
* **Gold:** Delta
* **Storage Format:** Parquet (underlying Delta)
* **Compression:** Snappy (default)

---

## 8. Data Lineage

### 8.1 Lineage Diagram

```
Source CSV Files (S3)
│
├─► accounts.csv
│      └─► accounts_raw
│            └─► transformed_accounts
│                  └─► dim_account
│                        ├─► fact_general_ledger
│                        │     └─► gl_data_cube
│                        │           └─► unified_data_cube
│                        │                 └─► KPIs
│
├─► company.csv
│      └─► company_raw
│            └─► transformed_company
│                  ├─► dim_company
│                  │     ├─► fact_general_ledger
│                  │     │     └─► gl_data_cube
│                  │     └─► fact_payroll
│                  │           └─► payroll_data_cube
│                  │                 └─► unified_data_cube
│                  │                       └─► KPIs
│                  └─► dim_employee (joined)
│
├─► departments.csv
│      └─► departments_raw
│            └─► transformed_departments
│                  ├─► dim_department
│                  │     ├─► fact_general_ledger
│                  │     └─► fact_payroll
│                  └─► dim_employee (joined)
│
├─► employee.csv
│      └─► employee_raw
│            └─► transformed_employee
│                  └─► dim_employee
│                        └─► fact_payroll
│                              └─► payroll_data_cube
│                                    └─► unified_data_cube
│                                          └─► KPIs
│
├─► general ledgers.csv
│      └─► general_ledgers_raw
│            ├─► transformed_general_ledgers
│            │     └─► fact_general_ledger
│            │           └─► gl_data_cube
│            │                 └─► unified_data_cube
│            │                       └─► KPIs
│            └─► dim_date (date range calculation)
│
└─► payroll.csv
       └─► payroll_raw
             ├─► transformed_payroll
             │     └─► fact_payroll
             │           └─► payroll_data_cube
             │                 └─► unified_data_cube
             │                       └─► KPIs
             └─► dim_date (date range calculation)
```

### 8.2 Critical Path

For KPI generation, the critical path is:

1. Bronze ingestion (all 6 tables in parallel)
2. Silver transformation (6 transformations in parallel)
3. Gold dimensions (dim_date requires GL and payroll; dim_employee requires company and department)
4. Gold facts (require all dimensions)
5. Gold data cubes (require facts and dimensions)
6. KPIs (require data cubes)

**Estimated Processing Time (typical load):**
* Bronze: 2-5 minutes
* Silver: 3-7 minutes
* Gold Dimensions: 2-4 minutes
* Gold Facts: 3-6 minutes
* Gold Data Cubes: 2-5 minutes
* **Total: ~15-30 minutes**

---

## 9. Deployment and Operations

### 9.1 Execution Order

Notebooks must be executed in this order:

1. **Bronze Layer**
   * `01_bronze/nb_01_bronze.ipynb`

2. **Silver Layer** (can run in parallel after bronze completes)
   * `02_silver/nb_trn_accounts.ipynb`
   * `02_silver/nb_trn_company.ipynb`
   * `02_silver/nb_trn_departments.ipynb`
   * `02_silver/nb_trn_employee.ipynb`
   * `02_silver/nb_trn_general_ledgers.ipynb`
   * `02_silver/nb_trn_payroll.ipynb`

3. **Gold Dimensions**
   * `03_gold/nb_gold_dims.ipynb`

4. **Gold Facts**
   * `03_gold/nb_gold_facts.ipynb`

5. **Gold Data Cubes**
   * `03_gold/nb_gold_data_cube.ipynb`

6. **KPIs**
   * `03_gold/nb_gold_kpis.ipynb`

### 9.2 Orchestration Recommendations

**Option 1: Databricks Workflows**
* Create a multi-task job
* Configure dependencies between tasks
* Schedule with cron expression
* Enable email notifications on success/failure

**Option 2: Databricks Delta Live Tables (DLT)**
* Define tables as transformations
* Automatic dependency resolution
* Built-in data quality checks
* Lineage visualization

### 9.3 Refresh Strategy

**Current Implementation:** Full refresh (overwrite mode)

**Recommended Enhancement:** Incremental loading
* Bronze: Append new files with timestamps
* Silver: Use MERGE for upserts
* Gold Facts: Incremental append with deduplication
* Gold Cubes: Rebuild only affected partitions

### 9.4 Monitoring and Alerts

**Key Metrics to Monitor:**
* Row counts by table
* Data freshness (last update timestamp)
* Schema drift detection
* Null value percentages
* Duplicate key detection
* Processing duration
* Failure rates

**Recommended Tools:**
* Databricks SQL Alerts
* Job execution metrics
* Delta Lake table history
* Unity Catalog data lineage

---

## 10. Best Practices and Conventions

### 10.1 Naming Conventions

**Tables:**
* Bronze: `{entity}_raw` (e.g., `company_raw`)
* Silver: `transformed_{entity}` (e.g., `transformed_company`)
* Gold Dimensions: `dim_{entity}` (e.g., `dim_company`)
* Gold Facts: `fact_{entity}` (e.g., `fact_general_ledger`)
* Gold Cubes: `{entity}_data_cube` (e.g., `gl_data_cube`)

**Columns:**
* Primary Keys: `{entity}_key` (e.g., `company_key`)
* Foreign Keys: `{entity}_key` (e.g., `company_key` in fact table)
* Surrogate Keys: `{entity}_sk` (e.g., `company_sk` in silver)
* Business Keys: `{entity}_id` (e.g., `company_id`)
* Boolean Flags: `{entity}_is_{condition}` (e.g., `company_is_active`)
* Dates: `{context}_date` (e.g., `entry_date`, `posting_date`)

**Notebooks:**
* Bronze: `nb_01_bronze.ipynb`
* Silver: `nb_trn_{entity}.ipynb` (e.g., `nb_trn_company.ipynb`)
* Gold: `nb_gold_{type}.ipynb` (e.g., `nb_gold_dims.ipynb`, `nb_gold_facts.ipynb`)

### 10.2 Code Patterns

**Surrogate Key Generation:**
```python
.withColumn("{entity}_sk", monotonically_increasing_id())
```

**Date Parsing:**
```python
.withColumn("{date_field}", to_date(col("{date_field}"), "{format}"))
```

**Null Handling:**
```python
.withColumn("{column}", when(col("{column}").isNull(), "{default}").otherwise(col("{column}")))
```

**Left Joins (Dimension Enrichment):**
```python
.join(dim_table.alias("d"), col("f.{key}") == col("d.{key}"), "left")
```

**Column Selection:**
```python
.select(
    col("alias.column").alias("new_name"),
    ...
)
```

### 10.3 Data Quality Checks

**Recommended Validations:**
* Primary key uniqueness
* Foreign key referential integrity
* Null checks on required fields
* Date range validity
* Amount sign consistency (debits positive, credits positive)
* Balance checks (debits = credits for closed periods)
* Employee payroll reconciliation (employee count consistency)

### 10.4 Performance Optimization

**Current Optimizations:**
* Delta Lake automatic file compaction
* Column pruning in SELECT statements
* Predicate pushdown with WHERE clauses
* Broadcast joins for small dimension tables

**Recommended Enhancements:**
* Partition fact tables by year_month
* Z-order dimensions by commonly filtered columns
* Liquid clustering for data cubes
* Cache frequently accessed dimensions
* Use Delta caching for interactive queries

### 10.5 Schema Evolution

**Delta Lake Schema Evolution:**
```python
.option("mergeSchema", "true")  # Allow new columns
.option("overwriteSchema", "true")  # Replace schema completely
```

**Recommendations:**
* Use additive schema changes when possible
* Version control schema definitions
* Test schema changes in development environment
* Document breaking changes

### 10.6 Documentation Standards

**Cell Titles:**
* Use descriptive titles for all notebook cells
* Examples: "Load Dimension Tables", "Calculate Net Profit", "Save Data Cubes"

**Comments:**
* Explain complex business logic
* Document assumptions
* Reference source system field mappings
* Note known data quality issues

**Metadata:**
* Table comments in Unity Catalog
* Column descriptions
* Tag PII and sensitive data
* Document refresh frequency

---

## Appendix A: Table Inventory

### Bronze Layer (01_global_tech_bronze.raw_tables)

| Table Name | Row Count (Approx) | Source File |
|-----------|-------------------|-------------|
| accounts_raw | 100-500 | accounts.csv |
| company_raw | 2-10 | company.csv |
| departments_raw | 10-50 | departments.csv |
| employee_raw | 500-5,000 | employee.csv |
| general_ledgers_raw | 10,000-100,000 | general ledgers.csv |
| payroll_raw | 5,000-50,000 | payroll.csv |

### Silver Layer (02_global_tech_silver.transformed_tables)

| Table Name | Transforms Applied |
|-----------|-------------------|
| transformed_accounts | Cleansing, surrogate key, category enrichment |
| transformed_company | Date conversion, null handling, surrogate key |
| transformed_departments | Cleansing, surrogate key |
| transformed_employee | Date conversion, tenure calculation, surrogate key |
| transformed_general_ledgers | Date parsing, surrogate key, status rename |
| transformed_payroll | Date conversion, total calculations, surrogate key |

### Gold Layer (03_global_tech_gold)

#### dims_tables Schema

| Table Name | Type | Row Count (Approx) |
|-----------|------|-------------------|
| dim_date | Dimension | 1,000-3,000 (date range) |
| dim_company | Dimension | 2-10 |
| dim_department | Dimension | 10-50 |
| dim_employee | Dimension | 500-5,000 |
| dim_account | Dimension | 100-500 |

#### facts_tables Schema

| Table Name | Type | Row Count (Approx) |
|-----------|------|-------------------|
| fact_general_ledger | Fact | 10,000-100,000 |
| fact_payroll | Fact | 5,000-50,000 |

#### data_cube Schema

| Table Name | Type | Row Count (Approx) |
|-----------|------|-------------------|
| gl_data_cube | Data Cube | 10,000-100,000 |
| payroll_data_cube | Data Cube | 5,000-50,000 |
| unified_data_cube | Data Cube (Aggregated) | 100-5,000 |

---

## Appendix B: Sample Queries

### Query 1: Top Revenue-Generating Accounts
```sql
SELECT 
    account_name,
    account_category,
    SUM(credit_amount) AS total_revenue
FROM 03_global_tech_gold.data_cube.gl_data_cube
WHERE account_category = 'Revenue'
  AND posting_year = 2025
GROUP BY account_name, account_category
ORDER BY total_revenue DESC
LIMIT 10;
```

### Query 2: Department Headcount Trend
```sql
SELECT 
    pay_year_month,
    department_name,
    COUNT(DISTINCT employee_id) AS headcount
FROM 03_global_tech_gold.data_cube.payroll_data_cube
WHERE employee_is_active = TRUE
GROUP BY pay_year_month, department_name
ORDER BY pay_year_month, department_name;
```

### Query 3: Company Profitability Summary
```sql
WITH revenue AS (
    SELECT 
        company_name,
        posting_year,
        SUM(net_amount) AS total_revenue
    FROM 03_global_tech_gold.data_cube.gl_data_cube
    WHERE account_category = 'Revenue'
    GROUP BY company_name, posting_year
),
expenses AS (
    SELECT 
        company_name,
        posting_year,
        SUM(net_amount) AS total_expenses
    FROM 03_global_tech_gold.data_cube.gl_data_cube
    WHERE account_category IN ('Cost of Sales', 'Operating Expense')
    GROUP BY company_name, posting_year
)
SELECT 
    r.company_name,
    r.posting_year,
    r.total_revenue,
    e.total_expenses,
    r.total_revenue - e.total_expenses AS net_profit,
    ROUND((r.total_revenue - e.total_expenses) / r.total_revenue * 100, 2) AS profit_margin_pct
FROM revenue r
JOIN expenses e ON r.company_name = e.company_name AND r.posting_year = e.posting_year
ORDER BY r.posting_year, r.company_name;
```

### Query 4: Labor Cost Analysis
```sql
SELECT 
    year_month,
    company_name,
    department_name,
    SUM(total_net_salary) AS dept_payroll_cost,
    SUM(CASE WHEN account_category = 'Revenue' THEN gl_net_amount ELSE 0 END) AS dept_revenue,
    ROUND(
        SUM(total_net_salary) / NULLIF(SUM(CASE WHEN account_category = 'Revenue' THEN gl_net_amount ELSE 0 END), 0) * 100,
        2
    ) AS labor_cost_pct_of_revenue
FROM 03_global_tech_gold.data_cube.unified_data_cube
WHERE year = 2025
GROUP BY year_month, company_name, department_name
ORDER BY year_month, company_name, labor_cost_pct_of_revenue DESC;
```

---

## Appendix C: Known Issues and Limitations

### Current Limitations

1. **Full Refresh Only**: All tables use overwrite mode, no incremental processing
2. **No SCD Type 2**: Historical changes to dimensions are not tracked
3. **Limited Data Quality Checks**: No automated validation framework
4. **Manual Orchestration**: Notebooks must be run manually in sequence
5. **No Partitioning**: Large fact tables not partitioned for optimal performance
6. **Date Range Dependency**: dim_date depends on existing fact data

### Known Issues

1. **Silver Company Transformation**: Cell 4 in `nb_trn_company.ipynb` references undefined `cleaned_company_df` variable (should be `company_df`)
2. **Date Format Inconsistency**: Some source dates use "dd-MM-yyyy HH:mm" while others may vary
3. **Null Handling**: Some transformations replace nulls with 'Unknown' string instead of proper NULL handling

### Planned Enhancements

1. **Incremental Loading**: Implement change data capture (CDC) for large tables
2. **Data Quality Framework**: Add Great Expectations or Databricks Data Quality checks
3. **Orchestration**: Migrate to Databricks Workflows or Delta Live Tables
4. **Partitioning Strategy**: Partition facts by year_month
5. **Historical Tracking**: Implement SCD Type 2 for critical dimensions
6. **Performance Tuning**: Add Z-ordering and optimize file sizes
7. **Error Handling**: Add try-catch blocks and logging
8. **Testing Framework**: Unit tests for transformations

---

## Appendix D: Contact and Support

**Data Engineering Team**
* Email: data-engineering@globaltech.com
* Slack: #data-engineering
* Wiki: https://wiki.globaltech.com/data-platform

**Change Management**
* Submit change requests via Jira: DATA project
* Documentation updates: Git repository `data-platform-docs`

**Incident Response**
* P1 (Critical): Page on-call engineer
* P2 (High): Create Jira ticket and notify in Slack
* P3 (Medium/Low): Create Jira ticket

---

## Document Revision History

| Version | Date | Author | Changes |
|---------|------|--------|----------|
| 1.0 | 2026-03-27 | Data Engineering Team | Initial comprehensive documentation |

---

**End of Document**