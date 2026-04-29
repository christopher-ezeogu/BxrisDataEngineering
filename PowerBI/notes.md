# steps to building a Power BI Dashboard
- connect to data source
- load data
- Build relational model
- Add DAX Measures - Define the calculated fields
- Visual Data

# Step 1
go to "Home" tab - "Get data" - to see all "common data sources" connectors and choose the one you intend to connect to example - CSV, SQLServer

select connector -  point to file - click "Transform Data" tab to QA your data before loading it into power BI using "Close & Apply"

ex -  
- airline.csv 
I intend to use the first row as header -- so I click on "Use First Row as Header" on the top right corner -- then "Close & Apply" to save changes

- airports.csv
Transformed data and no changes needed so closed and apply to save it.

- cancellations.csv
-- Transformed Data and promoted first row to header

- flights.csv
from the list i choose the following columns - YEAR, MONTH, DAY_OF_WEEK, AIRLINE, ORIGIN_AIRPORT, DEPARTURE_DELAY, CANCELLED, CANCELLATION_REASON .. I will be added a new column called "status"
-Hightlighted the columns I intend to keep, then "Remove other colummns"
-Added "Column - Conditional Column"
    - New column name - Status
    - if Column Name "CANCELLED" Operator "equals" Value  = 1 Output "Cancelled"
    - Else If Column Name "DEPARTURE_DELAY" Operator "is greater than" Value  = 0 Output "Delayed"
  Else
    On-time



# Step 2 - Build Relational Model 
-Click on the "Model view" tab on the left dialogue menu to connect the table models by their relational keys. 
-Drag keys to the tables based on relationship, verify and apply. Then save 

# Step 3 - Add DAX Measures - Define the calculated fields
Adding - right click -  add measure 

Data - on "flights" table - right click and add measure - 

Total Flights = COUNTROWS(flights)

Canceled Flights = CALCULATE([Total flights], flights[STATUS]="Canceled")

Delayed Flights = CALCULATE([Total flights], flights[STATUS]="Delayed")

On-Time Flights = CALCULATE([Total flights], flights[STATUS]="On-Time")

% Canceled = DIVIDE([Canceled Flights], [Total Flights], "-")

% Delayed = DIVIDE([Delayed Flights], [Total Flights], "-")

% On-Time = DIVIDE([On-Time Flights], [Total Flights], "-")

# Visualize the Data
- go to the "Report view" tab to visualize your data.
- To add text box, - go to Insert tab at the top menu, select shapes Text box, shapes, image, buttons
Card - good to display total 

line chart -- good to display the performance of two attributes on a x and y

Tooltip - to add the hover feature on a line chart


# Visual Interactions -  Allows for cross filtering between visuals

on the "Format" tab above -- edit interactions - 



# ########################################################################################################## #



# CLAIMS DATA PROCESSING
- load -+- transform data --->  build schema relationship --->  add dax measures ---> build schema ---> visualize data --> visualize interactions

-- "Get data" - to retrieve file -->>>
-- claims.csv - transform data -- fields formated and duplicates reomved before loading
-- providers.csv - transform data -- used first row as column headers
-- procedures.csv - transform data -- used first row as column headers
-- patients.csv - transform data -- column header looks goood -- "Close & Apply" to save.

# Power Query
    -   Convert service_date → Date
    -   Ensure claim_amount → Decimal
    -   Remove duplicates
    Filter out:
        -  Null claim_amount - to do (on the column - filter tab -  remove empty to remove nulls & blanks)
        -  Negative claim_amount - Table.SelectRows(#"Filtered Rows", each[claim_amount] >= 0)

Add column:
YearMonth = Date.ToText([service_date], "yyyy-MM") -- do this when you have already converted the text to Date else - YearMonth = FORMAT([service_date], "YYYY-MM")

-- add column -- list.NonNullcount(Records.ToList(_)) -- this will give you count of records with nulls (0 ) - removes rows where all values are nulls 

# Step 3: Build star schema
- Relationships:
  - claims.patient_id → patients.patient_id
  - claims.provider_id → providers.provider_id
  - claims.procedure_code → procedures.procedure_code
Important:
Single direction filtering
Claims = fact table (center)

# Step 4: Create DAX measures
- Total Cost: 
    Total Cost = SUM(claims[claim_amount])
- Cost per Patient: 
    Cost per Patient = DIVIDE([Total Cost], DISTINCTCOUNT(claims[patient_id]))
- Total Claims: 
    Total Claims = COUNT(claims[claim_id])
- Top Providers (just use Total Cost measure in visual)


# Visualize Data
- Build the dashboard
- KPI Cards
    Total Cost
    Total Claims
    Cost per Patient

- Trend Chart
    X-axis: service_date (Month)
    Y-axis: Total Cost

- Breakdown Chart
    Bar chart:
        Axis: provider_name
        Value: Total Cost

- Optional slicers (adds points)
    State
    Specialty
    Procedure Category

# To refresh data from SQL Server
- records when connected to has to be done using the "Direct Query" option instead of "Import"


