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