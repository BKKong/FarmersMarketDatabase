# Farmers Market Database

In this project, we provide a curated database of farmers markets in the
U.S. and an RPC service for accessing the database.

## Database

Data in the database are sourced from the [U.S. Department of Agriculture
National Farmers Market Directory](
https://www.ams.usda.gov/local-food-directories/farmersmarkets). The data
were imported from the original CSV format into an on-disk SQLite database
found in the `db` directory. Data cleaning was conducted using SQLite and
 OpenRefine. The OpenRefine operation history can be found in a JSON file
 located in the `etl` directory.

## RPC service

The RPC service is implemented in Java and supports create, read, update and
delete (CRUD) operations on farmers market records in the SQLite database.
Its server- and client-side interfaces are implemented using gRPC and
protocol buffers. It connects to SQLite using JDBC. Unit testing is based on
JUnit. The code is located in the `rpcservice` directory.
