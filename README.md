# mlb-game-data
mlb-game-data

Requirements
==============
1. A PostgreSQL client and connectivity to the PostgreSQL instance at hcubs.crtnht6h1zib.us-east-1.rds.amazonaws.com:5432, db name uqumrsnb
2. The ddl.sql file needs to be run in the target database
3. Python 3
4. Python needs the following to be installed via pip: requests, psycopg2
5. The main.py file included in this repo
6. Syntax works as follows: 'python3 main.py 2021-06-10'
7. All games for that day and all dependent records (for venue, team, and player) will be loaded on each run, but duplication is prevented in the SQL code. Running an individual day's games multiple times would only serve to refresh the data if any changes have been published.
