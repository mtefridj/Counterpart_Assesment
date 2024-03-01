import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('mock.db')

# Read the CSVsf file
policies = pd.read_csv('policies.csv')
quotes = pd.read_csv('quotes.csv')
submissions = pd.read_csv('submissions_(2).csv')

# Write the data to a new table in the SQLite database
policies.to_sql('policies', conn, if_exists='replace', index=False)

quotes.to_sql('quotes', conn, if_exists='replace', index=False)

submissions.to_sql('submissions', conn, if_exists='replace', index=False)

# Close the connection
conn.close()