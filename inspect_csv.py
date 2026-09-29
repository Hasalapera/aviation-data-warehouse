import pandas as pd

# Path එක data folder එක ඇතුළේ තියෙන flight.csv එකට දෙන්න
df = pd.read_csv("data/flight.csv")

print("Columns List:")
print(df.columns.tolist())

print("\nTotal Rows:", len(df))
print("\nFirst 3 Rows:")
print(df.head(3))