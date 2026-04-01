import pandas as pd

df = pd.read_csv("data/owid-energy-data.csv")

print("COLUMNS:")
print(df.columns.tolist())

print("\nSHAPE:")
print(df.shape)

print("\nSAMPLE:")
print(df.head())

print("\nNULL % per column:")
print((df.isnull().mean() * 100).sort_values(ascending=False).head(15))