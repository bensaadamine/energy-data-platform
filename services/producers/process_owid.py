import pandas as pd

df = pd.read_csv("data/owid-energy-data.csv")

df = df[df["iso_code"].notna() | df["oil_consumption"].notna() | df["gas_consumption"].notna()]