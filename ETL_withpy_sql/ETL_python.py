import pandas as pd
import sqlite3

# Extract session
def extract(file_path):
    df = pd.read_csv(file_path)
    print("Data Extracted:")
    print(df.head())
    return df

#Transform session
def transform(df):
    # Handle missing values that are these in the ETL_pysql_set.csv
    if 'quantity' in df.columns:
        df['quantity'] = df['quantity'].fillna(0).astype(int)
    if 'price' in df.columns:
        df['price'] = df['price'].fillna(0).astype(float)

    # Calculate revenue if both columns exist
    if 'quantity' in df.columns and 'price' in df.columns:
        df['revenue'] = df['quantity'] * df['price']

    # Standardize date format if column exists
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    print("\nData Transformed:")
    print(df.head())
    return df

#Load session
def load(df, db_name="etl_db.sqlite", table_name="etl_table"):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"\nData Loaded into {db_name} (table: {table_name})")

#Run ETL session
if __name__ == "__main__":
    file_path = "ETL_pysql_set.csv"   #File name here...
    
    raw_data = extract(file_path)
    transformed_data = transform(raw_data)
    load(transformed_data)
