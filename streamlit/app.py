import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# Database connection setup
DATABASE_URL = "postgresql://airflow:airflow@postgres/airflow"
engine = create_engine(DATABASE_URL)

# Fetch facts from the database
def fetch_facts():
    query = "SELECT fact, category FROM facts"
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df

# Streamlit app
def main():
    st.title("K9 Facts Viewer")
    st.write("Displaying facts categorized as 'with_numbers' and 'without_numbers'.")

    # Fetch facts from the database
    df = fetch_facts()

    # Display facts under each category
    categories = df['category'].unique()
    for category in categories:
        st.header(f"Category: {category}")
        category_facts = df[df['category'] == category]['fact'].tolist()
        for fact in category_facts:
            st.write(f"- {fact}")

if __name__ == "__main__":
    main()
