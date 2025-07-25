import streamlit as st

from dataset_creation import create_dataframe_of_career_paths
from visualisations import plot_countries_per_year_away_from_draft

df_career_paths = create_dataframe_of_career_paths()
st.plotly_chart(plot_countries_per_year_away_from_draft(df_career_paths), use_container_width=True)
