# Third-party library imports
import pandas as pd
import plotly.express as px

def plot_countries_per_year_away_from_draft(df_career_paths: pd.DataFrame):
    """
    Plot the number of players playing in each country for each year away from the draft.

    :param df_career_paths: DataFrame containing career paths with country information
    :type df_career_paths: pd.DataFrame
    :return: Plotly geographic scatter plot showing the distribution of players playing in a country
    :rtype: plotly.graph_objects.Figure
    """

    # Remove duplicates where players have played in the same country in the same year
    df_career_paths = df_career_paths.drop_duplicates(
        ['Proballers_ID', 'Years_From_Draft', 'Country_Alpha3']
    )

    # Get min and max draft years for indicating the range of years in the plot
    min_draft_year = df_career_paths['Draft_Year'].min()
    max_draft_year = df_career_paths['Draft_Year'].max()

    # Count the number of players playing in each country for each year away from the draft
    df_countries_per_year_from_draft = df_career_paths.groupby(
        ['Years_From_Draft', 'Country_Alpha3', 'Country_Name']
    ).size().reset_index(name='Count')

    fig = px.scatter_geo(
        data_frame=df_countries_per_year_from_draft,
        locations='Country_Alpha3',
        hover_name='Country_Name',
        hover_data={
            'Country_Alpha3': False,
            'Country_Name': False,
            'Years_From_Draft': True,
            'Count': True
        },
        size='Count',
        animation_frame='Years_From_Draft',
        animation_group='Years_From_Draft',
        labels={
            'Country_Name': 'Country',
            'Years_From_Draft': 'Years Away from Draft',
            'Count': 'Number of Players'
        },
        projection='natural earth',
        title='Where do drafted players play throughout their careers?',
        subtitle=f'Data only includes players drafted from {min_draft_year} to {max_draft_year}.',
        template='plotly_dark',
        width=1000,
        height=600
    )

    return fig
