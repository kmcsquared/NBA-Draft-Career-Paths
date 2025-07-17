"""
This script builds and updates the NBA draft career paths dataset.

It fetches draft history data from the NBA API and enriches it with the players' career paths
scraped from proballers.com.
"""

# Standard library imports
from datetime import datetime
from io import StringIO
import json
import os

# Third-part library imports
from bs4 import BeautifulSoup
from nba_api.stats.endpoints.drafthistory import DraftHistory
from pycountry import countries as pycountry_countries
import pandas as pd
import requests

URL_PROBALLERS = 'https://www.proballers.com/basketball/player/'

def download_draft_history():
    """
    Download the NBA draft history data, create a null column for Proballers ID,
    and save the data as a JSON file.

    :return: None
    """

    # Fetch the draft history data
    df_draft_history = DraftHistory().get_data_frames()[0]
    df_draft_history['SEASON'] = df_draft_history['SEASON'].astype(int)

    # Create new columns for Proballers ID
    df_draft_history['PROBALLERS_ID'] = pd.NA
    df_draft_history = df_draft_history[['PLAYER_NAME', 'PROBALLERS_ID', 'SEASON', 'OVERALL_PICK']]

    df_draft_history.to_json(
        'data/nba_draft_history.json', orient='records', indent=4
    )

def add_missing_players_to_draft_history():
    """
    Load the existing draft history from a JSON file, check for draft players missing in the file,
    and append them to the dataset. The new players will have a null value for Proballers ID, which
    should be manually updated later. Save the updated draft history back to the JSON file.

    :return: None
    """

    df_draft_history_from_file = pd.read_json('data/nba_draft_history.json')

    # Int64 allows the column to contain both integers and null values (pd.NA)
    df_draft_history_from_file['PROBALLERS_ID'] = (
        df_draft_history_from_file['PROBALLERS_ID'].astype('Int64')
    )

    # Fetch the draft history data
    df_draft_history_from_nba_api = DraftHistory().get_data_frames()[0]

    # Get missing draft players to add to the dataset
    is_new_draft_player = (
        ~(df_draft_history_from_nba_api['SEASON'].isin(df_draft_history_from_file['SEASON']))
        &
        ~(df_draft_history_from_nba_api['OVERALL_PICK'].isin(
            df_draft_history_from_file['OVERALL_PICK'])
        )
    )

    df_new_draft = df_draft_history_from_nba_api[is_new_draft_player].copy()
    df_new_draft['SEASON'] = df_new_draft['SEASON'].astype(int)

    # Create new columns for Proballers ID
    df_new_draft['PROBALLERS_ID'] = pd.NA
    df_new_draft = df_new_draft[['PLAYER_NAME', 'PROBALLERS_ID', 'SEASON', 'OVERALL_PICK']]

    # Append new draft players to the existing dataset
    df_draft_history = pd.concat([df_draft_history_from_file, df_new_draft])
    df_draft_history = df_draft_history.sort_values(
        ['SEASON', 'OVERALL_PICK'], ascending=[False, True]
    )

    df_draft_history = df_draft_history.reset_index(drop=True)

    df_draft_history.to_json(
        'data/nba_draft_history.json', orient='records', indent=4
    )

def scrape_career_paths(draft_years_to_update: list[int]):
    """
    Scrape career paths for players with PROBALLERS_IDs from Proballers and save them as JSON files.

    :param draft_years_to_update: List of draft years for which to fetch career paths
    :type draft_years_to_update: list[int]
    :return: None
    """

    df_draft_history = pd.read_json('data/nba_draft_history.json')

    for draft_year in draft_years_to_update:
        if draft_year not in df_draft_history['SEASON'].unique():
            print(f'No data for draft year {draft_year}, update failed.')
            return

        # Filter the dataset to get players with PROBALLERS_ID from the specified draft years
        df_career_paths_to_update = df_draft_history.loc[
            (df_draft_history['PROBALLERS_ID'].notna())
            & (df_draft_history['SEASON'] == draft_year)
        ].copy()

        df_career_paths_to_update['PROBALLERS_ID'] = (
            df_career_paths_to_update['PROBALLERS_ID'].astype(int)
        )

        print(f'Updating career paths for {len(df_career_paths_to_update)} players...')

        # Iterate through each player to fetch their career data from Proballers
        dfs_career_paths = []
        for _, row in df_career_paths_to_update.iterrows():
            id_proballers = row['PROBALLERS_ID']
            print(f'{row['SEASON']} #{row['OVERALL_PICK']} - {row['PLAYER_NAME']}')

            # Fetch the player's career data from Proballers
            url_player = f'{URL_PROBALLERS}{id_proballers}'
            response = requests.get(url_player, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the section containing the table with regular season career data
            section_regular_season = soup.find('section', id='anchor-regular-season')
            if section_regular_season is None:
                print(f'No career data found for player {row["PLAYER_NAME"]} (ID: {id_proballers})')
                continue

            # Find the table with regular season career data
            table = section_regular_season.find('table')
            if table:
                # Row format: {"Season":"23-24", "Team":"Bourg-en-Bresse", "League":"FRA-1"}
                df_player_career = pd.read_html(StringIO(str(table)))[0]
                # Add columns
                df_player_career['Player_Name'] = row['PLAYER_NAME']
                df_player_career['Proballers_ID'] = row['PROBALLERS_ID']
                df_player_career['Draft_Year'] = draft_year
                df_player_career = df_player_career[
                    ['Player_Name', 'Proballers_ID', 'Season', 'Team', 'League', 'Draft_Year']
                ]

                dfs_career_paths.append(df_player_career)

        print()

        # Concatenate all player career data into a single DataFrame
        df_career_paths_draft_year = pd.concat(dfs_career_paths, ignore_index=True)
        df_career_paths_draft_year.to_json(
            f'data/career_paths/career_paths_draft_{draft_year}.json', orient='records', indent=4
        )

def calculate_years_since_draft(season: str, draft_year: int) -> int:
    """
    Calculate the number of years between the draft year and the start year of a season.

    :param season: The season in the format 'YY-YY' (e.g., '99-00').
    :type season: str
    :param draft_year: The year the player was drafted.
    :type draft_year: int
    :return: The number of years since the draft.
    :rtype: int
    """

    start_yy = int(season.split('-')[0])
    current_year = datetime.now().year
    current_century = current_year - (current_year % 100)

    # Try converting start_yy to a full year intelligently
    for century_offset in [-100, 0, 100]:
        potential_year = current_century + century_offset + start_yy
        # Assumption that a player does not compete earlier than 10 years before the draft
        # and not later than 30 years after the draft
        if -10 < abs(potential_year - draft_year) < 30:
            return potential_year - draft_year

    print(f'Possible incorrect data: played in season {season} and was drafted in {draft_year}.')
    return current_century + start_yy - draft_year

def map_countries_to_alpha3(df_career_paths: pd.DataFrame) -> pd.DataFrame:
    """
    Map country names to their ISO 3166-1 alpha-3 codes.

    :param df_career_paths: DataFrame containing career paths with country names
    :type df_career_paths: pd.DataFrame
    :return: DataFrame with an additional column for alpha-3 country codes
    :rtype: pd.DataFrame
    """

    # Load the league to country mapping from a JSON file
    with open('data/league_to_country_mappings.json', 'r', encoding='utf-8') as f:
        country_mappings = json.load(f)

    # Create a mapping from league prefixes (e.g., UK for UK-1)
    # names to their alpha-3 codes and names
    prefix_to_alpha3 = {prefix: country_mappings[prefix]['ALPHA-3'] for prefix in country_mappings}
    prefix_to_name = {prefix: country_mappings[prefix]['NAME'] for prefix in country_mappings}
    df_career_paths['Country_Alpha3'] = df_career_paths['League_Prefix'].map(prefix_to_alpha3)
    df_career_paths['Country_Name'] = df_career_paths['League_Prefix'].map(prefix_to_name)

    # Identify countries that are not mapped to alpha-3 codes
    is_country_unmapped = df_career_paths['Country_Alpha3'].isna()
    countries_without_mapping = (
        df_career_paths.loc[is_country_unmapped, 'League_Prefix'].unique()
    )

    # Create a mapping for unmapped countries
    for country_unmapped in countries_without_mapping:
        country_unmapped_alpha3 = None
        country_unmapped_name = None

        # Attempt to get the alpha-3 code and name from pycountry
        try:
            if len(country_unmapped) == 2:
                country_unmapped_alpha3 = pycountry_countries.get(alpha_2=country_unmapped).alpha_3
                country_unmapped_name = pycountry_countries.get(alpha_2=country_unmapped).name
            elif len(country_unmapped) == 3:
                country_unmapped_alpha3 = pycountry_countries.get(alpha_3=country_unmapped).alpha_3
                country_unmapped_name = pycountry_countries.get(alpha_3=country_unmapped).name

            # If the alpha-3 code is found, update the DataFrame
            if country_unmapped_alpha3 is not None:
                is_current_country = df_career_paths['League_Prefix'] == country_unmapped
                df_career_paths.loc[is_current_country, 'Country_Alpha3'] = country_unmapped_alpha3
                df_career_paths.loc[is_current_country, 'Country_Name'] = country_unmapped_name
                country_mappings[country_unmapped] = {
                    'ALPHA-3': country_unmapped_alpha3,
                    'NAME': country_unmapped_name
                }

        except AttributeError as e:
            print(f'Error processing country "{country_unmapped}": {e}')

    # Save the updated country mappings to a JSON file in alphabetical order
    with open('data/league_to_country_mappings.json', 'w', encoding='utf-8') as f:
        json.dump(dict(sorted(country_mappings.items())), f, indent=2)

    print('Countries without alpha-3 mapping:')
    print(
        df_career_paths.loc[df_career_paths['Country_Alpha3'].isna(), 'League_Prefix'].unique()
    )

    print() # Make space in the console output

    return df_career_paths

def create_dataframe_of_career_paths() -> pd.DataFrame:
    """
    Create a DataFrame containing all career paths of players with PROBALLERS_IDs.

    :return: DataFrame containing aggregated career paths
    :rtype: pd.DataFrame
    """

    dfs_career_paths = [
        pd.read_json(f'data/career_paths_per_draft_year/{filename}')
        for filename
        in os.listdir('data/career_paths_per_draft_year')
        if filename.endswith('.json')
    ]

    df_career_paths = pd.concat(dfs_career_paths, ignore_index=True)
    # Get prefix of league from the 'League' column (e.g., 'ARG-1' -> 'ARG') and map it to
    # ISO 3166-1 alpha-3 codes and names
    df_career_paths['League_Prefix'] = df_career_paths['League'].str.split('-').str[0]
    df_career_paths = map_countries_to_alpha3(df_career_paths)

    # Get draft information from the draft history dataset
    df_draft_history = pd.read_json('data/nba_draft_history.json')
    df_career_paths = pd.merge(
        left=df_career_paths,
        right=df_draft_history[['PROBALLERS_ID', 'SEASON', 'OVERALL_PICK']],
        left_on='Proballers_ID',
        right_on='PROBALLERS_ID',
        how='left'
    )

    # Clean up the DataFrame
    df_career_paths = df_career_paths.drop(columns=['PROBALLERS_ID'])   # Remove duplicate column
    df_career_paths = df_career_paths.rename(
        columns={'SEASON': 'Draft_Year', 'OVERALL_PICK': 'Overall_Pick'}
    )

    df_career_paths['Years_From_Draft'] = df_career_paths.apply(
        lambda row: calculate_years_since_draft(row['Season'], row['Draft_Year']),
        axis=1
    )

    # Remove outliers by assuming that a player does not compete earlier than 10 years before the
    # draft and not later than 30 years after the draft
    df_career_paths = df_career_paths[
        (df_career_paths['Years_From_Draft'] > -10)
        & (df_career_paths['Years_From_Draft'] <= 30)
    ]

    df_career_paths = df_career_paths.drop_duplicates() # Possible duplicates in original data
    df_career_paths = df_career_paths.reset_index(drop=True)

    return df_career_paths

def main():
    """
    Main function to build the draft history and add new draft players.

    :return: None
    """

    # WARNING: Only needed for the first build (before any PROBALLERS_IDs are added)
    # download_draft_history()

    # Check for newly drafted players and add them to the dataset.
    # Then manually add PROBALLERS_IDs to the JSON file.
    # After adding PROBALLERS_IDs, run the script again to update career paths.
    add_missing_players_to_draft_history()

    # Update career paths for players with PROBALLERS_IDs
    draft_years_to_update = range(2003, datetime.now().year + 1)
    scrape_career_paths(draft_years_to_update)

if __name__ == '__main__':
    main()
