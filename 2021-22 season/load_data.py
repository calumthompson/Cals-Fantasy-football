import pandas as pd
import requests
import json
from pathlib import Path
import numpy as np
from tqdm import tqdm

bootstrap_API_address = 'https://fantasy.premierleague.com/api/bootstrap-static/'
fixtures_API_address = 'https://fantasy.premierleague.com/api/fixtures/'
matches_API_root_address = 'https://fantasy.premierleague.com/api/element-summary/'


def load_matches_data(player_ID):
    API_call_address = matches_API_root_address + str(player_ID) + '/'
    matches_API_response = requests.get(API_call_address).json()

    upcoming_games_df = parse_upcoming_games_data(matches_API_response)
    previous_games_df = parse_previous_games_data(matches_API_response)
    last_seasons_games_df = parse_last_seasons_data(matches_API_response)

    this_season_df = pd.concat([previous_games_df, upcoming_games_df], axis = 0)

    this_season_df['team_ID'] = this_season_df['team_ID'].bfill()

    this_season_df['joiner'] = 1
    last_seasons_games_df['joiner'] = 1

    this_season_df['player_ID'] = player_ID

    return pd.merge(this_season_df, last_seasons_games_df).drop(columns = 'joiner')


def parse_upcoming_games_data(matches_API_response):
    player_upcoming_df = pd.DataFrame.from_dict(matches_API_response['fixtures'])

    player_upcoming_df['team_ID'] = np.where(player_upcoming_df['is_home'] == True,
                                             player_upcoming_df['team_h'],
                                             player_upcoming_df['team_a'])

    player_upcoming_df['opponent_ID'] = np.where(player_upcoming_df['is_home'] == True,
                                                 player_upcoming_df['team_a'],
                                                 player_upcoming_df['team_h'])

    player_upcoming_df = player_upcoming_df[['event',
                                             'team_ID',
                                             'opponent_ID',
                                             'kickoff_time',
                                             'is_home',
                                             'difficulty']]

    player_upcoming_df = player_upcoming_df.rename(columns = {'event'     : 'GW',
                                                              'difficulty': 'fixture_difficulty'})

    return player_upcoming_df


def parse_previous_games_data(matches_API_response):
    ### For now just return total points and values that match upcoming games data

    previous_matches_df = pd.DataFrame.from_dict(matches_API_response['history'])

    previous_matches_df = previous_matches_df[['round',
                                               'kickoff_time',
                                               'opponent_team',
                                               'was_home',
                                               'total_points']]

    previous_matches_df = previous_matches_df.rename(columns = {'round'        : 'GW',
                                                                'opponent_team': 'opponent_ID',
                                                                'was_home'     : 'is_home'})

    return previous_matches_df


def parse_last_seasons_data(matches_API_response):

    historic_df = pd.DataFrame.from_dict(matches_API_response['history_past'])

    if historic_df.empty:

        return pd.DataFrame({'last_season_start_cost': {0: np.nan},
                             'last_season_end_cost': {0: np.nan},
                             'last_season_total_points': {0: np.nan},
                             'last_season_minutes': {0: np.nan},
                             'last_season_goals_scored': {0: np.nan},
                             'last_season_assists': {0: np.nan},
                             'last_season_clean_sheets': {0: np.nan},
                             'last_season_goals_conceded': {0: np.nan},
                             'last_season_own_goals': {0: np.nan},
                             'last_season_penalties_saved': {0: np.nan},
                             'last_season_penalties_missed': {0: np.nan},
                             'last_season_yellow_cards': {0: np.nan},
                             'last_season_red_cards': {0: np.nan},
                             'last_season_saves': {0: np.nan},
                             'last_season_bonus': {0: np.nan},
                             'last_season_bps': {0: np.nan},
                             'last_season_influence': {0: np.nan},
                             'last_season_creativity': {0: np.nan},
                             'last_season_threat': {0: np.nan},
                             'last_season_ict_index': {0: np.nan}})

    historic_df = historic_df.loc[historic_df['season_name'] == '2020/21']
    historic_df = historic_df.drop(['season_name', 'element_code'], axis = 1)
    historic_df.columns = ['last_season_' + x for x in historic_df.columns]

    return historic_df


def load_MVP_data():
    bootstrap_API_response = requests.get(bootstrap_API_address).json()
    fixtures_API_response = requests.get(fixtures_API_address).json()

    ### Load players
    elements_API_load_df = pd.DataFrame.from_dict(bootstrap_API_response['elements'])

    ### Map positions
    positions_df = pd.DataFrame.from_dict(bootstrap_API_response['element_types'])
    positions_map = pd.Series(positions_df['singular_name'].values, index = positions_df['id']).to_dict()

    players_df = elements_API_load_df.replace({'element_type': positions_map})

    players_df = players_df[['first_name',
                             'second_name',
                             'id',
                             'chance_of_playing_next_round',
                             'element_type',
                             'news',
                             'now_cost',
                             'status',
                             'team']]

    players_df = players_df.rename(columns = {'first_name'                  : 'player_first_name',
                                              'second_name'                 : 'player_second_name',
                                              'id'                          : 'player_ID',
                                              'chance_of_playing_next_round': 'player_chance_of_playing_next_round',
                                              'element_type'                : 'player_position',
                                              'news'                        : 'player_news',
                                              'now_cost'                    : 'player_cost',
                                              'status'                      : 'player_status',
                                              'team'                        : 'team_ID'})

    players_df['player_cost'] = players_df['player_cost'] / 10

    players_df['player_chance_of_playing_next_round'] = (players_df['player_chance_of_playing_next_round'] / 100).fillna(1)

    matches_load = pd.DataFrame()

    for player_id in tqdm(players_df['player_ID'].unique()):

        matches_load = pd.concat([matches_load , load_matches_data(player_id)])

    return pd.merge(players_df, matches_load, on =['player_ID', 'team_ID'], how = 'left')


def save_element_data():
    bootstrap_API_response = requests.get(bootstrap_API_address).json()

    elements_data = pd.DataFrame.from_dict(bootstrap_API_response['elements'])
    GW_data = pd.DataFrame.from_dict(bootstrap_API_response['events'])

    # return GW_data

    GW_deadline = GW_data.loc[GW_data['is_next'] == True]['deadline_time'].iloc[0]

    elements_data.to_csv('Elements data/' + str(GW_deadline) + '.csv')
