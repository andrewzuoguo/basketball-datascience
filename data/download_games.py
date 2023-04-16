import pandas as pd
from datetime import datetime, timedelta
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from connect_sqlite import connect, get_current_time


def download_games():
    """
    Downloads all NBA games for all NBA teams

    :return: DataFrame of games
    """

    nba_teams = teams.get_teams()
    nba_teams = pd.DataFrame(nba_teams)
    ids = list(nba_teams['id'])
    games = pd.DataFrame()

    while len(ids) > 0:
        try:
            id = ids[-1]

            if len(games) == 0:
                print(f'initiating with team {id}')
                result = leaguegamefinder.LeagueGameFinder(team_id_nullable=id)
                games = result.get_data_frames()[0]

            elif id not in list(games['TEAM_ID']):
                print(f'trying team {id}')
                result = leaguegamefinder.LeagueGameFinder(team_id_nullable=id)
                games = pd.concat([games, result.get_data_frames()[0]])

            else:
                print(f'already have team {id}')

            ids.pop()

        except:
            print(f'retrying player {id}')


    games = games.sort_values(by='GAME_DATE', ascending=False)

    return games



def get_last_update(CONNECTION):
    """
    Read the most recent date of updating Games table

    :param CONNECTION: Connection object representing the connection to the SQLite database.
    :return: Date of the last update
    """

    df = pd.read_sql_query('SELECT Date FROM Last_Updated WHERE Type = "game"', CONNECTION)
    return df.values[0][0]


def get_new_games(start_date):
    """
    Retrieves all NBA games that occurred after the specified start_date and returns a pandas DataFrame containing the games

    :param start_date: Date to start retrieving NBA games from
    :return: DataFrame of games
    """

    result = leaguegamefinder.LeagueGameFinder(date_from_nullable=start_date, league_id_nullable='00')
    games_toadd = result.get_data_frames()[0]

    return games_toadd


def combine_team_games(df, keep_method='home'):
    """
    Combine a TEAM_ID-GAME_ID unique table into rows by game

    :param df: DataFrame of games to merge
    :param keep_method: {'home', 'away', 'winner', 'loser', ``None``}, default 'home'
            - 'home' : Keep rows where TEAM_A is the home team.
            - 'away' : Keep rows where TEAM_A is the away team.
            - 'winner' : Keep rows where TEAM_A is the losing team.
            - 'loser' : Keep rows where TEAM_A is the winning team.
            - ``None`` : Keep all rows. Will result in an output DataFrame the same
                length as the input DataFrame.
    :return: DataFrame of merged games
    """

    # Join every row to all others with the same game ID.
    joined = pd.merge(df, df, suffixes=['_A', '_B'],
                      on=['SEASON_ID', 'GAME_ID', 'GAME_DATE'])
    # Filter out any row that is joined to itself.
    result = joined[joined.TEAM_ID_A != joined.TEAM_ID_B]
    # Take action based on the keep_method flag.
    if keep_method is None:
        # Return all the rows.
        pass
    elif keep_method.lower() == 'home':
        # Keep rows where TEAM_A is the home team.
        result = result[result.MATCHUP_A.str.contains(' vs. ')]
    elif keep_method.lower() == 'away':
        # Keep rows where TEAM_A is the away team.
        result = result[result.MATCHUP_A.str.contains(' @ ')]
    elif keep_method.lower() == 'winner':
        result = result[result.WL_A == 'W']
    elif keep_method.lower() == 'loser':
        result = result[result.WL_A == 'L']
    else:
        raise ValueError(f'Invalid keep_method: {keep_method}')
    return result

def save_sql(games_toadd, games_toadd_merged, CONNECTION, new_date):
    """
    Saves NBA game data to a SQLite database
    :param games_toadd: DataFrame unmerged NBA games to add to the database
    :param games_toadd_merged: DataFrame merged NBA games to add to the database
    :param CONNECTION: Connection object representing the connection to the SQLite database
    :param new_date: Date of the last NBA game
    :return: None
    """
    games_toadd.to_sql('ALL_GAMES_UNMERGED', CONNECTION, if_exists='append', index=False)
    games_toadd_merged.to_sql('ALL_GAMES_MERGED', CONNECTION, if_exists='append', index=False)

    df = pd.read_sql_query('SELECT * FROM Last_Updated', CONNECTION)
    df = df.set_index('Type')
    df.at['game', 'Date'] = new_date
    df.to_sql('Last_Updated', CONNECTION, if_exists='replace')

    return 0


if __name__ == "__main__":
    download_all = None

    # Connect to the SQLite database.
    with connect() as CONNECTION:
        CONNECTION = connect()

        try:
            # Get the last update date from the database.
            last_date = get_last_update(CONNECTION)
            last_date = datetime.strptime(last_date, '%m/%d/%Y').date()
            start_date = last_date + timedelta(days=1)
            start_date = start_date.strftime('%m/%d/%Y')

            # Only download games from the start_date onward.
            download_all = False
        except:
            # If there are no games in the database yet, download all games.
            print('No games currently in database')
            download_all = True

        if download_all == True:
            # Download all games
            games_toadd = download_games()
            games_toadd_merged = combine_team_games(games_toadd)

        elif download_all == False:
            # Download only new games
            games_toadd = get_new_games(start_date)
            games_toadd_merged = combine_team_games(games_toadd)

        else:
            print('Error downloading games')

        # Get the current date for the new update.
        new_date = datetime.today() - timedelta(days=1)
        new_date = new_date.strftime('%m/%d/%Y')  # WANT TO RUN MORNING AFTER ALL GAMES FROM PREVIOUS NIGHT FINISHED

        # Save the downloaded games to the database and update the last update date.
        save_sql(games_toadd, games_toadd_merged, CONNECTION, new_date)

        # Log the number of games downloaded and the new last update date.
        print(F'{get_current_time()}: Added {len(games_toadd_merged)} games, up to date through {new_date}')
