import pandas as pd
from datetime import datetime, timedelta
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from connect_sqlite import connect, get_current_time


def download_games():
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
    df = pd.read_sql_query('SELECT Date FROM Last_Updated WHERE Type = "game"', CONNECTION)
    return df.values[0][0]


def get_new_games(start_date):
    result = leaguegamefinder.LeagueGameFinder(date_from_nullable=start_date, league_id_nullable='00')
    games_toadd = result.get_data_frames()[0]

    return games_toadd


def combine_team_games(df, keep_method='home'):
    '''Combine a TEAM_ID-GAME_ID unique table into rows by game. Slow.

        Parameters
        ----------
        df : Input DataFrame.
        keep_method : {'home', 'away', 'winner', 'loser', ``None``}, default 'home'
            - 'home' : Keep rows where TEAM_A is the home team.
            - 'away' : Keep rows where TEAM_A is the away team.
            - 'winner' : Keep rows where TEAM_A is the losing team.
            - 'loser' : Keep rows where TEAM_A is the winning team.
            - ``None`` : Keep all rows. Will result in an output DataFrame the same
                length as the input DataFrame.

        Returns
        -------
        result : DataFrame
    '''
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
    games_toadd.to_sql('ALL_GAMES_UNMERGED', CONNECTION, if_exists='append', index=False)
    games_toadd_merged.to_sql('ALL_GAMES_MERGED', CONNECTION, if_exists='append', index=False)

    df = pd.read_sql_query('SELECT * FROM Last_Updated', CONNECTION)
    df = df.set_index('Type')
    df.loc['game'].at['Date'] = new_date
    df.to_sql('Last_Updated', CONNECTION, if_exists='replace')

    return 0


if __name__ == "__main__":
    download_all = None

    with connect() as CONNECTION:
        CONNECTION = connect()

        try:
            last_date = get_last_update(CONNECTION)
            last_date = datetime.strptime(last_date, '%m/%d/%Y').date()
            start_date = last_date + timedelta(days=1)
            start_date = start_date.strftime('%m/%d/%Y')

            download_all = False
        except:
            print('No games currently in database')
            download_all = True

        if download_all == True:
            games_toadd = download_games()
            games_toadd_merged = combine_team_games(games_toadd)

        elif download_all == False:
            games_toadd = get_new_games(start_date)
            games_toadd_merged = combine_team_games(games_toadd)

        else:
            print('Error downloading games')

        new_date = datetime.today() - timedelta(days=1)
        new_date = new_date.strftime('%m/%d/%Y') #WANT TO RUN MORNING AFTER ALL GAMES FROM PREVIOUS NIGHT FINISHED

        save_sql(games_toadd, games_toadd_merged, CONNECTION, new_date)

        print(F'{get_current_time()}: Added {len(games_toadd_merged)} games, up to date through {new_date}')

