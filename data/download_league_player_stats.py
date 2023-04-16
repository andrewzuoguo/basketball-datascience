import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats, playerdashptshots
from connect_sqlite import connect, get_current_time


def get_seasons(START: int, END: int):
    """
    Given a start year (inclusive) and end year (exclusive), return a list of NBA seasons in the format "YYYY-YY".

    :param START: The start year (inclusive) of the range of seasons to include.
    :param END: The end year (exclusive) of the range of seasons to include.
    :return: A list of strings representing the NBA seasons in the format "YYYY-YY".
    """

    seasons = []
    for i in range(START, END):
        ii = str(i + 1)
        ii = ii[2:]
        seasons.append(F"{i}-{ii}")

    return seasons


def download_league_player_stats(seasons, current_season=False):
    """
    Download NBA player statistics for a given list of seasons.

    :param seasons: A list of seasons in the format 'YYYY-YY' to download the statistics for.
    :param current_season: Bool whether the downloaded statistics correspond to the current active season. Default is False.
    :return: A tuple containing three elements:
        1. DataFrame containing the downloaded statistics.
        2. 1D numpy array of player IDs for the downloaded statistics.
        3. 1D numpy array of team IDs for the downloaded statistics.
    """
    league_stats = pd.DataFrame()
    for season in seasons:
        while True:
            try:
                if len(league_stats) == 0:
                    league_stats = leaguedashplayerstats.LeagueDashPlayerStats(season=season).get_data_frames()[0]
                    league_stats['SEASON'] = season
                else:
                    temp = leaguedashplayerstats.LeagueDashPlayerStats(season=season).get_data_frames()[0]
                    temp['SEASON'] = season
                    league_stats = pd.concat([league_stats, temp])

            except:
                continue

            break

    league_stats['SEASON_CURRENT'] = current_season

    player_id = league_stats['PLAYER_ID'].to_numpy()
    team_id = league_stats['TEAM_ID'].to_numpy()

    return league_stats, player_id, team_id


def download_player_pt_shots(player_id, team_id, season, CONNECTION, current = False):
    """
    Downloads shooting data for all NBA players for a given season. Shooting data is available beginning in the 2013-14 NBA season.
    More info at https://www.nba.com/stats/players/shots-general

    :param player_id: 1D numpy array of player IDs
    :param team_id: 1D numpy array of team IDs
    :param season: str representing a season, in the format 'YYYY-YY'
    :param current: bool indicating whether season is the current active season. Default False
    :param CONNECTION: A database connection object
    :return: Nothing. Tables are saved to database directly
    """

    dfs = [pd.DataFrame() for _ in range(7)]

    for p_id, t_id in zip(player_id, team_id):
        while True:
            try:
                result = playerdashptshots.PlayerDashPtShots(t_id, p_id, season=season).get_data_frames()
            except:
                continue
            break

        for i, df in enumerate(result):
            dfs[i] = pd.concat([dfs[i], df])
            dfs[i]['SEASON'] = season

    if current:
        table_names = ['SHOT_OVERALL_CURRENT', 'SHOT_TYPE_CURRENT', 'SHOT_CLOCK_CURRENT', 'SHOT_DRIBBLE_CURRENT', 'SHOT_CLOSEDEF_CURRENT', 'SHOT_CLOSEDEF_10PLUS_CURRENT', 'SHOT_TOUCHTIME_CURRENT']
        for name, df in zip(table_names, dfs):
            df.to_sql(name, CONNECTION, if_exists='replace')

    else:
        table_names = ['SHOT_OVERALL_PAST', 'SHOT_TYPE_PAST', 'SHOT_CLOCK_PAST', 'SHOT_DRIBBLE_PAST', 'SHOT_CLOSEDEF_PAST', 'SHOT_CLOSEDEF_10PLUS_PAST', 'SHOT_TOUCHTIME_PAST']
        for name, df in zip(table_names, dfs):
            df.to_sql(name, CONNECTION, if_exists='append')

    return


if __name__ == '__main__':

    #Check if past table exists
    with connect() as conn:
        try:
            pd.read_sql_query("SELECT * FROM LEAGUE_PLAYER_STATS_PAST", conn)

        except Exception as e: #Create the table with stats from previous completed seasons
            seasons = get_seasons(START=1996, END=2021)

            league_stats, player_id, team_id = download_league_player_stats(seasons)
            league_stats.to_sql('LEAGUE_PLAYER_STATS_PAST', conn, if_exists='replace')
            print(f'{get_current_time()}: Added {len(league_stats)} League-Player Stats (Past)')

            seasons = get_seasons(START=2013, END=2021)
            for season in seasons:
                league_stats, player_id, team_id = download_league_player_stats(season)
                download_player_pt_shots(player_id, team_id, season, conn, current=False)
                print(f'{get_current_time()}: Updated {len(league_stats)} Shot Profile Stats from season {season} (PAST)')


        #Update current season only
        current_season = '2022-23'

        league_stats, player_id, team_id = download_league_player_stats(current_season)
        league_stats.to_sql('LEAGUE_PLAYER_STATS_CURRENT', conn, if_exists='replace')
        print(f'{get_current_time()}: Updated {len(league_stats)} League-Player Stats')

        download_player_pt_shots(player_id, team_id, current_season, conn, current=True)
        print(f'{get_current_time()}: Updated {len(league_stats)} Shot Profile Stats from season {current_season}')