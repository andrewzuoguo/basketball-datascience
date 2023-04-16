import pandas as pd
from nba_api.stats.static import teams, players
from connect_sqlite import connect

def download_static(conn):
    """
    Downloads:
        - List of NBA Teams
        - List of Active Players
        - List of Inactive Players
    Saves to separate table in connected SQLite database

    :param conn: Connection object representing the connection to the SQLite database
    """
    team_list = pd.DataFrame(teams.get_teams())
    team_list = team_list.set_index('id')
    team_list.to_sql('TEAM_LIST', conn, if_exists='replace')

    active_player_list = pd.DataFrame(players.get_active_players())
    active_player_list = active_player_list.set_index('id')
    active_player_list.to_sql('PLAYER_LIST_ACTIVE', conn, if_exists='replace')

    inactive_player_list = pd.DataFrame(players.get_inactive_players())
    inactive_player_list = inactive_player_list.set_index('id')
    inactive_player_list.to_sql('PLAYER_LIST_INACTIVE', conn, if_exists='replace')

if __name__ == '__main__':
    with connect() as conn:
        download_static(conn)

    print('Updated Team List')
    print('Updated Active Player List')
    print('Updated Inactive Player List')