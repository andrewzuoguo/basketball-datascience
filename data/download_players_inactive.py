import pandas as pd
from nba_api.stats.endpoints import playercareerstats
from connect_sqlite import connect, get_current_time


def download_allplayerseasons(player_ids):
    """
    Download regular season player statistics for all inactive NBA players
    :param player_ids: List of ids of inactive players
    :return: DataFrame of all regular season player statistics for the given player IDs, with additional columns for PPG, RPG, and APG
    """

    allplayerseasons = pd.DataFrame()

    while len(player_ids) > 0:
        try:
            id = player_ids[-1]

            if len(allplayerseasons) == 0:
                playercareer = playercareerstats.PlayerCareerStats(player_id=id, timeout=100)
                allplayerseasons = playercareer.get_data_frames()[0]

            elif id not in list(allplayerseasons['PLAYER_ID']):
                playercareer = playercareerstats.PlayerCareerStats(player_id=id, timeout=100)
                allplayerseasons = pd.concat([allplayerseasons, playercareer.get_data_frames()[0]])

            player_ids.pop()

        except Exception as e:
            print(f'Error downloading inactive player {id}: {e}')
            continue


    allplayerseasons['PPG'] = allplayerseasons['PTS']/allplayerseasons['GP']
    allplayerseasons['RPG'] = allplayerseasons['REB']/allplayerseasons['GP']
    allplayerseasons['APG'] = allplayerseasons['AST']/allplayerseasons['GP']

    return allplayerseasons


if __name__ == '__main__':

    with connect() as conn:
        player_info = pd.read_sql_query("SELECT id, full_name, first_name, last_name, is_active FROM PLAYER_LIST_INACTIVE", conn, params=())
        player_ids = list(player_info['id'])

        allplayerseasons = download_allplayerseasons(player_ids)
        allplayerseasons = allplayerseasons.merge(player_info, how='right', left_on='PLAYER_ID', right_on='id')

        allplayerseasons.to_sql('PLAYERS_INACTIVE', conn, if_exists='replace')
        print(F'{get_current_time()}: Updated Inactive Players Table')
