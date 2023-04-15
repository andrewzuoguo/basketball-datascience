import pandas as pd
from nba_api.stats.endpoints import alltimeleadersgrids
from connect_sqlite import connect

def download_alltimeleaders():
    """
    Download data of all-time NBA leaders and return as a pandas DataFrame.
    """

    leaders = alltimeleadersgrids.AllTimeLeadersGrids().get_data_frames()

    desc = ['GP', 'PTS', 'AST', 'STL', 'OREB', 'DREB', 'REB', 'BLK', 'FGM', 'FGA', 'FGPCT', 'TOV', 'FG3M', 'FG3A', 'FG3PCT', 'PF', 'FTM', 'FTA', 'FTPCT']

    out = pd.DataFrame()
    for df, des in zip(leaders, desc):
        df.columns = ['PLAYER_ID', 'PLAYER_NAME', 'VALUE', 'RANK', 'IS_ACTIVE']
        df['TYPE'] = des

        if len(out) == 0:
            out = df
        else:
            out = pd.concat([out, df])

    out['IS_ACTIVE'] = out['IS_ACTIVE'].apply(lambda x: False if x=='N' else True)

    COLUMN_TYPES = {
        'PLAYER_ID': int,
        'PLAYER_NAME': str,
        'VALUE': int,
        'RANK': int,
        'IS_ACTIVE': bool,
        'TYPE': str
    }
    out = out.astype(COLUMN_TYPES)

    return out



if __name__ == '__main__':
    # Download all-time NBA leaders data
    leaders = download_alltimeleaders()

    # Save data to SQLite database
    TABLE_NAME = 'ALLTIMELEADERS'
    with connect() as conn:
        leaders.to_sql(TABLE_NAME, conn, if_exists='replace')