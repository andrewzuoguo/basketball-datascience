import schedule
import time
import os


def job():
    os.system('python3 download_games.py >> log.txt 2')
    os.system('python3 download_league_player_stats.py >> log.txt 2>&1')



schedule.every().day.at('06:00').do(job)

while True:
    schedule.run_pending()
    time.sleep(1)