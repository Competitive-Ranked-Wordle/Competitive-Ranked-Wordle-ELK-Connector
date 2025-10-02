"""
Competitive Ranked Wordle Elasticsearch Logging Script

Copyright (C) 2025  Jivan RamjiSingh

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

from elasticsearch import Elasticsearch
from datetime import datetime, date, timedelta
from bin.mariadb_handler import MariaDB

class WordlELK:
    def __init__(self, config: dict):
        self.config = config
        self.db = MariaDB(self.config)
        self.elk = Elasticsearch(
            self.config['elasticsearch']['host'],
            ca_certs=self.config['elasticsearch']['cert'],
            api_key=self.config['elasticsearch']['api_key']
        )

        self.player_index = self.config['elasticsearch']['players_index']
        self.score_index = self.config['elasticsearch']['scores_index']
        self.enriched_index = self.config['elasticsearch']['enriched_index']

    def handle_and_send(self, submissions: list, index: str, puzzle: int):
        for submission in submissions:
            puzzle_date = self.get_date_from_puzzle(puzzle)
            puzzle_datetime = datetime.combine(puzzle_date, datetime.max.time())
            submission['timestamp'] = puzzle_datetime
            res = self.elk.index(index=index, document=submission)
            print(res['result'])

    def get_wordle_puzzle(self, puzzle_date):
        first_wordle = date(2021, 6, 19)
        delta = puzzle_date - first_wordle
        return delta.days

    def get_date_from_puzzle(self, puzzle: int):
        first_wordle = date(2021, 6, 19)
        puzzle_date = first_wordle + timedelta(days=puzzle)
        return puzzle_date

    def backfill(self, start: int, stop: int):
        for i in range(start, stop):
            submissions = self.db.get_daily_submissions(f"WHERE puzzle = {i}")
            if submissions == []:
                print(f"No Wordle Submissions for puzzle {i}")
                continue
            self.handle_and_send(submissions, self.score_index, i)
    
    def add_users(self):
        user_data = self.db.get_all_players()
        for user in user_data:
            user['timestamp'] = datetime.combine(date.today() - timedelta(days=1), datetime.max.time())
            res = self.elk.index(index=self.player_index, document=user)
            print(res['result'])
    
    def add_scores(self):
        puzzle = self.get_wordle_puzzle(date.today() - timedelta(days=1))
        submissions = self.db.get_daily_submissions(f"WHERE puzzle = {puzzle}")
        if submissions == []:
            print("No submissions today :(")
            return False
        self.handle_and_send(submissions, self.score_index, puzzle)

    def enriched_backfill(self, start: int, stop: int):
        for i in range(start, stop):
            submissions = self.db.get_enriched_puzzle_results(i)
            if submissions == []:
                print(f"No Wordle Submissions for puzzle {i}")
                continue
            self.handle_and_send(submissions, self.enriched_index, i)
    
    def add_enriched(self):
        puzzle = self.get_wordle_puzzle(date.today() - timedelta(days=1))
        submissions = self.db.get_enriched_puzzle_results(puzzle)
        if submissions == []:
            print("No submissions today :(")
            return False
        self.handle_and_send(submissions, self.enriched_index, puzzle)

if __name__ == '__main__':
    import os
    import yaml
    import argparse

    parser = argparse.ArgumentParser(description='Competitive Ranked Wordle Elastic Connector Script')
    parser.add_argument('--config', default='config.yml')

    subparsers = parser.add_subparsers(dest='mode', help='Available Commands')
    
    backfill_parser = subparsers.add_parser('backfill')
    backfill_parser.add_argument('--start', required=True)
    backfill_parser.add_argument('--end', required=True)

    score_parser = subparsers.add_parser('add_scores')

    user_parser = subparsers.add_parser('add_users')

    enriched_backfill_parser = subparsers.add_parser('enriched_backfill')
    enriched_backfill_parser.add_argument('--start', required=True)
    enriched_backfill_parser.add_argument('--end', required=True)

    enriched_parser = subparsers.add_parser('add_enriched')

    args = parser.parse_args()

    config_file = os.getenv('CONFIG_FILE', args.config)
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    connector = WordlELK(config)

    match args.mode:
        case 'backfill':
            print(f"Backfilling between Wordle {args.start} and {args.end}")
            connector.backfill(int(args.start), int(args.end))
        case 'add_scores':
            print('Adding Yesterdays Scores to ELK')
            connector.add_scores()
        case 'add_users':
            print('Adding Current User Information to ELK')
            connector.add_users()
        case 'enriched_backfill':
            print(f"Backfilling enriched data between Wordle {args.start} and {args.end}")
            connector.enriched_backfill(int(args.start), int(args.end))
        case 'add_enriched':
            print('Adding enriched data from yesterday to ELK')
            connector.add_enriched()