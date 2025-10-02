"""
Competitive Ranked Wordle MariaDB Database Handler

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


import mariadb

class MariaDB:
    def __init__(self, config: dict):
        self.config = config

        # For some reason, mariadb doesn't like common sense
        # If anyone ever reads this, going with this method yields:
        # TypeError: connect() argument 2 must be str or None, not tuple

        # self.username = config['mariadb']['user']
        # self.password = config['mariadb']['password']
        # self.host = config['mariadb']['host'],
        # self.port = config['mariadb']['port']
        # self.database = config['mariadb']['database']

    def connect_db(self):
        # sigh

        # conn = mariadb.connect(
        #     user=self.username,
        #     password=self.password,
        #     host=self.host,
        #     port=self.port,
        #     database=self.database,
        # )

        conn = mariadb.connect(
            user=self.config['mariadb']['user'],
            password=self.config['mariadb']['password'],
            host=self.config['mariadb']['host'],
            port=self.config['mariadb']['port'],
            database=self.config['mariadb']['database'],
        )
        cur = conn.cursor()
        return conn, cur
    
    def collate_cols(self, cols: list):
        query_string = ""
        i = 1
        for col in cols:
            query_add = ""
            if i == len(cols):
                query_add = f"{col} "
            else:
                query_add = f"{col}, "
            query_string = f"{query_string}{query_add}"
            i += 1
        return query_string
    
    def db_to_json(self, cols: list, data: list):
        json_data = []
        for row in data:
            i = 0
            row_dict = {}
            for cell in row:
                row_dict[cols[i]] = cell
                i += 1
            json_data.append(row_dict)
        return json_data

    def get_daily_submissions(self, filters: str):
        conn, cur = self.connect_db()
        
        cols = [
            'id', 
            'player_id', 
            'puzzle', 
            'raw_score', 
            'score', 
            'calculated_score', 
            'hard_mode', 
            'elo', 
            'mu', 
            'sigma', 
            'ordinal', 
            'elo_delta',
            'ordinal_delta' 
        ]

        query_string = f"SELECT {self.collate_cols(cols)}FROM scores {filters}"
        cur.execute(query_string)
        scores_raw = cur.fetchall()
        score_data = self.db_to_json(scores_raw)
        conn.close()
        return score_data

    def get_all_players(self, filters: str = ""):
        conn, cur = self.connect_db()

        cols = [
            'player_id', 
            'player_uuid', 
            'player_name', 
            'player_platform', 
            'player_mu', 
            'player_sigma', 
            'player_ord', 
            'player_elo', 
            'elo_delta', 
            'ord_delta', 
            'mu_delta', 
            'sigma_delta' 
        ]

        query_string = f"SELECT {self.collate_cols(cols)}FROM players"
        if filters:
            query_string = f"{query_string} WHERE {filters}"

        cur.execute(query_string)
        player_raw = cur.fetchall()
        players = self.db_to_json(cols, player_raw)
        conn.close()
        return players
    
    def get_enriched_puzzle_results(self, puzzle: int):
        conn, cur = self.connect_db()

        cols = [
            'scores.player_id',
            'scores.puzzle',
            'scores.raw_score',
            'scores.score',
            'scores.calculated_score',
            'scores.hard_mode',
            'scores.elo',
            'scores.mu',
            'scores.sigma',
            'scores.ordinal',
            'scores.elo_delta',
            'scores.ordinal_delta',
            'players.player_name',
            'players.player_uuid',
            'players.player_platform'
        ]

        query_string = f"SELECT {self.collate_cols(cols)}FROM scores INNER JOIN players ON scores.player_id = players.player_id WHERE puzzle = {puzzle}"
        cur.execute(query_string)
        data = cur.fetchall()
        data = self.db_to_json(cols, data)
        conn.close()
        return data