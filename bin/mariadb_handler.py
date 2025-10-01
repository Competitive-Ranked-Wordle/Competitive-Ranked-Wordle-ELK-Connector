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

        query_string = f"SELECT "
        i = 1
        for col in cols:
            query_add = ""
            if i == len(cols):
                query_add = f"{col} "
            else:
                query_add = f"{col}, "
            query_string = f"{query_string}{query_add}"
            i += 1
        query_string = f"{query_string}FROM scores {filters}" 
        cur.execute(query_string)
        scores_raw = cur.fetchall()
        
        score_data = []
        for row in scores_raw:
            i = 0
            row_dict = {}
            for cell in row:
                row_dict[cols[i]] = cell
                i += 1
            score_data.append(row_dict)

        conn.close()
        return score_data

    def get_all_players(self):
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

        query_string = f"SELECT "
        i = 1
        for col in cols:
            query_add = ""
            if i == len(cols):
                query_add = f"{col} "
            else:
                query_add = f"{col}, "
            query_string = f"{query_string}{query_add}"
            i += 1

        query_string = f"{query_string}FROM players"
        cur.execute(query_string)
        player_raw = cur.fetchall()

        players = []
        for player in player_raw:
            player_data = {}
            i = 0
            for cell in player:
                player_data[cols[i]] = cell
                i += 1
            players.append(player_data)

        conn.close()
        return players