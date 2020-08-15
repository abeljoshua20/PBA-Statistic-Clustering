import pandas as pd
import numpy as np
import os
import re
from sqlalchemy import create_engine

raw_team_cols = ['GP', 'MIN', 'FGm', 'FGa', 'FG%', '3Pm', '3Pa',
                 '3P%', 'FTm', 'FTa', 'FT%', 'APG', 'STL', 'BLK',
                 'oREB', 'dREB', 'REB', 'PF', 'TOV', '+/-', 'PTS',
                 'team_id', 'hist_id', 'player_id']
final_team_cols = ['team_id', 'hist_id', 'player_id',
                   'GP', 'MIN', 'FGm', 'FGa', 'FG%', '3Pm', '3Pa',
                   '3P%', 'FTm', 'FTa', 'FT%', 'APG', 'STL', 'BLK',
                   'oREB', 'dREB', 'REB', 'PF', 'TOV', '+/-', 'PTS']
raw_season_cols = ['GP', '3Pm', '3Pa', '3P%', '2Pm',
                   '2Pa', '2P%', 'FGm', 'FGa', 'FG%', 'FTm', 'FTa',
                   'FT%', 'dREB', 'oREB', 'REB', 'AST', 'STL', 'BLK',
                   'TO', 'PTO', 'PF', 'FBm', 'FBa', 'FBm%', 'bPTS',
                   'PTS', 'W', 'L', 'team_id', 'hist_id']
final_season_cols = ['team_id', 'hist_id', 'GP', '3Pm', '3Pa', '3P%',
                     '2Pm', '2Pa', '2P%', 'FGm', 'FGa', 'FG%', 'FTm',
                     'FTa', 'FT%', 'dREB', 'oREB', 'REB', 'AST', 'STL',
                     'BLK', 'TO', 'PTO', 'PF', 'FBm', 'FBa', 'FBm%',
                     'bPTS', 'PTS', 'W', 'L']


class PBA_Consolidator:
    """Represents a PBA Consolidator CSV class"""

    def __init__(self):
        """Initialize a new instance of a PBA_Consolidator"""
        self.repo_team = 'data/pba_team_csv'
        self.repo_season = 'data/pba_season_csv'
        self.fp_player_desc = 'data/player_desc.csv'
        self.df_tot_stat = pd.DataFrame(columns=raw_team_cols)
        self.df_avg_stat = pd.DataFrame(columns=raw_team_cols)
        self.df_team_tot_stat = pd.DataFrame(columns=raw_season_cols)
        self.df_team_avg_stat = pd.DataFrame(columns=raw_season_cols)
        self.df_history = pd.DataFrame(columns=['year', 'conference_id'])
        self.df_conference = pd.DataFrame(columns=['conference'])
        self.df_player = pd.DataFrame(columns=['player_name',
                                               'j_number',
                                               'height',
                                               'pos'])
        self.df_team = pd.DataFrame(columns=['team_name'])

    def _add_team(self, team_name):
        """Add team to the dataframe of team"""
        m1 = self.df_team.team_name == team_name
        result = self.df_team.loc[m1]
        if len(result):
            return result.index[0]
        team_id = len(self.df_team)
        self.df_team.loc[team_id] = team_name
        return team_id

    def _add_player(self, player_name):
        """Add player to the dataframe of player"""
        m1 = self.df_player.player_name == player_name
        result = self.df_player.loc[m1]
        if len(result):
            return result.index[0]
        player_id = len(self.df_player)
        df_desc = pd.read_csv(self.fp_player_desc).fillna(0)
        srs = df_desc.loc[df_desc.Name == player_name]

        if len(srs):
            jersey = srs.j_number.values[0]
            height = srs.height.values[0]
            pos = srs.pos.values[0]
        else:
            jersey = None
            height = None
            pos = None

        self.df_player.loc[player_id] = (player_name, jersey,
                                         height, pos)
        return player_id

    def _add_hist(self, year, conf_id):
        """Add history to the dataframe of history"""
        m1 = self.df_history.year == year
        m2 = self.df_history.conference_id == conf_id
        result = self.df_history.loc[m1 & m2]
        if len(result):
            return result.index[0]
        hist_id = len(self.df_history)
        self.df_history.loc[hist_id] = [year, conf_id]
        return hist_id

    def _add_conf(self, conf_name):
        """Add conference to the dataframe of conference"""
        m1 = self.df_conference.conference == conf_name
        result = self.df_conference.loc[m1]
        if len(result):
            return result.index[0]
        conf_id = len(self.df_conference)
        self.df_conference.loc[conf_id] = conf_name
        return conf_id

    def _find_history(self, year, conf_id):
        """Return history id based on year and conference"""
        m1 = self.df_history.year == year
        m2 = self.df_history.conference_id == conf_id
        result = self.df_history.loc[m1 & m2]
        if len(result):
            return result.index[0]
        hist_id = len(self.df_history)
        self.df_history.loc[hist_id] = [year, conf_id]
        return hist_id

    def _consolidate_playerstat(self):
        """Consolidate player statistics located in the repository of teams"""
        # Define a lambda function
        for file in os.listdir(self.repo_team):
            fp = os.path.join(self.repo_team, file)
            obj = re.search(r'(\d{4})_([A-Z]+)_([A-Z]+)_(AVG|TOT)',
                            file)
            year = obj.group(1)
            conf = obj.group(2)
            team = obj.group(3)
            stat_type = obj.group(4)
            df = pd.read_csv(fp).drop_duplicates()

            conf_id = self._add_conf(conf)
            team_id = self._add_team(team)
            df['team_id'] = team_id
            hist_id = self._add_hist(year, conf_id)
            df['hist_id'] = hist_id
            df['player_id'] = df.PLAYERS.apply(lambda x: self._add_player(x))
            # Drop foreign key columns
            df.drop(['PLAYERS', 'Team'], axis=1, inplace=True)
            # Drop duplicates
            df.drop_duplicates(subset=['team_id', 'player_id', 'hist_id'],
                               inplace=True)
            # Drop players that have zero record
            m1 = df['MIN'] != 0
            m2 = df['+/-'] != 0
            df = df.loc[m1 | m2]

            # Convert percentage to float
            perc_col = [col for col in df.columns if '%' in col]
            for col in perc_col:
                if df[col].dtype != 'O':
                    # If column type is not a string continue
                    continue
                df[col] = (df[col].apply(lambda x: int(x.replace('%', '')) /
                                         100))

            if stat_type == 'TOT':
                self.df_tot_stat = self.df_tot_stat.append(df,
                                                           ignore_index=True)
            else:
                self.df_avg_stat = self.df_avg_stat.append(df,
                                                           ignore_index=True)
        self.df_tot_stat = self.df_tot_stat[final_team_cols]
        self.df_avg_stat = self.df_avg_stat[final_team_cols]

    def _consolidate_teamstat(self):
        """Consolidate team statistics located in the repository of season"""
        for file in os.listdir(self.repo_season):
            fp = os.path.join(self.repo_season, file)
            obj = re.search(r'(\d{4})_([A-Z]+)_(AVG|TOT)', file)
            year = obj.group(1)
            conf = obj.group(2)
            stat_type = obj.group(3)
            df = pd.read_csv(fp).drop_duplicates()

            conf_id = self._add_conf(conf)
            hist_id = self._find_history(year, conf_id)
            df['team_id'] = df.Team.apply(lambda x: self._add_team(x))
            df['hist_id'] = hist_id

            # Drop the team column
            df.drop(['Team'], axis=1, inplace=True)

            # Convert percentage to float
            perc_col = [col for col in df.columns if '%' in col]
            for col in perc_col:
                if df[col].dtype != 'O':
                    # If column type is not a string continue
                    continue
                df[col] = (df[col].apply(lambda x: int(x.replace('%', '')) /
                                         100))

            if stat_type == 'TOT':
                self.df_team_tot_stat = (self.df_team_tot_stat
                                         .append(df, ignore_index=True))
            else:
                self.df_team_avg_stat = (self.df_team_avg_stat
                                         .append(df, ignore_index=True))
        self.df_team_tot_stat = self.df_team_tot_stat[final_season_cols]
        self.df_team_avg_stat = self.df_team_avg_stat[final_season_cols]

    def create_db(self):
        """Create PBA database"""
        self._consolidate_playerstat()
        self._consolidate_teamstat()
        engine = create_engine('sqlite:///pba.db')
        with engine.connect() as conn:
            self.df_tot_stat.to_sql('total_stat', con=conn,
                                    if_exists='replace')
            self.df_avg_stat.to_sql('avg_stat', con=conn,
                                    if_exists='replace')
            self.df_team_tot_stat.to_sql('team_total_stat', con=conn,
                                         if_exists='replace')
            self.df_team_avg_stat.to_sql('team_avg_stat', con=conn,
                                         if_exists='replace')
            self.df_history.to_sql('history', con=conn,
                                   if_exists='replace')
            self.df_conference.to_sql('conference', con=conn,
                                      if_exists='replace')
            self.df_player.to_sql('player', con=conn,
                                  if_exists='replace')
            self.df_team.to_sql('team', con=conn,
                                if_exists='replace')
        engine.dispose()