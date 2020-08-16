import pandas as pd
from sqlalchemy import create_engine


class PBA_Fetcher:
    def __init__(self):
        """Initialize an instance of PBA Fetcher"""
        engine = create_engine('sqlite:///pba.db')
        with engine.connect() as conn:
            self.df_player = pd.read_sql('SELECT * FROM player', con=conn)
            self.df_player['pos'] = self.df_player['pos'].fillna('O')
            self.df_avg_stat = pd.read_sql('SELECT * FROM avg_stat',
                                           con=conn)
            self.df_tot_stat = pd.read_sql('SELECT * FROM total_stat',
                                           con=conn)
            self.df_team = pd.read_sql('SELECT * FROM team', con=conn)
            self.df_team_avg_stat = pd.read_sql('SELECT * FROM team_avg_stat',
                                                con=conn)
            self.df_team_tot_stat = pd.read_sql("""SELECT *
                                                FROM team_total_stat""",
                                                con=conn)
            self.df_history = pd.read_sql('SELECT * FROM history', con=conn)
            self.df_conference = pd.read_sql(
                'SELECT * FROM conference', con=conn)
        engine.dispose()

    def get_total_player(self):
        """Get Total player statistics"""
        return self._get_playerstat_with_name(self.df_tot_stat)

    def get_avg_player(self):
        """Get Average player statistics"""
        return self._get_playerstat_with_name(self.df_avg_stat)

    def get_total_team(self):
        """Get Total team statistics"""
        return self._get_teamstat_with_name(self.df_team_tot_stat)

    def get_avg_team(self):
        """Get Average team statistics"""
        return self._get_teamstat_with_name(self.df_team_avg_stat)

    def get_total_player_team(self):
        """Get Total of player & team statistics"""
        df_all_total = get_total_stat(self)
        return self._get_allstat_with_name(df_all_total)

    def get_avg_player_team(self):
        """Get Average of player & team statistics"""
        df_all_average = get_average_stat(self)
        return self._get_allstat_with_name(df_all_average)

    def get_avg_total_player(self):
        """Get Average + Total player statistics"""
        df_stat = (pd.merge(self.df_avg_stat.add_suffix('_avg'),
                            self.df_tot_stat.add_suffix('_total'),
                            how='inner',
                            left_on=['hist_id_avg',
                                     'player_id_avg', 'team_id_avg'],
                            right_on=['hist_id_total', 'player_id_total',
                                      'team_id_total']))

        all_cols = [col for col in df_stat.columns
                    if ('id' not in col) & ('index' not in col)]
        all_cols.insert(0, 'year')
        all_cols.insert(1, 'conference')
        all_cols.insert(2, 'player_name')
        all_cols.insert(3, 'team_name')
        all_cols.insert(4, 'pos')
        all_cols.insert(5, 'j_number')
        all_cols.insert(6, 'height')
        all_cols.insert(7, 'weight')

        df_all_year = pd.merge(df_stat, self.df_history, how='inner',
                               left_on='hist_id_avg', right_on='index')
        df_all_conf = pd.merge(df_all_year, self.df_conference,
                               how='left',
                               left_on='conference_id', right_on='index')
        df_all_player = pd.merge(df_all_conf, self.df_player,
                                 how='left',
                                 left_on='player_id_avg', right_on='index')
        df_all_named = pd.merge(df_all_player, self.df_team,
                                how='left',
                                left_on='team_id_avg', right_on='index')
        return df_all_named[all_cols]

    def get_avg_total_team(self):
        """Get Average + Total team statistics"""
        df_stat = (pd.merge(self.df_team_avg_stat.add_suffix('_avg'),
                            self.df_team_tot_stat.add_suffix('_total'),
                            how='inner',
                            left_on=['team_id_avg', 'hist_id_avg'],
                            right_on=['team_id_total', 'hist_id_total']))
        all_cols = [col for col in df_stat.columns
                    if ('id' not in col) & ('index' not in col)]
        all_cols.insert(0, 'year')
        all_cols.insert(1, 'conference')
        all_cols.insert(2, 'team_name')

        df_all_year = pd.merge(df_stat, self.df_history, how='inner',
                               left_on='hist_id_avg', right_on='index')
        df_all_conf = pd.merge(df_all_year, self.df_conference,
                               how='left',
                               left_on='conference_id', right_on='index')
        df_all_team = pd.merge(df_all_conf, self.df_team,
                               how='left',
                               left_on='team_id_avg', right_on='index')

        return df_all_team[all_cols]

    def get_all(self):
        """"Get Average + TOTAL player & team statistics"""
        df_all_stat = self._get_all_stat()
        return self._get_allstat_with_name(df_all_stat)

    def _get_total_stat(self):
        """Return all total stat"""
        df_all_total = (pd.merge(self.df_tot_stat.add_prefix('ply_'),
                                 self.df_team_tot_stat.add_prefix('tm_'),
                                 how='left',
                                 left_on=['ply_team_id', 'ply_hist_id'],
                                 right_on=['tm_team_id', 'tm_hist_id']))
        return df_all_total

    def _get_average_stat(self):
        """Return all average stat"""
        df_all_average = (pd.merge(self.df_avg_stat.add_prefix('ply_'),
                                   self.df_team_avg_stat.add_prefix('tm_'),
                                   how='left',
                                   left_on=['ply_team_id', 'ply_hist_id'],
                                   right_on=['tm_team_id', 'tm_hist_id']))
        return df_all_average

    def _get_all_stat(self):
        """Return all stat values"""
        df_all_total = self._get_total_stat()
        df_all_average = self._get_average_stat()
        df_all_stat = pd.merge(df_all_average, df_all_total,
                               on=['ply_team_id', 'ply_hist_id',
                                   'ply_player_id'],
                               how='inner',
                               suffixes=['_avg', '_total'])
        return df_all_stat

    def _get_playerstat_with_name(self, df_stat):
        """Return df_stat with name of the foreign keys"""
        all_cols = [col for col in df_stat.columns
                    if ('id' not in col) & ('index' not in col)]
        all_cols.insert(0, 'year')
        all_cols.insert(1, 'conference')
        all_cols.insert(2, 'player_name')
        all_cols.insert(3, 'team_name')
        all_cols.insert(4, 'pos')
        all_cols.insert(5, 'j_number')
        all_cols.insert(6, 'height')
        all_cols.insert(7, 'weight')
        df_all_year = pd.merge(df_stat, self.df_history, how='inner',
                               left_on='hist_id', right_on='index')
        df_all_conf = pd.merge(df_all_year, self.df_conference,
                               how='left',
                               left_on='conference_id', right_on='index')
        df_all_player = pd.merge(df_all_conf, self.df_player, how='left',
                                 left_on='player_id', right_on='index')
        df_all_named = pd.merge(df_all_player, self.df_team, how='left',
                                left_on='team_id', right_on='index')
        return df_all_named[all_cols]

    def _get_teamstat_with_name(self, df_stat):
        """Return df_stat with name of the foreign keys"""
        all_cols = [col for col in df_stat.columns
                    if ('id' not in col) & ('index' not in col)]
        all_cols.insert(0, 'year')
        all_cols.insert(1, 'conference')
        all_cols.insert(2, 'team_name')
        df_all_year = pd.merge(df_stat, self.df_history, how='inner',
                               left_on='hist_id', right_on='index')
        df_all_conf = pd.merge(df_all_year, self.df_conference,
                               how='left',
                               left_on='conference_id', right_on='index')
        df_all_named = pd.merge(df_all_conf, self.df_team, how='left',
                                left_on='team_id', right_on='index')
        return df_all_named[all_cols]

    def _get_allstat_with_name(self, df_stat):
        """Return df_stat (Average + Total) with name of the foreign keys"""
        all_cols = [col for col in df_stat.columns
                    if ('id' not in col) & ('index' not in col)]
        all_cols.insert(0, 'year')
        all_cols.insert(1, 'conference')
        all_cols.insert(2, 'player_name')
        all_cols.insert(3, 'team_name')
        all_cols.insert(4, 'pos')
        all_cols.insert(5, 'j_number')
        all_cols.insert(6, 'height')
        all_cols.insert(7, 'weight')
        df_all_year = pd.merge(df_stat, self.df_history, how='inner',
                               left_on='ply_hist_id', right_on='index')
        df_all_conf = pd.merge(df_all_year, self.df_conference,
                               how='left',
                               left_on='conference_id', right_on='index')
        df_all_player = pd.merge(df_all_conf, self.df_player, how='left',
                                 left_on='ply_player_id', right_on='index')
        df_all_named = pd.merge(df_all_player, self.df_team, how='left',
                                left_on='ply_team_id', right_on='index')
        return df_all_named[all_cols]