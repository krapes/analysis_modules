import os
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import datetime
from typing import Tuple, Optional, Union
import plotly.graph_objects as go

from src import SankeyFlow

credential_path = "/home/kerri/bigquery-jaya-consultant-cosmic-octane-88917-c46ba9b53a3b.json"
assert os.path.exists(credential_path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
project_id = 'cosmic-octane-88917'
client = bigquery.Client(project=project_id)


class Flow(SankeyFlow):
    dir_path = os.path.dirname(os.path.realpath(__file__))

    def __init__(self,
                 flow_name: str,
                 start_date: datetime.date = None,
                 end_date: datetime.date = None) -> None:
        super().__init__()
        self._flow_name = flow_name
        self.start_date = start_date if start_date is not None else (datetime
                                                                     .datetime
                                                                     .strptime('2020-01-01', '%Y-%m-%d')
                                                                     .date())
        self.end_date = end_date if end_date is not None else datetime.date.today()

    def _open_sql(self, filename) -> str:
        file_path = os.path.join(self.dir_path, 'SQLs', filename)
        with open(file_path) as f:
            file_content = f.read()
        return file_content

    @staticmethod
    def time_stats(df, hue, topics) -> plt.Figure:
        rows = 2 * len(topics)
        fig, axes = plt.subplots(nrows=rows, figsize=(15, 7.5 * rows))

        for i, topic in enumerate(topics):
            chart = sns.lineplot(x="date", y=f"avg_14_day_{topic}",
                                 hue=hue,
                                 data=df, ax=axes[i])
            chart.set_title(f"14 Day Rolling Average {topic}")

            chart = sns.lineplot(x="date", y=topic,
                                 hue=hue,
                                 data=df, ax=axes[i + len(topics)])
            chart.set_title(topic)
        return fig

    def top_paths_plot(self) -> None:
        top_paths = self._open_sql('top_paths.sql')
        df = client.query(top_paths.format(f"('{self._flow_name}')")).to_dataframe()
        fig = self.time_stats(df, 'nickname', ['count', 'avg_duration'])
        # return fig

    def distinct_sessionId_count_plot(self) -> None:
        top_paths = self._open_sql('distinct_sessionId_count.sql')
        df = client.query(top_paths.format(f"('{self._flow_name}')")).to_dataframe()
        fig = self.time_stats(df, 'FlowName', ['count'])
        # return fig

    def _get_dates(self, start_date, end_date) -> Tuple[datetime.date, datetime.date]:
        if start_date is None:
            start_date = self.start_date
        elif type(start_date) is str:
            start_date.strptime('%Y-%m-%d')

        if end_date is None:
            end_date = self.end_date
        elif type(end_date) is str:
            end_date.strptime('%Y-%m-%d')

        return start_date, end_date


    def _formatted_flow_name(self):
        return f"('{self._flow_name}')"

    def create_user_sequence(self,
                             start_date: datetime.date = None,
                             end_date: datetime.date = None) -> pd.DataFrame:
        start_date, end_date = self._get_dates(start_date, end_date)
        top_paths = self._open_sql('user_sequence.sql')
        query = top_paths.format(self._formatted_flow_name(),
                                 start_date.strftime('%Y-%m-%d'),
                                 end_date.strftime('%Y-%m-%d'))
        df = client.query(query).to_dataframe()

        return df

    @staticmethod
    def _to_datetime(date):
        return datetime.datetime.combine(date, datetime.datetime.min.time()).replace(tzinfo=None)

    def sankey_plot(self,
                    start_date: Optional[Union[str, datetime.date]] = None,
                    end_date: Optional[Union[str, datetime.date]] = None,
                    threshold: int = 0,
                    title: str = None) -> go.Figure:
        start_date, end_date = self._get_dates(start_date, end_date)
        self._data = self.create_user_sequence(start_date, end_date) if self._data is None else self._data
        if start_date is not None:
            self._data = self._data[self._data['time_event'] > self._to_datetime(start_date)]
        if end_date is not None:
            self._data = self._data[self._data['time_event'] < self._to_datetime(end_date)]
        title = f"{self._flow_name} From {start_date} to {end_date}" if title is None else title
        fig = self.plot(threshold, title)
        return fig

    def sankey_plot_of_path(self, path_nickname):
        path_session_ids_query = self._open_sql('path_session_ids.sql')
        path_session_ids_query = path_session_ids_query.format(self._formatted_flow_name(),
                                                               path_nickname)
        path_session_ids = client.query(path_session_ids_query).to_dataframe().SessionId.to_list()
        data = self.create_user_sequence()
        self._data = data[data['user_id'].isin(path_session_ids)]
        self.sankey_plot(title=f"User Path of {path_nickname}")

