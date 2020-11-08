import os
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import datetime
from typing import Tuple, Optional, Union, List
import plotly.graph_objects as go
import pytz
import plotly.express as px
from plotly.subplots import make_subplots

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
        print(f"Flow {flow_name} successfully created")

    def _open_sql(self, filename) -> str:
        """ Opens the file from the SQLs folder in this module and returns
            it as a string

        :param filename: name of file containing the desired query
        :return: string containing the contents of that file
        """
        file_path = os.path.join(self.dir_path, 'SQLs', filename)
        with open(file_path) as f:
            file_content = f.read()
        return file_content

    def date_at_percent(self, percentage):
        if hasattr(self, 'master') == False:
            self._get_master()
        start, end = self.master['time_event'].min(), self.master['time_event'].max()
        delta_from_start = (end - start) * percentage / 100
        date = (start + delta_from_start).to_pydatetime().date()
        print(percentage, start, delta_from_start, date)
        return date

    @staticmethod
    def time_stats(df: pd.DataFrame, hue: str, topics: List[str]) -> plt.Figure:
        """Returns two line plots for every topic. The first containing a 14 day rolling
            average, the second containing the daily average.

        :param df: data containing values to be plotted
        :param hue: column name of category labels
        :param topics: metrics that are being plotted (Ex. count, duration, etc)
        :return: plotly figure containing a total of 2*len(topics) graphs
        """
        def plot_traces(fig: go.Figure,
                        data: pd.DataFrame,
                        x: str,
                        y: str,
                        hue: str,
                        row: int,
                        col: int,
                        mode: str = 'lines') -> go.Figure:
            """ The goal is create a similar behavior as plotly express or seaborn.
                This function will take x, y, and hue column names and use them to layer
                the correct scatter plots together.

            :param fig:
            :param data:
            :param x:
            :param y:
            :param hue:
            :param row:
            :param col:
            :param mode:
            :return:
            """
            for n, category in enumerate(data[hue].unique()):
                temp = data[data[hue] == category]
                chart = go.Scatter(x=temp[x],
                                                y=temp[y],
                                                mode='lines',
                                                name=category,
                                                marker_color=px.colors.sequential.Plasma[n]
                                                )
                fig.add_trace(chart, row=row, col=col)
            return fig

        rows = 2 * len(topics)

        titles = [""] * rows
        for i, topic in enumerate(topics):
            titles[i] = f"14 Day Rolling Average {topic}"
            titles[(i + len(topics))] = topic
        fig = make_subplots(rows=rows, cols=1, subplot_titles=tuple(titles))

        for i, topic in enumerate(topics, 1):
            fig = plot_traces(fig,
                              data=df,
                              x='date',
                              y=f"avg_14_day_{topic}",
                              hue=hue,
                              row=i, col=1)

            fig = plot_traces(fig,
                              data=df,
                              x='date',
                              y=topic,
                              hue=hue,
                              row=(i + len(topics)), col=1)

        fig.update_layout(width=700, height=(300 * rows))
        return fig

    @staticmethod
    def _fig_layout(fig):
        fig.update_layout(
            xaxis=dict(
                showline=True,
                showgrid=False,
                showticklabels=True,
                linecolor='rgb(204, 204, 204)',
                linewidth=2,
                ticks='outside',
                tickfont=dict(
                    family='Arial',
                    size=12,
                    color='rgb(82, 82, 82)',
                ),
            ),
            yaxis=dict(
                showgrid=False,
                zeroline=False,
                showline=False,
                showticklabels=False,
            ),
            autosize=False,
            margin=dict(
                autoexpand=False,
                l=100,
                r=20,
                t=110,
            ),
            showlegend=True,
            plot_bgcolor='white'
        )
        return fig

    def top_paths_plot(self) -> None:
        """ Calculates the 10 most common user paths and plots their distinct
            SessionId count and average call duration

        :return: 4 Seaborn line plots containing distinct sessionId counts and
        average call duration
        """
        top_paths = self._open_sql('top_paths.sql')
        df = client.query(top_paths.format(f"('{self._flow_name}')")).to_dataframe()
        fig = self.time_stats(df, 'nickname', ['count', 'avg_duration'])
        fig = self._fig_layout(fig)
        return fig

    def distinct_sessionId_count_plot(self) -> None:
        """ Gets the count of unique sessionIds per day and

        :return: two plots containing unique sessionId count and the 14 day rolling average
        """
        sql = self._open_sql('distinct_sessionId_count.sql')
        df = client.query(sql.format(self._formatted_flow_name())).to_dataframe()
        fig = self.time_stats(df, 'FlowName', ['count'])
        fig = self._fig_layout(fig)
        return fig

    def _get_date(self,
                  date: Optional[Union[str, datetime.date]],
                  default: datetime.date) -> datetime.date:
        """ takes a date in various formats and returns it or it's default in the format
            datetime.date

        :param date: date that needs to be transformed to datetime.date
        :param default: default value if date is None
        :return: datetime.date
        """
        if date is None:
            date = default
        elif type(date) is str:
            date = date.strptime('%Y-%m-%d')
        elif type(date) is datetime.date:
            pass
        else:
            raise Exception(f"Value date need to be type str or datetime.date found {type(date)}")

        return date

    def _formatted_flow_name(self):
        """ Returns the flow name formatted as a single entry in a tuple for the SQL
        :return: the flow name formatted as a single entry in a tuple for the SQL
        """
        return f"('{self._flow_name}')"

    def _get_master(self):
        start_date, end_date = self._get_date(None, self.start_date), self._get_date(None, self.end_date)
        top_paths = self._open_sql('user_sequence.sql')
        query = top_paths.format(self._formatted_flow_name(),
                                 start_date.strftime('%Y-%m-%d'),
                                 end_date.strftime('%Y-%m-%d'))
        self.master = client.query(query).to_dataframe()
        print(f"length master: {len(self.master)}")

    def create_user_sequence(self,
                             start_date: datetime.date = None,
                             end_date: datetime.date = None) -> pd.DataFrame:
        """ Runs query user_sequence that returns the ADR events with timestamp and rank

        :param start_date: all entries will be after this date
        :param end_date: all entries will be before this date
        :return: pandas dataframe with data
        """
        if hasattr(self, 'master') == False:
            self._get_master()

        df = self.master.copy()
        df = df[df['time_event'] > self._to_datetime(start_date)]
        df = df[df['time_event'] < self._to_datetime(end_date)]
        print(f"length df: {len(df)}")
        print(self._to_datetime(start_date), self._to_datetime(end_date))
        return df

    @staticmethod
    def _to_datetime(date):
        """ Converts a date object to a datetime object with time of midnight

        :param date: date object that needs to be converted
        :return: datetime object that starts at midnight
        """
        return pytz.utc.localize(
            datetime.datetime.combine(date, datetime.datetime.min.time()))  # .replace(tzinfo='utc')

    def sankey_plot(self,
                    start_date: Optional[Union[str, datetime.date]] = None,
                    end_date: Optional[Union[str, datetime.date]] = None,
                    threshold: int = 0,
                    title: str = None,
                    data: pd.DataFrame = None) -> go.Figure:
        """
        Creates a plotly Sankey figure of the flow between the dates start_date and end_date if
        they are provided and using the data if provided. If not provided it will use self._data

        :param start_date: All entries will occur after this date
        :param end_date: All entries will occur before this date
        :param threshold: paths with less than this number of users will not be displayed
        :param title: chart title
        :param data: Optional data that will be used to generate the plot
        :return: SanKey figure
        """
        start_date, end_date = self._get_date(start_date, self.start_date), self._get_date(end_date, self.end_date)
        if data is not None:
            self._data = data
        else:
            self._data = self.create_user_sequence(start_date, end_date)
        # TODO reinstate date selection
        '''
        if start_date is not None:
            self._data = self._data[self._data['time_event'] > self._to_datetime(start_date)]
        if end_date is not None:
            self._data = self._data[self._data['time_event'] < self._to_datetime(end_date)]
        '''
        title = f"{self._flow_name} From {start_date} to {end_date}" if title is None else title
        fig = self.plot(threshold, title)
        return fig

    def sankey_plot_of_path(self, path_nickname):
        """ creates a sankey plot of only the path path_nickname

        :param path_nickname: name of path to be extracted and plotted
        :return: Sankey figure
        """
        path_session_ids_query = self._open_sql('path_session_ids.sql')
        path_session_ids_query = path_session_ids_query.format(self._formatted_flow_name(),
                                                               path_nickname)
        path_session_ids = client.query(path_session_ids_query).to_dataframe().SessionId.to_list()
        data = self.create_user_sequence()
        print(f"DATA LENGTH {len(data)}")
        data = data[data['user_id'].isin(path_session_ids)]
        print(f"DATA LENGTH {len(data)}")
        fig = self.sankey_plot(title=f"User Path of {path_nickname}", data=data)
        return fig
