import os
from google.cloud import bigquery
import matplotlib.pyplot as plt
import pandas as pd
import datetime
from typing import Tuple, Optional, Union, List, Dict, Any
import plotly.graph_objects as go
import pytz
import plotly.express as px
from plotly.subplots import make_subplots
import time
from anycache import anycache
import json
from google.oauth2 import service_account

from src import SankeyFlow

# credential_path = "/home/kerri/bigquery-jaya-consultant-cosmic-octane-88917-c46ba9b53a3b.json"
project_id = 'cosmic-octane-88917'

if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') == None:
    # the json credentials stored as env variable
    json_str = os.environ.get('GOOGLE_CREDENTIALS')

    # generate json - if there are errors here remove newlines in .env
    json_data = json.loads(json_str)
    # the private_key needs to replace \n parsed as string literal with escaped newlines
    json_data['private_key'] = json_data['private_key'].replace('\\n', '\n')

    # use service_account to generate credentials object
    credentials = service_account.Credentials.from_service_account_info(
        json_data)
else:
    credential_path = "/home/kerri/bigquery-jaya-consultant-cosmic-octane-88917-c46ba9b53a3b.json"
    assert os.path.exists(credential_path)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
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

    def _open_sql(self, filename: str) -> str:
        """ Opens the file from the SQLs folder in this module and returns
            it as a string

        :param filename: name of file containing the desired query
        :return: string containing the contents of that file
        """
        file_path = os.path.join(self.dir_path, 'SQLs', filename)
        with open(file_path) as f:
            file_content = f.read()
        return file_content

    def date_at_percent(self, percentage: int):
        """ Given an int that represents a percentage from zero to 100 the function returns
        the date that represents percentage of the total timeline for the dataset
        Example: minimum date in dataset == January 1st, Maximum date in dataset == December 31st
        percentage: 50 --> function returns July 1st

        :param percentage: desired point on the timeline
        :return: date at desired point on the timeline
        """
        if percentage < 0 or percentage > 100:
            raise Exception("percentage must be between 0 and 100")

        if hasattr(self, 'master') == False:
            self._get_master()
        start, end = self.master['time_event'].min(), self.master['time_event'].max()
        delta_from_start = (end - start) * percentage / 100
        date = (start + delta_from_start).to_pydatetime().date()
        return date

    @staticmethod
    def time_stats(df: pd.DataFrame,
                   hue: str,
                   topics: Dict[str, int],
                   dates: Tuple[datetime.date, datetime.date] = None) -> plt.Figure:
        """Returns two line plots for every topic. The first containing a 14 day rolling
            average, the second containing the daily average.

        :param df: data containing values to be plotted
        :param hue: column name of category labels
        :param topics: metrics that are being plotted (Ex. count, duration, etc)
        :param dates: date range where the plots will be shaded
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

            :param fig: ploty fig where this plot (or these traces) will be places
            :param data: dataframe containing all the columns listed in x, y, and hue
            :param x: column name of x-axis
            :param y: column name of y-axis
            :param hue: column name of category column
            :param row: the row number inside the fig where the plot will be located
            :param col: the column number inside the fig where the plot
            :param mode: plotly.graph_objects.Scatter mode value
            :return: plot fig with plot added s
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

        rows = len(topics.keys())

        titles = [""] * len(topics.keys()) * 2
        for i, topic in enumerate(topics.keys()):
            titles[i] = f"14 Day Rolling Average {topic}"
            titles[(i + len(topics.keys()))] = topic
        fig = make_subplots(rows=2, cols=len(topics.keys()), subplot_titles=tuple(titles))

        for i, topic in enumerate(topics, 1):
            fig = plot_traces(fig,
                              data=df,
                              x='date',
                              y=f"avg_14_day_{topic}",
                              hue=hue,
                              row=1, col=topics[topic])
            y_max = df[f"avg_14_day_{topic}"].max()

            if dates is not None:
                fig.add_trace(go.Scatter(x=[dates[0], dates[1]], y=[y_max, y_max], fill='tozeroy'),
                              row=1, col=topics[topic])
            fig = plot_traces(fig,
                              data=df,
                              x='date',
                              y=topic,
                              hue=hue,
                              row=2, col=topics[topic])
            y_max = df[topic].max()
            if dates is not None:
                fig.add_trace(go.Scatter(x=[dates[0], dates[1]], y=[y_max, y_max], fill='tozeroy'),
                              row=2, col=topics[topic])

        fig.update_layout(width=700, height=(300 * rows))
        return fig

    @staticmethod
    def _fig_layout(fig: go.Figure) -> go.Figure:
        """ Modify the figure to match the desired style

        :param fig: figure to be formatted
        :return: formatted figure
        """
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
            autosize=True,
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
        print("Creating top_paths_plot")
        if hasattr(self, '_data') == False:
            self._data = self.create_user_sequence(self.start_date, self.end_date)

        df = self._data.copy()
        target_paths = df.value_counts(['path_nickname'])[:10].reset_index().path_nickname.to_list()
        df = df[df['path_nickname'].isin(target_paths)]
        path_metrics = df.groupby(['path_nickname', 'date']).agg({'session_duration': ['mean'], 'count': ['sum']},
                                                                 as_index=False).reset_index()
        df = pd.DataFrame({'path_nickname': path_metrics['path_nickname'],
                           'date': path_metrics['date'],
                           'avg_duration': path_metrics['session_duration']['mean'],
                           'count': path_metrics['count']['sum']})
        df['avg_14_day_avg_duration'] = df['avg_duration'].rolling(14).mean()
        df['avg_14_day_count'] = df['count'].rolling(14).mean()
        fig = self.time_stats(df,
                              'path_nickname',
                              {'count': 1, 'avg_duration': 2})
        fig = self._fig_layout(fig)
        return fig

    def distinct_sessionId_count_plot(self) -> None:
        """ Gets the count of unique sessionIds per day and

        :return: two plots containing unique sessionId count and the 14 day rolling average
        """
        print("Creating distinct_sessionId_count_plot")
        if hasattr(self, 'master') == False:
            self._get_master()

        df = self.master.copy()
        path_metrics = df.groupby(['date', 'FlowName']).agg({'count': ['sum']},
                                                            as_index=False).reset_index()
        df = pd.DataFrame({'FlowName': path_metrics['FlowName'],
                           'date': path_metrics['date'],
                           'count': path_metrics['count']['sum']})

        df['avg_14_day_count'] = df['count'].rolling(14).mean()
        fig = self.time_stats(df, 'FlowName', {'count': 1}, (self.start_date, self.end_date))
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

    @staticmethod
    @anycache(cachedir=os.path.join(dir_path, 'data/anycache.my'))
    def query_db(query: str, flow_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        """ Get source data from Bigquery

        :param query: .sql that should be run
        :param flow_name: name of flow or flows that should be inserted into query
        :param start_date: date that should be inserted into query
        :param end_date: date that should be inserted into query
        :return: dataframe containing results of query
        """
        print("Starting Query")

        query = query.format(flow_name,
                             start_date,
                             end_date)
        return client.query(query).to_dataframe()

    # TODO Testing only, remove this
    @anycache(cachedir=os.path.join(dir_path, 'data/anycache.my'))
    def _get_master(self):
        """ Get the global dataset that will be used for all calculations inside
        this instance of the class

        :return: None
        """
        start_time = time.time()
        start_date, end_date = self._get_date(None, self.start_date), self._get_date(None, self.end_date)
        query = self._open_sql('user_sequence.sql')

        # Temp fix for testing because my credentials are not working
        cache = True
        if cache:
            df = pd.read_csv(os.path.join(self.dir_path,
                                          'data/manually_loaded_data',
                                          'United Kingdom-Customer Service.csv'))
            print("Converting object types...")
            df = df.astype({'time_event': 'datetime64[ns]'})
            df['time_event'] = df.time_event.apply(lambda x: pytz.utc.localize(x))
            df['date'] = df.time_event.dt.date
            self.master = df
        else:
            self.master = self.query_db(query,
                                        self._formatted_flow_name(),
                                        start_date.strftime('%Y-%m-%d'),
                                        end_date.strftime('%Y-%m-%d'))
        print(f"Master Dataset Gathered in {round(time.time() - start_time, 0)} seconds")

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
        if start_date is not None:
            df = df[df['time_event'] > self._to_datetime(start_date)]
        if end_date is not None:
            df = df[df['time_event'] < self._to_datetime(end_date)]
        return df

    @staticmethod
    def _to_datetime(date: datetime.date) -> datetime.datetime:
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
