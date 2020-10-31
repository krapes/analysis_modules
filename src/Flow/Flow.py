import os
from google.cloud import bigquery
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
import datetime




credential_path = "/home/kerri/bigquery-jaya-consultant-cosmic-octane-88917-c46ba9b53a3b.json"
assert os.path.exists(credential_path)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
project_id = 'cosmic-octane-88917'
client = bigquery.Client(project=project_id)

class Flow():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    def __init__(self, FlowName):
        self._FlowName = FlowName


    def _open_sql(self, filename):
        file_path = os.path.join(self.dir_path, 'SQLs', filename)
        with open(file_path) as f:
            file_content = f.read()
        return file_content

    @staticmethod
    def time_stats(df, hue, topics):
        rows = 2 * len(topics)
        fig, axes = plt.subplots(nrows=rows, figsize=(15, 7.5*rows))

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


    def top_paths_plot(self):
        top_paths = self._open_sql('top_paths.sql')
        df = client.query(top_paths.format(f"('{self._FlowName}')")).to_dataframe()
        fig = self.time_stats(df, 'nickname', ['count', 'avg_duration'])
        return fig
