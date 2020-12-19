import os
from src import Utilities



class CpassStatus:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    def __init__(self, project_id: str) -> None:
        self.project_id = project_id
        self.client = Utilities.get_bigquery_client(project_id)

    def get_available_flows(self):
        query = Utilities.open_sql(self.dir_path, 'flownames.sql')
        df = self.client.query(query).to_dataframe()
        return df['FlowName'].to_list()
