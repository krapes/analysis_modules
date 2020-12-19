import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account


def get_bigquery_client(project_id):
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
        return client



def open_sql(dir_path: str, filename: str) -> str:
    """ Opens the file from the SQLs folder in this module and returns
        it as a string

    :param filename: name of file containing the desired query
    :return: string containing the contents of that file
    """
    file_path = os.path.join(dir_path, 'SQLs', filename)
    with open(file_path) as f:
        file_content = f.read()
    return file_content