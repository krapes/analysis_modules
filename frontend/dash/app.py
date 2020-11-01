# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

from src import Flow

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

FLOW_NAME = "Italy - Customer Service"
flow = Flow(flow_name=FLOW_NAME)
fig = flow.sankey_plot()


app.layout = html.Div(children=[
    html.H1(children=f'SharkNinja - {FLOW_NAME}'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='sankey'
    ),
    html.Label('Slider'),
    dcc.Slider(
        id='threshold_slider',
        min=1,
        max=100,
        value=10,
        marks={str(i): str(i) for i in range(100)},
        step=None
    )
])

@app.callback(
    Output('sankey', 'figure'),
    [Input('threshold_slider', 'value')])
def update_figure(threshold):
    try:
        fig = flow.sankey_modify_threshold(threshold)
    except:
        fig = flow.sankey_plot()
    return fig




if __name__ == '__main__':
    app.run_server(debug=True)
