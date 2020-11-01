# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

from src import Flow

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.GRID, external_stylesheets])

FLOW_NAME = "Italy - Customer Service"
flow = Flow(flow_name=FLOW_NAME)
fig = flow.sankey_plot()


app.layout = html.Div(children=[
    dbc.Row(dbc.Col(html.H1(children=f'SharkNinja'))),
    dbc.Row(dbc.Col(html.H1(children=f'{FLOW_NAME}'))),
    dbc.Row(
            [
                dbc.Col(dcc.Graph(id='sankey'), width=11),
                dbc.Col([html.Label('Threshold3'),
                        dcc.Slider(
                            id='threshold_slider',
                            min=0,
                            max=flow.sankey_max_links(),
                            value=10,
                            marks={str(i): str(i) for i in range(flow.sankey_max_links()) if i % int(flow.sankey_max_links()/25) == 0},
                            step=10,
                            vertical=True
                        )])
            ])
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
