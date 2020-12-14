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
server = app.server

AVAILABLE_FLOWS = ["Italy - Customer Service",
                   "Germany - Customer Service"]
LOADER = 'dot'

global flow
flow = Flow(flow_name=AVAILABLE_FLOWS[0])


app.layout = html.Div(children=[
    dbc.Row(dbc.Col(html.H1(children=f'SharkNinja'))),
    dbc.Row([dbc.Col(dcc.Dropdown(
        id='available_flows',
        options=[{'label': i, 'value': i} for i in AVAILABLE_FLOWS],
        value=AVAILABLE_FLOWS[0]
    ), width=3),
        dbc.Col(dcc.Dropdown(id='path_name',
                             options=[
                                 {'label': '1-Path_Freq_Rank', 'value': '1-Path_Freq_Rank'},
                                 {'label': '2-Path_Freq_Rank', 'value': '2-Path_Freq_Rank'},
                                 {'label': '3-Path_Freq_Rank', 'value': '3-Path_Freq_Rank'},
                                 {'label': '4-Path_Freq_Rank', 'value': '4-Path_Freq_Rank'},
                                 {'label': '5-Path_Freq_Rank', 'value': '5-Path_Freq_Rank'},
                                 {'label': '6-Path_Freq_Rank', 'value': '6-Path_Freq_Rank'},
                                 {'label': '7-Path_Freq_Rank', 'value': '7-Path_Freq_Rank'},
                                 {'label': '8-Path_Freq_Rank', 'value': '8-Path_Freq_Rank'},
                                 {'label': '9-Path_Freq_Rank', 'value': '9-Path_Freq_Rank'},
                                 {'label': '10-Path_Freq_Rank', 'value': '10-Path_Freq_Rank'},
                             ],
                             value='1-Path_Freq_Rank'
                             ), width=3)]),
    dbc.Row(dbc.Col(html.H1(id='flow_name'))),
    dbc.Row([
        dbc.Col([dcc.Loading(
            id="loading-1",
            type=LOADER,
            children=[dcc.Graph(id='paths_time')])], width=5),
        dbc.Col([dcc.Loading(
            id="loading-2",
            type=LOADER,
            children=[dcc.Graph(id='sankey')])], width=6),
        dbc.Col([html.Label('Threshold'),
                 dcc.Slider(
                     id='threshold_slider',
                     min=0,
                     max=100,
                     value=10,
                     step=1,
                     vertical=True
                 )])
    ]),
    dbc.Row([

        dbc.Col([dcc.Loading(
            id="loading-3",
            type=LOADER,
            children=[dcc.Graph(id='totals_time')]),
            html.H4(id='date_range'),
            dcc.RangeSlider(
                id='date_slider',
                min=0,
                max=100,
                value=[0, 100],
                step=5,
            )], width={"size": 10, "offset": 1}, align="center")])

])


@app.callback(
    [Output('sankey', 'figure'),
     Output('paths_time', 'figure'),
     Output('totals_time', 'figure'),
     Output('flow_name', 'children'),
     Output('date_range', 'children')],
    [Input('threshold_slider', 'value'),
     Input('available_flows', 'value'),
     Input('date_slider', 'value'),
     Input('path_name', 'value')])
def update_figure(threshold, flow_name, date_range, path_name):
    print(f"threshold: {threshold} flow_name: {flow_name} date_range: {date_range} path_name: {path_name}")
    global flow
    new_start_date, new_end_date = flow.date_at_percent(date_range[0]), flow.date_at_percent(date_range[1])
    date_range = f"Showing Sessions from {flow.start_date} to {flow.end_date}"
    if flow_name == flow._flow_name and new_start_date == flow.start_date and new_end_date == flow.end_date:
        flow.threshold = threshold
        flow.path_highlight = path_name
        fig_sankey = flow.sankey_modify_path_highlight(path_name)
    elif flow_name == flow._flow_name:
        print("Change dates")
        flow.start_date = new_start_date
        flow.end_date = new_end_date
        flow.threshold = threshold
        flow.path_highlight = path_name
        fig_sankey = flow.sankey_plot()
    else:
        flow = Flow(flow_name=flow_name, start_date=new_start_date, end_date=new_end_date)
        flow.threshold = threshold
        flow.path_highlight = path_name
        fig_sankey = flow.sankey_plot()
    fig_totals_time = flow.distinct_sessionId_count_plot()
    fig_paths_time = flow.top_paths_plot()
    return fig_sankey, fig_paths_time, fig_totals_time, flow_name, date_range


if __name__ == '__main__':
    app.run_server(debug=True)
