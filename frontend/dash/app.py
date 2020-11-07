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

AVAILABLE_FLOWS = ["Italy - Customer Service",
                    "Germany - Customer Service"]

global flow
flow = Flow(flow_name=AVAILABLE_FLOWS[0])
fig = flow.sankey_plot()



app.layout = html.Div(children=[
    dbc.Row(dbc.Col(html.H1(children=f'SharkNinja'))),
    dbc.Row(dbc.Col(dcc.Dropdown(
                id='available_flows',
                options=[{'label': i, 'value': i} for i in AVAILABLE_FLOWS],
                value=AVAILABLE_FLOWS[0]
            ), width=3)),
    dbc.Row(dbc.Col(html.H1(id='flow_name'))),
    dbc.Row(
            [
                dbc.Col(dcc.Graph(id='sankey'), width=11),
                dbc.Col([html.Label('Threshold'),
                        dcc.Slider(
                            id='threshold_slider',
                            min=0,
                            max=flow.sankey_max_links(),
                            value=10,
                            #marks={str(i): str(i) for i in range(flow.sankey_max_links()) if i % int(flow.sankey_max_links()/25) == 0},
                            step=10,
                            vertical=True
                        )])
            ]),
    dbc.Row([dbc.Col(html.H4(id='date_range'), width=3),
             dbc.Col(dcc.RangeSlider(
                id='date_slider',
                min=0,
                max=100,
                value=[0, 100],
                step=5,
            ), width=11)])

])

@app.callback(
    [Output('sankey', 'figure'),
     Output('threshold_slider', component_property='max'),
     Output('flow_name', 'children'),
     Output('date_range', 'children')],
    [Input('threshold_slider', 'value'),
     Input('available_flows', 'value'),
     Input('date_slider', 'value')])
def update_figure(threshold, flow_name, date_range):
    print(f"threshold: {threshold} flow_name: {flow_name} date_range: {date_range}")
    global flow
    try:
        if flow_name != flow._flow_name:
            raise Exception
        fig = flow.sankey_modify_threshold(threshold)
    except:
        print("Plot not found, starting compute...")
        flow = Flow(flow_name=flow_name)
        print("New plot computed")
    flow.start_date, flow.end_date = flow.date_at_percent(date_range[0]), flow.date_at_percent(date_range[1])
    date_range = f"Showing Sessions from {flow.start_date} to {flow.end_date}"
    fig = flow.sankey_plot()
    max = flow.sankey_max_links()
    return fig, max, flow_name, date_range


if __name__ == '__main__':
    app.run_server(debug=True)
