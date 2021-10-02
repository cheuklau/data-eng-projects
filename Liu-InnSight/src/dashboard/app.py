# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import random
from time import gmtime, strftime
import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
color = 'rgb(245, 94, 97)'
month_mapping = {'1': 'Jan', '2': 'Feb', '3': 'Mar', '4': 'Apr', '5': 'May', '6': 'Jun', '7': 'Jul', '8': 'Aug',
                 '9': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'}

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(style={'font-family': 'monospace'}, children=[
    html.Div([
        html.H1(children='InnSight', style={'margin': 'auto', 'text-align': 'center', 'font-weight': 'bold'}),
        html.H5(children='Insights for Airbnb Hosts',
                style={'margin': 'auto', 'text-align': 'center', 'font-weight': 'bold'})
    ]),

    html.Br(),

    html.Div(style={'margin': 'auto', 'text-align': 'center'}, children=[
        html.Label('Please enter a zipcode: ', style={'display': 'inline', 'font-size': '130%'}),
        dcc.Input(id='input-box', type='text', style={'margin': '10px'}),
        html.Button('Submit', id='button'),
    ]),

    html.Div([
        html.Div([
            html.Div([
                html.Div([
                    dcc.Graph(
                        id='average-price'
                    )
                ], className='six columns'),
                html.Div([
                    dcc.Graph(
                        id='seasonality'
                    )
                ], className='six columns'),
            ], className='row'),
            html.Div([
                html.Div([
                    dcc.Graph(
                        id='property-type'
                    )
                ], className='six columns'),
                html.Div([
                    dcc.Graph(
                        id='room-type'
                    )
                ], className='six columns'),
            ], className='row')
        ], className='nine columns'),
        html.Div([
            html.P('Live local booking price', style={'font-size': '130%'}),
            dash_table.DataTable(
                id='price-event',
                columns=[{"name": "Time", "id": "time"}, {"name": "Price", "id": "price"}],
            )
        ], className='three columns'),
    ], className='row'),
])


@app.callback(
    dash.dependencies.Output('average-price', 'figure'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(_, value):
    return {'data': [get_average_price(value)],
            'layout': {'title': 'Historical Price Trend',
                       'paper_bgcolor': 'rgba(0,0,0,0)',
                       'plot_bgcolor': 'rgba(0,0,0,0)',
                       'font': {'family': 'monospace'}}}


@app.callback(
    dash.dependencies.Output('seasonality', 'figure'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(_, value):
    return {'data': [get_seasonality(value)],
            'layout': {'title': 'Seasonality',
                       'paper_bgcolor': 'rgba(0,0,0,0)',
                       'plot_bgcolor': 'rgba(0,0,0,0)',
                       'font': {'family': 'monospace'}}}


@app.callback(
    dash.dependencies.Output('property-type', 'figure'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(_, value):
    return {'data': [get_property_type(value)],
            'layout': {'title': 'Property Type',
                       'paper_bgcolor': 'rgba(0,0,0,0)',
                       'plot_bgcolor': 'rgba(0,0,0,0)',
                       'font': {'family': 'monospace'}}}


@app.callback(
    dash.dependencies.Output('room-type', 'figure'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(_, value):
    return {'data': [get_bedroom_type(value)],
            'layout': {'title': 'Room Type',
                       'paper_bgcolor': 'rgba(0,0,0,0)',
                       'plot_bgcolor': 'rgba(0,0,0,0)',
                       'font': {'family': 'monospace'}}}


@app.callback(
    dash.dependencies.Output('price-event', 'data'),
    [dash.dependencies.Input('button', 'n_clicks')],
    [dash.dependencies.State('input-box', 'value')])
def update_output(_, value):
    return get_price_event(value)


def read_data_from_db(database, sql):
    try:
        connection = psycopg2.connect(user=config['DEFAULT']['DB_USER'],
                                      password=config['DEFAULT']['DB_PASSWORD'],
                                      host=config['DEFAULT']['POSTGRESQL_IP'],
                                      port=config['DEFAULT']['POSTGRESQL_PORT'],
                                      database=database)
        cursor = connection.cursor()
        cursor.execute(sql)
        records = cursor.fetchall()

        return records
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        # closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def get_bedroom_type(zipcode):
    if zipcode is None:
        # return empty dict
        return {'x': [], 'y': [], 'type': 'bar', 'name': 'Price', 'marker': {'color': color}}

    rows = read_data_from_db('price_insight_db',
                             "select * from result_room_type_distribution_all where zipcode = '%s' order by bedrooms" % zipcode)

    dict_data = {'x': [], 'y': [], 'type': 'bar', 'name': 'Price', 'marker': {'color': color}}
    for row in rows:
        dict_data['x'].append(str(row[1]).split(' ')[0])
        dict_data['y'].append(str(row[2]))

    return dict_data


def get_property_type(zipcode):
    if zipcode is None:
        # return empty dict
        return {'x': [], 'y': [], 'type': 'bar', 'name': 'Price', 'marker': {'color': color}}

    rows = read_data_from_db('price_insight_db',
                             "select * from result_rental_type_distribution_all where zipcode = '%s' and count > 5" % zipcode)

    dict_data = {'x': [], 'y': [], 'type': 'bar', 'name': 'Price', 'marker': {'color': color}}
    for row in rows:
        dict_data['x'].append(str(row[1]).split(' ')[0])
        dict_data['y'].append(str(row[2]))

    return dict_data


def get_seasonality(zipcode):
    if zipcode is None:
        # return empty dict
        return {'x': [], 'y': [], 'type': 'bar', 'name': 'Price', 'marker': {'color': color}}

    rows = read_data_from_db('price_insight_db',
                             "select * from seasonality_all where zipcode = '%s' order by month" % zipcode)

    dict_data = {'x': [], 'y': [], 'type': 'bar', 'name': 'Price', 'marker': {'color': color}}
    for row in rows:
        dict_data['x'].append(month_mapping.get(str(row[1])))
        dict_data['y'].append(str(row[2]))

    return dict_data


def get_average_price(zipcode):
    if zipcode is None:
        # return empty dict
        return {'x': [], 'y': [], 'type': 'scatter', 'mode': 'lines', 'name': 'Price', 'marker': {'color': color}}

    rows = read_data_from_db('price_insight_db',
                             "select * from average_price_trend_all where zipcode = '%s' order by timestamp" % zipcode)

    x = []
    y = []
    for row in rows:
        x.append(str(row[1]).split(' ')[0])
        y.append(str(row[2]))

    return {'x': x, 'y': y, 'type': 'scatter', 'mode': 'lines', 'name': 'Price', 'marker': {'color': color}}


curtime = strftime("%Y-%m-%d %H:%M:%S", gmtime())


def get_price_event(zipcode):
    if zipcode is None:
        # return empty dict
        return []

    rows = read_data_from_db('price_insight_db',
                             "select timestamp, price from streaming_data where zipcode = '%s' order by timestamp "
                             "limit 10" % zipcode)

    data = []
    for row in rows:
        data.append({'time': str(row[0]).split(' ')[0], 'price': str(row[1])})

    return data


if __name__ == '__main__':
    app.run_server(host="0.0.0.0", port=80)
