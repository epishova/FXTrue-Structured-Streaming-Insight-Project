import dash
from dash.dependencies import Output, Input, Event
import dash_core_components as dcc
import dash_html_components as html
import plotly
import plotly.graph_objs as go
import datetime
import cassandra
from cassandra.cluster import Cluster
import pandas as pd

app = dash.Dash('fxtrue')

# Define pandas row factory for fast row retrivals into pandas dataframe:
def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns = colnames)

# Define configs to connect to cassandra:
cass_servers = ['127.0.0.1']
keyspace = 'fx'
cluster = Cluster(cass_servers)
session = cluster.connect(keyspace)
session.row_factory = pandas_factory
session.default_fetch_size = None
print('Connected to Cassandra')

# Define bind statements and variables:
time_delta = 0 #plot FX rates for this number of days
bind_marker = 'USD/JPY'
bind_date_from = (datetime.datetime.now() - datetime.timedelta(days = time_delta)).strftime('%Y-%m-%d')
bind_date_to = datetime.datetime.now().strftime('%Y-%m-%d')
bind_limit = 100 #upper bound for expected number of fx rates during two days per marker
fx_rates_prep = session.prepare("select timestamp_ms, bid_points from fx_rates where fx_marker = ? and timestamp_d in (?, ?) limit ?")
fx_avg_prep = session.prepare("select window_start, bid_points_avg from fx_rates_avg where fx_marker = ? and timestamp_d in (?, ?) limit ?")

def update_input():
    rows_fx_rates = session.execute(fx_rates_prep, [bind_marker, bind_date_from, bind_date_to, bind_limit])
    rows_fx_avg = session.execute(fx_avg_prep, [bind_marker, bind_date_from, bind_date_to, bind_limit])
    return (rows_fx_rates._current_rows, rows_fx_avg._current_rows)

app.layout = html.Div(children = [
    html.H1(children='FXTrue'),

    html.Div(children='Please note that there is no FX trade during the weakends'),

    html.H3(children = 'Real-time currency exchange rates and trends:'),

    dcc.Graph(id = 'fx_graph'),
    
    html.H3(children = 'Anomaly detection in rates of EUR, JPY, USD:'),

    dcc.Graph(id = 'anomaly_graph'),

    dcc.Interval(
        id = 'graph_update',
        interval = 1 * 1000
        #n_intervals = 0
    ),
])

#flag = True
#df, df_avg = update_input()

@app.callback(Output('fx_graph', 'figure'), events=[Event('graph_update', 'interval')])
def update_graph_scatter():
    df, df_avg = update_input()
    df_avg = df_avg.loc[df_avg['window_start'] >= df['timestamp_ms'].min()]
    #global flag
    #global df
    #global df_avg
    #if flag:
    #    df, df_avg = update_input()
    #    flag = False
    #else:
    #    df['timestamp_ms'] = df['timestamp_ms'] + datetime.timedelta(seconds = 1) 
    #    df['bid_big'] = df['bid_big'] + 0.1
    #print(df)
    #print(df_avg)
    #X.append(X[-1]+1)
    #Y.append(Y[-1]+Y[-1]*random.uniform(-0.1,0.1))

    data_fx = go.Scatter(
        x = df['timestamp_ms'],
        y = df['bid_points'],
        name = 'Real-time ' + bind_marker,
        mode =  'lines+markers'
    )
    data_avg = go.Scatter(
        x = df_avg['window_start'],
        y = df_avg['bid_points_avg'],
        name = 'Running AVG ' + bind_marker,
        mode =  'lines+markers'
    )
    return {'data': [data_fx, data_avg], 
            'layout' : go.Layout(
                xaxis = { 'autorange': True, 'title': 'Timestamp'}, 
#xaxis = { 'range': [min(df['timestamp_ms'].min(), df_avg['window_start'].min()), df['timestamp_ms'].max()], 'title': 'Timestamp'},
#xaxis = { 'range': [df['timestamp_ms'].min(), df['timestamp_ms'].max()], 'title': 'Timestamp'},
                yaxis = { 'autorange': True, 'title': 'Rates' },
#yaxis = { 'range': [df['bid_big'].min(), df['bid_big'].max()], 'title': 'Rates' },
                legend = { 'x': 0, 'y': 1 },
            )
           }

#@app.callback(Output('fx_graph', 'figure'),
#              events = [Event('graph_update', 'interval')])
#def update_graph_scatter():
#    df = update_input()
    #X.append(X[-1]+1)
    #Y.append(Y[-1]+Y[-1]*random.uniform(-0.1,0.1))

#    data = go.Scatter(
#            x = df['timestamp_ms'],
#            y = df['bid_big'],
#            name = bind_marker,
#            mode =  'lines+markers'
#            )
#
#    return {'data': [data],'layout' : go.Layout(
#
#xaxis = dict(range = [min(X), max(X)]),
#                                                yaxis = dict(range = [min(Y), max(Y)]),)}

if __name__ == '__main__':
    app.run_server(debug = True, host = '0.0.0.0')
