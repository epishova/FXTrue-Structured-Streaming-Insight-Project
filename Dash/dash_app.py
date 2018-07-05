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
cassandra_host_names = ['ec2-52-23-103-178.compute-1.amazonaws.com', 'ec2-52-2-16-225.compute-1.amazonaws.com', 'ec2-34-192-194-39.compute-1.amazonaws.com']
keyspace = 'fx'
cluster = Cluster(cassandra_host_names)
session = cluster.connect(keyspace)
session.row_factory = pandas_factory
session.default_fetch_size = None
print('Connected to Cassandra')

# Define bind statements and variables:
time_delta = 0 #plot FX rates for this number of days
bind_marker = 'USD/JPY'
bind_date_from = (datetime.datetime.now() - datetime.timedelta(days = time_delta)).strftime('%Y-%m-%d')
bind_date_to = datetime.datetime.now().strftime('%Y-%m-%d')
bind_limit = 100 #plot this number of points
fx_rates_prep = session.prepare("select timestamp_ms, bid_points from fx_rates where fx_marker = ? and timestamp_d in (?, ?) limit ?")
fx_avg_prep = session.prepare("select window_start, bid_points_avg from fx_rates_avg where fx_marker = ? and timestamp_d in (?, ?) limit ?")
ad_simp_count_prep = session.prepare("select window_start, window_end from anomaly_by_simple_count where fx_marker = ? and timestamp_d in (?, ?) limit 10")

def update_input():
    """Select from Cassandra new FX rates and averages to plot in real time"""
    rows_fx_rates = session.execute(fx_rates_prep, [bind_marker, bind_date_from, bind_date_to, bind_limit])
    rows_fx_avg = session.execute(fx_avg_prep, [bind_marker, bind_date_from, bind_date_to, bind_limit])
    return (rows_fx_rates._current_rows, rows_fx_avg._current_rows)

def update_ad_input():
    """Select from Cassandra last detected anomalies"""
    rows_ad_simp_count = session.execute(ad_simp_count_prep, [bind_marker, bind_date_from, bind_date_to])
    return (rows_ad_simp_count._current_rows)

# Define the body of the web page
app.layout = html.Div(children = [
    html.H1(children='FXTrue'),

    html.Div(children='Please note that there is no FX trade during the weakends'),

    html.H4(children = 'Real-time bid points and trends'),

    dcc.Graph(id = 'fx_graph'),
    
    html.Div(id = 'ad_simcount_table'),

    dcc.Interval(
        id = 'graph_update',
        interval = 1 * 1000
        #n_intervals = 0
    ),
])

def generate_table(dataframe, max_rows = 10):
    """Generate HTML table containing detected anomalies"""
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))], style = {'backgroundColor': '#ffc0cb'}
    )

@app.callback(Output('ad_simcount_table', 'children'), events=[Event('graph_update', 'interval')])
def update_ad_table():
    """ Update HTML table containing anomalies"""
    df_ad = update_ad_input()
    df_ad.drop('window_end', axis = 1, inplace = True)
    df_ad.rename({"window_start": "Time when anomaly was detected"}, axis = 'columns', inplace = True)
    return generate_table(df_ad)

@app.callback(Output('fx_graph', 'figure'), events=[Event('graph_update', 'interval')])
def update_graph_scatter():
    """ Update plot with FX rates and averages"""
    df, df_avg  = update_input()
    df_ad = update_ad_input()
    df_ad = df_ad.loc[df_ad['window_start'] >= df['timestamp_ms'].min()]
    df_avg = df_avg.loc[df_avg['window_start'] >= df['timestamp_ms'].min()]

    # Define new data for the plot
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

    # Plot regions marked as anomaly
    shapes = []
    for index, anomaly in df_ad.iterrows():
        shape = {
            'type': 'rect',
            'xref': 'x',
            'yref': 'paper',
            'x0': anomaly["window_start"],
            'y0': 0,
            'x1': anomaly["window_end"],
            'y1': 1,
            'fillcolor': '#ff545e',
            'opacity': 0.2,
            'line': {
                'width': 0,
                }            
        }
        shapes.append(shape)

    layout = {}
    if len(shapes) == 0:
        layout = go.Layout(
            xaxis = { 'autorange': True, 'title': 'Timestamp'},
            yaxis = { 'autorange': True, 'title': 'Bid Points' },
            legend = { 'x': 0, 'y': 1 },
        )
    else:
        layout = go.Layout(
            shapes = shapes,
            xaxis = { 'autorange': True, 'title': 'Timestamp'},
            yaxis = { 'autorange': True, 'title': 'Bid Points' },
            legend = { 'x': 0, 'y': 1 },
        )        

    return {'data': [data_fx, data_avg], #, data_ad], 
            'layout' : layout
           }

if __name__ == '__main__':
    # Run main application
    app.run_server(debug = True, host = '0.0.0.0')
