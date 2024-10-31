import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
from sqlalchemy import create_engine
import redis

class DataQualityDashboard:
    def __init__(self, db_connection: str):
        self.app = dash.Dash(__name__)
        self.engine = create_engine(db_connection)
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
        self.app.layout = self.create_layout()
        
    def create_layout(self):
        return html.Div([
            html.H1("Market Data Quality Dashboard"),
            
            dcc.Tabs([
                dcc.Tab(label='Real-Time Metrics', children=[
                    dcc.Graph(id='data-freshness-graph'),
                    dcc.Graph(id='data-volume-graph'),
                    dcc.Interval(
                        id='interval-component',
                        interval=30*1000,  # 30 seconds
                        n_intervals=0
                    )
                ]),
                
                dcc.Tab(label='Data Quality Metrics', children=[
                    dcc.Graph(id='completeness-graph'),
                    dcc.Graph(id='accuracy-graph')
                ]),
                
                dcc.Tab(label='Anomaly Detection', children=[
                    dcc.Graph(id='anomaly-graph'),
                    dcc.Graph(id='pattern-graph')
                ])
            ])
        ])
    
    def get_data_quality_metrics(self):
        query = """
        SELECT 
            symbol,
            DATE_TRUNC('hour', timestamp) as hour,
            COUNT(*) as record_count,
            COUNT(*) FILTER (WHERE price > 0) as valid_price_count,
            COUNT(*) FILTER (WHERE volume > 0) as valid_volume_count,
            AVG(price) as avg_price,
            AVG(volume) as avg_volume
        FROM market_data
        GROUP BY symbol, hour
        ORDER BY hour DESC
        LIMIT 1000;
        """
        return pd.read_sql(query, self.engine)

    def update_freshness_graph(self, n_intervals):
        df = self.get_data_quality_metrics()
        
        return {
            'data': [
                go.Scatter(
                    x=df[df['symbol'] == symbol]['hour'],
                    y=df[df['symbol'] == symbol]['record_count'],
                    name=symbol,
                    mode='lines+markers'
                ) for symbol in df['symbol'].unique()
            ],
            'layout': go.Layout(
                title='Data Freshness by Symbol',
                xaxis={'title': 'Time'},
                yaxis={'title': 'Record Count'},
                hovermode='closest'
            )
        }

    def run_dashboard(self, host='0.0.0.0', port=8050):
        self.app.run_server(host=host, port=port, debug=True)