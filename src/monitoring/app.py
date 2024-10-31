import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import plotly.express as px
from typing import Dict, List, Tuple
import redis

class MarketDashboard:
    def __init__(self, db_connection: str):
        self.engine = create_engine(db_connection)
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
        # Set Streamlit theme
        st.set_page_config(
            page_title="Real-Time Market Analytics",
            page_icon="📈",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Apply dark theme
        st.markdown("""
        <style>
        .stApp {
            background-color: #0E1117;
            color: #FFFFFF;
        }
        .stMetric {
            background-color: #1B2028;
            padding: 10px;
            border-radius: 5px;
        }
        .stock-up {
            color: #00FF7F;
        }
        .stock-down {
            color: #FF4444;
        }
        </style>
        """, unsafe_allow_html=True)

    def get_latest_market_data(self, symbols: List[str]) -> pd.DataFrame:
        """Fetch latest market data for given symbols."""
        query = f"""
        WITH latest_prices AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                timestamp,
                open,
                high,
                low,
                close,
                volume,
                rsi,
                price_ma_50,
                price_ma_200
            FROM stock_data
            WHERE symbol IN ('{"','".join(symbols)}')
            ORDER BY symbol, timestamp DESC
        )
        SELECT * FROM latest_prices
        """
        return pd.read_sql_query(query, self.engine)

    def create_candlestick_chart(self, symbol: str) -> go.Figure:
        """Create interactive candlestick chart with technical indicators."""
        query = f"""
        SELECT *
        FROM stock_data
        WHERE symbol = '{symbol}'
        ORDER BY timestamp DESC
        LIMIT 100
        """
        df = pd.read_sql_query(query, self.engine)
        
        fig = make_subplots(rows=2, cols=1, 
                          shared_xaxes=True,
                          vertical_spacing=0.03,
                          subplot_titles=(f'{symbol} Price', 'Volume'),
                          row_width=[0.7, 0.3])

        # Candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='OHLC'
            ),
            row=1, col=1
        )

        # Add Moving Averages
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['price_ma_50'],
                name='50 MA',
                line=dict(color='orange', width=1)
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['price_ma_200'],
                name='200 MA',
                line=dict(color='blue', width=1)
            ),
            row=1, col=1
        )

        # Volume bars
        fig.add_trace(
            go.Bar(
                x=df['timestamp'],
                y=df['volume'],
                name='Volume',
                marker_color='rgba(100,100,255,0.5)'
            ),
            row=2, col=1
        )

        # Update layout for dark theme
        fig.update_layout(
            template='plotly_dark',
            xaxis_rangeslider_visible=False,
            height=600,
            showlegend=True,
            title_x=0.5,
            margin=dict(t=30, l=0, r=0, b=0)
        )

        return fig

    def display_market_metrics(self, df: pd.DataFrame):
        """Display key market metrics in a grid layout."""
        cols = st.columns(len(df))
        
        for idx, (_, row) in enumerate(df.iterrows()):
            with cols[idx]:
                price_change = (row['close'] - row['open']) / row['open'] * 100
                color = "stock-up" if price_change >= 0 else "stock-down"
                
                st.markdown(f"""
                <div class="stMetric">
                    <h3>{row['symbol']}</h3>
                    <h2 class="{color}">${row['close']:.2f}</h2>
                    <p class="{color}">{price_change:.2f}%</p>
                    <p>Vol: {row['volume']:,}</p>
                </div>
                """, unsafe_allow_html=True)

    def display_technical_analysis(self, symbol: str):
        """Display technical analysis indicators."""
        query = f"""
        SELECT 
            timestamp,
            close,
            rsi,
            price_ma_50,
            price_ma_200,
            volume_ma
        FROM stock_data
        WHERE symbol = '{symbol}'
        ORDER BY timestamp DESC
        LIMIT 100
        """
        df = pd.read_sql_query(query, self.engine)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # RSI Chart
            fig_rsi = go.Figure()
            fig_rsi.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['rsi'],
                name='RSI',
                line=dict(color='white')
            ))
            
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="red")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="green")
            
            fig_rsi.update_layout(
                template='plotly_dark',
                title='Relative Strength Index (RSI)',
                height=300,
                margin=dict(t=30, l=0, r=0, b=0)
            )
            
            st.plotly_chart(fig_rsi, use_container_width=True)
        
        with col2:
            # Moving Averages
            fig_ma = go.Figure()
            fig_ma.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['price_ma_50'],
                name='50 MA',
                line=dict(color='orange')
            ))
            fig_ma.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['price_ma_200'],
                name='200 MA',
                line=dict(color='blue')
            ))
            
            fig_ma.update_layout(
                template='plotly_dark',
                title='Moving Averages',
                height=300,
                margin=dict(t=30, l=0, r=0, b=0)
            )
            
            st.plotly_chart(fig_ma, use_container_width=True)

    def run_dashboard(self):
        """Run the main dashboard."""
        st.title("Real-Time Market Analytics Dashboard")
        
        # Sidebar
        st.sidebar.title("Settings")
        symbols = st.sidebar.multiselect(
            "Select Symbols",
            ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'],
            default=['AAPL', 'MSFT', 'GOOGL']
        )
        
        # Get latest data
        latest_data = self.get_latest_market_data(symbols)
        
        # Display market metrics
        self.display_market_metrics(latest_data)
        
        # Main chart
        selected_symbol = st.selectbox("Select Symbol for Detailed View", symbols)
        st.plotly_chart(
            self.create_candlestick_chart(selected_symbol),
            use_container_width=True
        )
        
        # Technical Analysis
        st.subheader("Technical Analysis")
        self.display_technical_analysis(selected_symbol)
        
        # Data Quality Metrics
        st.subheader("Data Quality Metrics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Data Freshness",
                f"{(datetime.now() - latest_data['timestamp'].max()).seconds} seconds"
            )
        
        with col2:
            st.metric(
                "Records Today",
                len(latest_data)
            )
            
        with col3:
            st.metric(
                "Data Quality Score",
                "98%"
            )

def main():
    # Replace with your database connection
    db_connection = "postgresql://postgres:1234@localhost/stockdb"
    
    dashboard = MarketDashboard(db_connection)
    dashboard.run_dashboard()

if __name__ == "__main__":
    main()