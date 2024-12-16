import plotly.express as px
from plotly.subplots import make_subplots
from plotly.graph_objs import Line,Bar,Scatter,Layout,Figure
from datetime import datetime as dt
import  polars as pl



def profit_factor(df) -> Figure:
    # Asegúrate de que 'Magic Number' sea tratado como categoría
    df = df.with_columns(bot_number="bot_"+pl.col("Magic Number").cast(pl.String))
    print(df)
    # Crear la figura de barras
    x  = df["Magic Number"].to_list()
    
    fig = px.bar(
         df,
        x="bot_number",
        y="pf",
        labels={'bot_number': "Magic Number", 'pf': 'Profit factor'},
      
    )
    fig.update_xaxes(categoryorder='array', categoryarray= x)
    return fig
def pct_win(df) -> Figure:
    # Asegúrate de que 'Magic Number' sea tratado como categoría
    df = df.with_columns(bot_number="bot_"+pl.col("Magic Number").cast(pl.String))
    print(df)
    # Crear la figura de barras
    x  = df["Magic Number"].to_list()
    
    fig = px.bar(
         df,
        x="bot_number",
        y="pct_win",
        labels={'bot_number': "Magic Number", 'pct_win': '% win'},
      
    )
    fig.update_xaxes(categoryorder='array', categoryarray= x)
    return fig
    
    # fig.update_yaxes(title={'text':'Frec'})
    # 
# 