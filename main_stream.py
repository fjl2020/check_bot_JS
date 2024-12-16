import streamlit as st
from datetime import datetime as dt
import polars as pl
import file_service as fscv
import visualization as vs
import metrics

st.set_page_config(layout="wide")

st.header   ("Resumen de métricas de bots")

def date_to_datetime(_date):
    return dt.combine(_date,dt.min.time())
with st.sidebar:
    with st.container(border=True):
        st.subheader('Parametros iniciales')
        
        fileOpen = st.file_uploader("Seleccione xlsx ",accept_multiple_files=False,type =['xlsx'])
        
        st.session_state.archivo_cargado  = False

        if fileOpen:
            try:
                df=fscv.load_file_st(fileOpen)
                df_statistics = fscv.process_df(df)

            except :
                st.error("Error al cargar el archivo")
            
            

        # submitted = st.button('Submit',disabled=not st.session_state.archivo_cargado)
    
        # if submitted:
        #     operations_df=operations_df.with_columns(pl.when((pl.col('OpenTime')>=start_date_oos) & (pl.col('OpenTime')<=end_date_oos))
        #             .then(pl.lit("OOS")).otherwise(pl.lit("IS")).alias("Type_sample"))
            
        
        
if fileOpen :
    st.dataframe(df_statistics['Magic Number','trades_total','trades_positivos',
                               'trades_negativos','ganancia_total','ganancia_positiva',
                               'avg_ganancia_total','avg_ganancia_positiva','pf','pct_win','win_loss_ratio'])
    
    st.dataframe(df_statistics.filter(pl.col('trades_total')>10))
    st.subheader('Profit factor')
    fig = vs.profit_factor(df_statistics.filter(pl.col('trades_total')>10))
    st.plotly_chart(fig)
    st.subheader('% win')
    fig = vs.pct_win(df_statistics.filter(pl.col('trades_total')>10))
    st.plotly_chart(fig)
# tab1, tab2, tab3,tab4 = st.tabs(["Datos", "Métricas", "Distribución de retornos","Distribución temporal"])
            
# with tab1:
#     st.header('Datos')
#     if submitted and  operations_df is not None and operations_df.shape[0]>1:
            
#             st.dataframe(operations_df)
#             fig = vs.fig_main_drawdown(operations_df) 
            
#             st.plotly_chart(fig)


            
# with tab2:
#     st.header('Métricas')
#     if submitted and operations_df is not None and operations_df.shape[0]>1:
#             st.subheader("General")
#             st.dataframe(metrics.metrics(operations_df))
#             st.subheader("Strategy")
#             st.dataframe(metrics.st_metrics(operations_df))
#             st.subheader("Trades")
#             st.dataframe(metrics.trades_metrics(operations_df))
#             st.subheader("Returns years by month")
#             st.dataframe(metrics.yield_by_month_day(operations_df))

    
# with tab3:
#     st.header('Distribución de retornos')
#     if submitted and operations_df is not None and operations_df.shape[0]>1:
#         fig = vs.dist_returns(operations_df)
#         st.plotly_chart(fig)
#         col1,col2,col3,col4 = st.columns(4)
#         is_sample,oos_sample=metrics.nb_samples(operations_df)
#         alpha = 0.05
#         kstest,p_value = metrics.ks_test(operations_df)
#         with col1:
            
#             st.metric(label="samples IS",value=is_sample)
        
#         with col2:
#             st.metric(label="samples OOS",value=oos_sample)
        
#         with col3:
            
#             st.metric(label="K statistic",value=f'{kstest:.4f}')
           
#         with col4:
#             st.metric(label="p_value",value=f'{p_value:.5f}')
#         st.divider() 
#         if p_value<alpha:
#             st.html('<h3>No se cumple la hipótesis nula, las distribuciones de datos no son similares</h3>')
#         else:
#             st.html('<h3>Se cumple la hipótesis nula, las distribuciones de datos son similares</h3>')
        
# with tab4:
#     st.header('Distribución termporales')
#     # st.dataframe(vs.weekday_mean_profit(operations_df))
#     if submitted and  operations_df is not None and operations_df.shape[0]>1:


#         fig= vs.fig_profit_weekday(operations_df)
#         st.plotly_chart(fig)
#         fig= vs.fig_profit_month(operations_df)
#         st.plotly_chart(fig)
#         fig= vs.fig_profit_day(operations_df)
#         st.plotly_chart(fig)

            