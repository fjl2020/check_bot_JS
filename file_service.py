import polars as pl

def load_file_st(file):
    df = pl.read_excel("historico_trades_2024-12-15.xlsx", sheet_name="Sheet1")
    df = df.filter(pl.col("Ganancia") != 0)
    df = df.with_columns(
    pl.when(pl.col('Ganancia')>0).then(1).otherwise(0).alias('trade_positivo'),
    pl.when(pl.col('Ganancia')>0).then(pl.col('Ganancia')).otherwise(0).alias('ganancia_positiva'),
    pl.when(pl.col('Ganancia')<0).then(1).otherwise(0).alias('trade_negativo'),
    pl.when(pl.col('Ganancia')<0).then(pl.col('Ganancia')).otherwise(0).alias('ganancia_negativa'),)
    return df

def process_df(df):
    df = df[1:]
    df_statistics = df.group_by('Magic Number').agg(
        pl.col('trade_positivo').count().alias('trades_total'),
        pl.col('trade_positivo').sum().alias('trades_positivos'),
        pl.col('trade_negativo').sum().alias('trades_negativos'),
        pl.col('Ganancia').sum().alias('ganancia_total'),
        pl.col('ganancia_positiva').sum().alias('ganancia_positiva'),
        pl.col('ganancia_negativa').sum().alias('ganancia_negativa'),
        pl.col('Ganancia').mean().alias('avg_ganancia_total'),
        pl.col('ganancia_positiva').mean().alias('avg_ganancia_positiva'),
        pl.col('ganancia_negativa').mean().alias('avg_ganancia_negativa'),
        ).sort('Magic Number')
    df_statistics = df_statistics.with_columns(
        (pl.col('trades_positivos')/pl.col('trades_negativos')).alias('pf'),
        (pl.col('trades_positivos')/pl.col('trades_total')).alias('pct_win'),
        (pl.col('ganancia_positiva')/pl.col('ganancia_negativa')).alias('win_loss_ratio'),
    )   
    
    return df_statistics