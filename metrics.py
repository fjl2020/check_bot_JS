import polars as pl

def get_initial_amount(df):
    ganancia_inicial = df.select(pl.col("Ganancia")).to_numpy()[0][0]

    return ganancia_inicial

def get_final_amount(df):
    ganancia_final = df['Ganancia'].sum()

    return ganancia_final
