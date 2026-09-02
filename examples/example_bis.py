# Ejemplo de uso


from econ_api_bridge.bis import BIS_API

# Ejemplo de uso de la clase API_Banxico|
bis_api = BIS_API()
serie_id = ["Q.MX.P.A.M.770.A", "Q.US.P.A.M.770.A", "Q.CH.P.A.M.770.A"]

# Obtener datos de la serie desde 2023-01-01 hasta hoy
serie = bis_api.get_series_data(serie_id, last_data=True)

print(serie)
print("\n")

serie = bis_api.get_series_data(
    serie_id, start_date="2020-01-01", end_date="2024-12-31"
)
print(serie)
