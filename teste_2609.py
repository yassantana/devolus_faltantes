import pandas as pd

# CSV com todos os imóveis retornados da API
api_csv = "imoveis_completos.csv"

# CSV com os códigos de imóveis válidos do site (ou lista manual)
site_csv = "imoveis_site.csv"  # deve ter uma coluna 'codigoExterno'

# Ler os CSVs
api_df = pd.read_csv(api_csv, dtype=str).fillna("")
site_df = pd.read_csv(site_csv, dtype=str).fillna("")

# Normalizar códigos
api_df["codigoExterno"] = api_df["codigoExterno"].str.strip()
site_df["codigoExterno"] = site_df["codigoExterno"].str.strip()

# Filtrar imóveis que estão na API, mas não no site
extras_df = api_df[~api_df["codigoExterno"].isin(site_df["codigoExterno"])]

# Salvar os imóveis “extras”
extras_csv = "imoveis_extras.csv"
extras_df.to_csv(extras_csv, index=False, encoding="utf-8")

print(f"✅ Encontrados {len(extras_df)} imóveis extras da API. Salvos em '{extras_csv}'.")

