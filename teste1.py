import requests

TOKEN = "3f57dca4-c52d-44c3-af71-0b932be8a4d3"
URL = "https://api.devolusvistoria.com.br/api/imoveis"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}

codigo_procurado = "1609"
pagina = 0
encontrado = False

while not encontrado:
    params = {"limit": 50, "pagina": pagina}
    resp = requests.get(URL, headers=HEADERS, params=params)
    
    if resp.status_code != 200:
        print(f"Erro {resp.status_code}: {resp.text}")
        break

    imoveis = resp.json()
    
    if not imoveis:
        print("🚫 Código não encontrado em nenhuma página.")
        break

    for imovel in imoveis:
        if str(imovel.get("codigoExterno")).zfill(4) == codigo_procurado.zfill(4):
            print(f"✅ Imóvel encontrado na página {pagina}:")
            print(imovel)
            encontrado = True
            break

    pagina += 1

