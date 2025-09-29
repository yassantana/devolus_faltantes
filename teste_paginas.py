import requests
import json
import time

TOKEN = "3f57dca4-c52d-44c3-af71-0b932be8a4d3"
URL_IMOVEIS = "https://api.devolusvistoria.com.br/api/imoveis"
LIMIT = 10 

def buscar_pagina(pagina):
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {"limit": LIMIT, "pagina": pagina}
    r = requests.get(URL_IMOVEIS, headers=headers, params=params, timeout=30)

    if r.status_code == 429:
        retry_after = int(r.headers.get("Retry-After", "60"))
        print(f"⚠ Limite atingido, esperar {retry_after} segundos...")
        time.sleep(retry_after)
        return []

    r.raise_for_status()
    return r.json()

def eh_ativo(ativo):
    return ativo in [True, "true", "True", 1, "1"]

if __name__ == "__main__":
    for pagina in range(75, 85):  # só algumas páginas de teste
        imoveis = buscar_pagina(pagina)
        print(f"\n📄 Página {pagina} retornou {len(imoveis)} imóveis")

        for imovel in imoveis:
            print(f" - ID {imovel.get('id')} | Codigo {imovel.get('codigoExterno')} | Ativo={imovel.get('ativo')}")
            if eh_ativo(imovel.get("ativo")):
                print("   ✅ DETECTADO como ativo!")
            else:
                print("   ❌ NÃO considerado ativo")
