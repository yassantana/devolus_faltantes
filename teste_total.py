import requests
import time
import csv

url = "https://api.devolusvistoria.com.br/api/imoveis"
headers = {
    "Authorization": "Bearer 3f57dca4-c52d-44c3-af71-0b932be8a4d3"
}

todos_imoveis = []
pagina = 0
limite = 10
max_requisicoes = 250  # limite por hora
contador_requisicoes = 0

print("🔄 Iniciando captura de imóveis da API...")

while True:
    params = {"pagina": pagina, "limite": limite}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        imoveis = data if isinstance(data, list) else data.get("data", [])

        if not imoveis:
            print("✅ Nenhum imóvel a mais para buscar. Fim da coleta.")
            break

        for imovel in imoveis:
            if imovel.get("ativo"):
             todos_imoveis.append({
                "codigoExterno": imovel.get("codigoExterno"),
                "endereco": imovel.get("endereco"),
                "numero": imovel.get("numero"),
                "complemento": imovel.get("complemento"),
                "bairro": imovel.get("bairro"),
                "cidade": imovel.get("cidade"),
                "uf": imovel.get("uf"),
                "cep": imovel.get("cep"),
                "tipoImovel": imovel.get("tipoImovel"),
                "metragem": imovel.get("metragem"),
                "status": "Ativo" if imovel.get("ativo") else "Inativo"
            })

        print(f"📄 Página {pagina} capturada ({len(imoveis)} imóveis).")
        pagina += 1
        contador_requisicoes += 1

        # pausa se chegou no limite de requisições
        if contador_requisicoes >= max_requisicoes:
            print("⏸️ Limite de 250 requisições atingido. Pausando por 1 hora...")
            time.sleep(3600)
            contador_requisicoes = 0

        time.sleep(1)

    elif response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 3600))
        print(f"⚠ Erro 429: Limite atingido. Aguardando {retry_after} segundos...")
        time.sleep(retry_after)
        continue

    else:
        print(f"❌ Erro {response.status_code} na página {pagina}: {response.text}")
        break

# salvar em CSV
arquivo_csv = "imoveis_completos.csv"
with open(arquivo_csv, "w", newline="", encoding="utf-8") as csvfile:
    colunas = ["codigoExterno", "endereco", "numero", "complemento", "bairro", "cidade", "uf", "cep", "tipoImovel", "metragem", "status"]
    writer = csv.DictWriter(csvfile, fieldnames=colunas)
    writer.writeheader()
    writer.writerows(todos_imoveis)

print(f"\n✅ Finalizado! {len(todos_imoveis)} imóveis salvos em '{arquivo_csv}'.")


