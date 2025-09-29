import csv
import requests

arquivo_csv = r"C:\Users\yasmin\Downloads\teste_faltantes\torres_2025-09-29_08-30.csv"

url = "https://api.devolusvistoria.com.br/api/imoveis"
headers = {
    "Authorization": "Bearer 3f57dca4-c52d-44c3-af71-0b932be8a4d3",
    "Content-Type": "application/json"
}

imoveis_para_enviar = ['4965']

with open(arquivo_csv, newline="", encoding="utf-8") as csvfile:
    leitor = csv.DictReader(csvfile)
    for row in leitor:
        if row['codigo'] in imoveis_para_enviar:
            payload = {
                "codigoExterno": row.get('codigo', ''),
                "endereco": row.get('endereco', ''),
                "numero": row.get('numero', ''),
                "complemento": row.get('complemento', ''),
                "bairro": row.get('bairro', ''),
                "cidade": row.get('cidade', ''),
                "uf": row.get('uf', ''),
                "cep": row.get('cep', ''),
                "tipoImovel": row.get('tipo', ''),
                "ativo": True,  # Alteração feita aqui
                "metragem": int(float(row['area'].replace(',', '.'))) if row.get('area') else 0
            }

            print(f"Enviando imóvel {row['codigo']}...")
            response = requests.post(url, json=payload, headers=headers)

            if response.status_code in [200, 201]:
                print(f"✅ Imóvel {row['codigo']} inserido/atualizado com sucesso!")
            else:
                print(f"❌ Erro ao inserir/atualizar {row['codigo']}: {response.status_code} - {response.text}")