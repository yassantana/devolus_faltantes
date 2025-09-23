import requests
import os
import time
import json
import csv
from datetime import datetime
import pyodbc

TOKEN = "3f57dca4-c52d-44c3-af71-0b932be8a4d3"
PASTA = r"C:\Users\yasmin\Downloads\devolus_torres"
LIMIT = 10  
URL_IMOVEIS = "https://api.devolusvistoria.com.br/api/imoveis"

SERVER = "131.100.24.30"
DATABASE = "DWHeaders"
USERNAME = "usr_api_torres"
PASSWORD = "ZadmXkJ4DSsUKE"
TABELA = "TB_API_TORRES_MELO_DEVOLUS"
BATCH_SIZE = 500

os.makedirs(PASTA, exist_ok=True)
ARQUIVO_CONTINUA = os.path.join(PASTA, "ultimo_offset.txt")

def salvar_offset(pagina):
    with open(ARQUIVO_CONTINUA, "w") as f:
        f.write(str(pagina))

def carregar_offset():
    if os.path.exists(ARQUIVO_CONTINUA):
        with open(ARQUIVO_CONTINUA, "r") as f:
            return int(f.read().strip())
    return 0

def pausa_contador(segundos):
    while segundos > 0:
        mins, secs = divmod(segundos, 60)
        timer = f'⏳ Pausa: {mins:02d}:{secs:02d}'
        print(timer, end='\r', flush=True)
        time.sleep(1)
        segundos -= 1
    print('⏳ Pausa encerrada! Continuando...      ')

def baixar_imoveis_ativos():
    pagina = carregar_offset()
    todos_imoveis_ativos = []
    duplicados_lista = []
    seen_hashes = set()
    duplicados_reais = 0
    requisicoes = 0

    while True:
        params = {"limit": LIMIT, "pagina": pagina}
        headers = {"Authorization": f"Bearer {TOKEN}"}

        try:
            r = requests.get(URL_IMOVEIS, headers=headers, params=params, timeout=30)

            if r.status_code == 429:
                retry_after = int(r.headers.get("Retry-After", "3600"))
                print(f"⚠ Limite atingido, aguardando {retry_after} segundos...")
                pausa_contador(retry_after)
                continue

            r.raise_for_status()
            imoveis = r.json()
            qtd_total = len(imoveis)
            print(f"📥 GET retornou {qtd_total} imóveis (página {pagina})")

            if not imoveis:
                print("✅ Todos os imóveis baixados")
                break

            for imovel in imoveis:
                if str(imovel.get("ativo")).strip().lower() == "true":
                    assinatura = (
                        str(imovel.get("codigoExterno")) + "|" +
                        str(imovel.get("endereco")) + "|" +
                        str(imovel.get("bairro")) + "|" +
                        str(imovel.get("cidade")) + "|" +
                        str(imovel.get("numero")) + "|" +
                        str(imovel.get("complemento")) + "|" +
                        str(imovel.get("tipoImovel")) + "|" +
                        str(imovel.get("metragem"))
                    )
                    imovel_hash = hash(assinatura)

                    if imovel_hash not in seen_hashes:
                        imovel["DATA_REGARGA"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        todos_imoveis_ativos.append(imovel)
                        seen_hashes.add(imovel_hash)
                    else:
                        duplicados_reais += 1
                        duplicados_lista.append(imovel)

            filepath = os.path.join(PASTA, f"pagina_{pagina}.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(imoveis, f, ensure_ascii=False, indent=2)

            print(f"[SALVO] Página {pagina} -> {qtd_total} imóveis (ativos filtrados)")

            pagina += 1
            salvar_offset(pagina)
            requisicoes += 1

            if requisicoes % 250 == 0:
                print("⚠ Pausa de 60 minutos atingida. Aguardando...")
                pausa_contador(3600)

            time.sleep(1)

        except requests.RequestException as e:
            print(f"❌ Erro na requisição: {e}")
            time.sleep(10)
            continue

    print(f"\n⚠ Total de duplicados reais: {duplicados_reais}")
    print(f"✅ Total de imóveis ativos processados: {len(todos_imoveis_ativos)}")
    return todos_imoveis_ativos, duplicados_lista

def json_para_csv(nome, imoveis):
    if not imoveis:
        print(f"⚠ Nenhum imóvel para gerar CSV de {nome}")
        return

    campos = set()
    for imovel in imoveis:
        campos.update(imovel.keys())
    campos = list(campos)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    caminho_csv = os.path.join(PASTA, f"{nome}_{timestamp}.csv")

    with open(caminho_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=campos)
        writer.writeheader()
        for imovel in imoveis:
            linha = {}
            for campo in campos:
                valor = imovel.get(campo)
                if isinstance(valor, (list, dict)):
                    valor = json.dumps(valor, ensure_ascii=False)
                linha[campo] = valor
            writer.writerow(linha)

    print(f"[CSV GERADO] {caminho_csv}")

def inserir_sql(imoveis):
    if not imoveis:
        print("⚠ Nenhum imóvel para inserir no SQL")
        return

    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}"
    )
    cursor = conn.cursor()

    total = len(imoveis)
    for i in range(0, total, BATCH_SIZE):
        batch = imoveis[i:i+BATCH_SIZE]
        placeholders = ",".join(["?"] * 14)
        query = f"""
        INSERT INTO {TABELA} 
        (BAIRRO, CEP, ENDERECO, ATIVO, COMPLEMENTO, NUMERO, ID, METRAGEM, CODIGO_MOBILE, UF, TIPO_IMOVEL, CODIGO_EXTERNO, CIDADE, DATA_REGARGA)
        VALUES ({placeholders})
        """
        valores = [
            (
                im.get("bairro"),
                im.get("cep"),
                im.get("endereco"),
                1 if im.get("ativo") else 0,
                im.get("complemento"),
                im.get("numero"),
                im.get("id"),
                im.get("metragem"),
                im.get("codigoMobile"),
                im.get("uf"),
                im.get("tipoImovel"),
                im.get("codigoExterno"),
                im.get("cidade"),
                im.get("DATA_REGARGA")
            )
            for im in batch
        ]
        cursor.executemany(query, valores)
        conn.commit()
        print(f"[SQL] Inseridos {len(batch)} registros no banco")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    if os.path.exists(ARQUIVO_CONTINUA):
        os.remove(ARQUIVO_CONTINUA)

    imoveis_ativos, duplicados_lista = baixar_imoveis_ativos()

    for im in imoveis_ativos:
        if "codigoExterno" in im and im["codigoExterno"]:
            im["codigoExterno"] = str(im["codigoExterno"]).zfill(4)

    for im in imoveis_ativos:
        im["endereco"] = im.get("endereco") or ""
        im["bairro"] = im.get("bairro") or ""
        im["cidade"] = im.get("cidade") or ""

    json_para_csv("todos_imoveis_ativos", imoveis_ativos)

    if duplicados_lista:
        json_para_csv("imoveis_duplicados", duplicados_lista)

    inserir_sql(imoveis_ativos)







