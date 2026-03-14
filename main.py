import csv
import re
import sqlite3
import requests
import logging
from datetime import datetime

CSV_FILE = "solicitacoes.csv"
API_URL = "http://api_mock:8000/solicitacoes"
DB_FILE = "processamento.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("processamento.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()


def criar_banco():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS processamento (
        id INT PRIMARY KEY,
        status TEXT,
        tentativas INTEGER,
        ultimo_erro TEXT,
        timestamp_processamento TEXT
    )
    """)
    conn.commit()
    conn.close()

def ja_processado(id):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM processamento WHERE id=?", (id,))
    result = cursor.fetchone()
    conn.close()
    return result is not None

def registrar_resultado(id, status, tentativas, erro=None):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT OR REPLACE INTO processamento
    (id, status, tentativas, ultimo_erro, timestamp_processamento)
    VALUES (?, ?, ?, ?, ?)
    """, (
        id, status, tentativas, erro, datetime.now().isoformat()
    ))
    conn.commit()
    conn.close()


def email_valido(email):
    if not email: return True
    regex = r"[^@]+@[^@]+\.[^@]+"
    return re.match(regex, email)

def telefone_valido(telefone):
    if not telefone: return True
    if not telefone.isdigit(): return False
    return len(telefone) >= 10

def data_valida(data):
    try:
        datetime.fromisoformat(data)
        return True
    except:
        return False

def validar(row):
    obrigatorios = ["id", "cliente", "tipo", "descricao", "created_at"]
    for campo in obrigatorios:
        if not row.get(campo):
            return False, f"campo obrigatório ausente: {campo}"

    if not email_valido(row.get("email")):
        return False, "email inválido"
    if not telefone_valido(row.get("telefone")):
        return False, "telefone inválido (somente números >=10)"
    if row["tipo"].lower() == "incidente":
        if not row.get("email") and not row.get("telefone"):
            return False, "incidente sem meio de contato"
    if not data_valida(row.get("created_at")):
        return False, "data inválida"

    return True, None


def classificar(row):
    tipo = row["tipo"].lower()
    queues = {
        "incidente": "N1-INCIDENTES",
        "suporte": "N1-SUPORTE",
        "financeiro": "BACKOFFICE-FIN",
        "melhoria": "PRODUTO"
    }
    prioridade_default = {
        "incidente": "alta", "financeiro": "media",
        "suporte": "baixa", "melhoria": "baixa"
    }
    queue = queues.get(tipo, "GERAL")
    prioridade = row.get("prioridade")
    prioridade_final = prioridade.lower() if prioridade else prioridade_default.get(tipo, "baixa")
    
    return queue, prioridade_final

def enviar_api(payload):
    tentativas = 0
    erro = None
    while tentativas < 3:
        tentativas += 1
        try:
            response = requests.post(API_URL, json=payload, timeout=5)
            if response.status_code == 200:
                return True, tentativas, None
            if response.status_code == 400:
                return False, tentativas, f"API erro 400: {response.text}"
            erro = f"API erro {response.status_code}"
        except Exception as e:
            erro = str(e)
    return False, tentativas, erro


def processar():
    logger.info("iniciando processamento do arquivo")
    criar_banco()

    stats = {
        "sucesso": 0,
        "erros_validacao": 0,
        "erros_envio": 0,
        "ja_processados": 0  
    }

    try:
        with open(CSV_FILE, newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                id = row["id"]

                if ja_processado(id):
                    logger.info(f"id={id} | já processado anteriormente")
                    stats["ja_processados"] += 1
                    continue

                logger.info(f"id={id} | iniciando validação")
                valido, erro = validar(row)

                if not valido:
                    logger.error(f"id={id} | status=erro_validacao | erro={erro}")
                    registrar_resultado(id, "erro_validacao", 1, erro)
                    stats["erros_validacao"] += 1
                    continue

                queue, priority = classificar(row)
                payload = {
                    "id": id,
                    "cliente": row["cliente"],
                    "tipo": row["tipo"],
                    "queue": queue,
                    "priority_final": priority,
                    "descricao": row["descricao"],
                    "contato": {"email": row["email"], "telefone": row["telefone"]},
                    "created_at": row["created_at"]
                }

                logger.info(f"id={id} | enviando para API")
                sucesso, tentativas, erro = enviar_api(payload)

                if sucesso:
                    logger.info(f"id={id} | status=sucesso | tentativas={tentativas}")
                    registrar_resultado(id, "sucesso", tentativas)
                    stats["sucesso"] += 1
                else:
                    logger.error(f"id={id} | status=erro_envio | erro={erro}")
                    registrar_resultado(id, "erro_envio", tentativas, erro)
                    stats["erros_envio"] += 1
    except FileNotFoundError:
        logger.error(f"Arquivo {CSV_FILE} não encontrado.")
        return None

    return stats

if __name__ == "__main__":
    res = processar()

    if res:
        novos_processados = res["sucesso"] + res["erros_validacao"] + res["erros_envio"]

        if novos_processados == 0 and res["ja_processados"] > 0:
            print("\n" + "="*45)
            print("INFO: Todos os arquivos já foram reprocessados.")
            print("="*45)
        else:
            print("\nResumo do processamento")
            print("-----------------------")
            print(f"Novos registros: {novos_processados}")
            print(f"Sucesso: {res['sucesso']}")
            print(f"Erros de validação: {res['erros_validacao']}")
            print(f"Erros de envio: {res['erros_envio']}")
            print(f"Ignorados (já processados): {res['ja_processados']}")