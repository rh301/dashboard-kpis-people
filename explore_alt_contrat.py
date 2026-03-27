"""Explora tabelas do pipe 305643176 (alteracoes contratuais)."""
import csv, io, json, os, sys, requests

NEKT_API_URL = "https://api.nekt.ai/api/v1/sql-query/"
NEKT_API_KEY = os.environ.get("NEKT_API_KEY", "")

def nekt_query(sql):
    resp = requests.post(NEKT_API_URL, json={"sql": sql, "mode": "csv"},
                         headers={"x-api-key": NEKT_API_KEY, "Content-Type": "application/json"},
                         timeout=120)
    resp.raise_for_status()
    body = resp.json()
    if body.get("state") != "SUCCEEDED":
        raise RuntimeError(f"Query failed: {body.get('state_change_reason', body)}")
    urls = body.get("presigned_urls", [])
    if not urls:
        return []
    rows = []
    for url in urls:
        csv_resp = requests.get(url, timeout=60)
        csv_resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(csv_resp.text))
        rows.extend(list(reader))
    return rows

# 1. Buscar TODAS as tabelas com 305643176
print("=== TABELAS COM 305643176 ===")
try:
    rows = nekt_query("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name LIKE '%305643176%'
        ORDER BY table_schema, table_name
    """)
    for r in rows:
        print(f"  {r['table_schema']}.{r['table_name']}")
except Exception as e:
    print(f"  Erro: {e}")

# 2. Tambem buscar tabelas com 'alterac'
print("\n=== TABELAS COM 'alterac' ===")
try:
    rows = nekt_query("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name LIKE '%alterac%'
        ORDER BY table_schema, table_name
    """)
    for r in rows:
        print(f"  {r['table_schema']}.{r['table_name']}")
except Exception as e:
    print(f"  Erro: {e}")

# 3. Contar cards em cada tabela encontrada
TABLES = [
    '"nekt_trusted"."migracao_de_contratos_all_cards_305643176"',
    '"nekt_service"."pipefy_migracao_de_contratosall_cards_305643176"',
]
print("\n=== CONTAGEM DE CARDS ===")
for t in TABLES:
    try:
        rows = nekt_query(f"SELECT COUNT(*) AS total FROM {t}")
        print(f"  {t}: {rows[0]['total']} cards")
    except Exception as e:
        print(f"  {t}: ERRO {e}")

# 4. Buscar tabelas pipefy_all_cards que tem muitos cards (pode ser o pipe certo com nome diferente)
print("\n=== TABELAS PIPEFY PEOPLE/DP ===")
try:
    rows = nekt_query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'nekt_service'
          AND table_name LIKE 'pipefy_all_cards_%'
        ORDER BY table_name
    """)
    for r in rows:
        tn = r['table_name']
        try:
            cnt = nekt_query(f'SELECT COUNT(*) AS total FROM "nekt_service"."{tn}"')
            print(f"  {tn}: {cnt[0]['total']} cards")
        except:
            print(f"  {tn}: erro")
except Exception as e:
    print(f"  Erro: {e}")

# 5. Verificar se ha tabela silver com alteracao contratual
print("\n=== TABELAS SILVER COM 'contrat' ou 'alterac' ===")
try:
    rows = nekt_query("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE (table_name LIKE '%contrat%' OR table_name LIKE '%alterac%')
          AND table_schema IN ('nekt_silver', 'nekt_trusted', 'nekt_service')
        ORDER BY table_schema, table_name
    """)
    for r in rows:
        print(f"  {r['table_schema']}.{r['table_name']}")
except Exception as e:
    print(f"  Erro: {e}")
