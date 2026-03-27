"""Explora a tabela de admissoes para entender a estrutura."""
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

# 1. Colunas da tabela
print("=== COLUNAS ===")
try:
    rows = nekt_query("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'pipefy_all_cards_303470834_colunas_expandidas'
          AND table_schema = 'nekt_service'
        ORDER BY ordinal_position
    """)
    for r in rows:
        print(f"  {r['column_name']}")
except Exception as e:
    print(f"  Erro: {e}")

# 2. Ultimos 10 cards criados
print("\n=== ULTIMOS 10 CARDS ===")
try:
    rows = nekt_query("""
        SELECT id, title, createdat, currentphasename, createdbyname
        FROM "nekt_service"."pipefy_all_cards_303470834_colunas_expandidas"
        ORDER BY createdat DESC
        LIMIT 10
    """)
    for r in rows:
        print(f"  {r['createdat'][:16]} | {r['currentphasename'][:30]} | {r.get('createdbyname','?')[:20]} | {r['title'][:40]}")
except Exception as e:
    print(f"  Erro: {e}")

# 3. Cards criados em marco 2026 por dia
print("\n=== CARDS CRIADOS EM MARCO 2026 POR DIA ===")
try:
    rows = nekt_query("""
        SELECT
          DATE(createdat) AS data,
          COUNT(*) AS total
        FROM "nekt_service"."pipefy_all_cards_303470834_colunas_expandidas"
        WHERE createdat >= TIMESTAMP '2026-03-01'
        GROUP BY DATE(createdat)
        ORDER BY data
    """)
    for r in rows:
        print(f"  {r['data']}: {r['total']}")
except Exception as e:
    print(f"  Erro: {e}")

# 4. Checar se tem coluna de recrutador ou quem criou
print("\n=== COLUNAS COM RECRUT/CRIADO/RESPONSAVEL ===")
try:
    rows = nekt_query("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'pipefy_all_cards_303470834_colunas_expandidas'
          AND table_schema = 'nekt_service'
          AND (column_name LIKE '%recru%' OR column_name LIKE '%creat%'
               OR column_name LIKE '%assign%' OR column_name LIKE '%responsav%'
               OR column_name LIKE '%name%')
        ORDER BY column_name
    """)
    for r in rows:
        print(f"  {r['column_name']}")
except Exception as e:
    print(f"  Erro: {e}")
