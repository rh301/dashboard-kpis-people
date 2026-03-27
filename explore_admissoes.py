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

# 4. Responsavel pela admissao - valores
print("\n=== RESPONSAVEL PELA ADMISSAO ===")
try:
    rows = nekt_query("""
        SELECT responsavel_pela_admissao, COUNT(*) AS total
        FROM "nekt_service"."pipefy_all_cards_303470834_colunas_expandidas"
        WHERE responsavel_pela_admissao IS NOT NULL
          AND responsavel_pela_admissao != ''
        GROUP BY responsavel_pela_admissao
        ORDER BY total DESC
        LIMIT 20
    """)
    for r in rows:
        print(f"  {r['responsavel_pela_admissao']}: {r['total']}")
except Exception as e:
    print(f"  Erro: {e}")

# 5. Cards criados em marco 2026 por dia e responsavel
print("\n=== ADMISSOES MARCO 2026 POR DIA E RESPONSAVEL ===")
try:
    rows = nekt_query("""
        SELECT
          DATE(createdat) AS data,
          responsavel_pela_admissao AS responsavel,
          COUNT(*) AS total
        FROM "nekt_service"."pipefy_all_cards_303470834_colunas_expandidas"
        WHERE createdat >= TIMESTAMP '2026-03-01'
          AND title != 'Teste'
        GROUP BY DATE(createdat), responsavel_pela_admissao
        ORDER BY data, responsavel
    """)
    for r in rows:
        print(f"  {r['data']} | {r.get('responsavel','?')[:30]} | {r['total']}")
except Exception as e:
    print(f"  Erro: {e}")

# 6. Tambem testar via inhire_positions.hiredat (metodo antigo)
print("\n=== POSICOES FINALIZADAS (INHIRE) MARCO 2026 ===")
try:
    rows = nekt_query("""
        SELECT
          DATE(p.hiredat) AS data,
          CASE
            WHEN j.recruiterid = '8e68aa32-1214-4c69-870b-626d1515bfe1' THEN 'Clara'
            WHEN j.recruiterid = '9fe49d18-58ce-4225-aa46-0536ca9bfca8' THEN 'Jonas'
            WHEN j.recruiterid = '47baa32f-5986-418f-b42f-d55c168f4a4c' THEN 'Julia'
            WHEN j.recruiterid = '8722b94a-7758-421a-bc2a-3c932fe6e715' THEN 'Mario'
            ELSE 'Outro'
          END AS recrutador,
          COUNT(*) AS total
        FROM "nekt_trusted"."inhire_positions" p
        JOIN "nekt_trusted"."inhire_jobs" j ON p.jobid = j.id
        WHERE p.hiredat IS NOT NULL
          AND p.hiredat >= TIMESTAMP '2026-03-01'
        GROUP BY DATE(p.hiredat),
          CASE
            WHEN j.recruiterid = '8e68aa32-1214-4c69-870b-626d1515bfe1' THEN 'Clara'
            WHEN j.recruiterid = '9fe49d18-58ce-4225-aa46-0536ca9bfca8' THEN 'Jonas'
            WHEN j.recruiterid = '47baa32f-5986-418f-b42f-d55c168f4a4c' THEN 'Julia'
            WHEN j.recruiterid = '8722b94a-7758-421a-bc2a-3c932fe6e715' THEN 'Mario'
            ELSE 'Outro'
          END
        ORDER BY data, recrutador
    """)
    for r in rows:
        print(f"  {r['data']} | {r['recrutador']} | {r['total']}")
except Exception as e:
    print(f"  Erro: {e}")
