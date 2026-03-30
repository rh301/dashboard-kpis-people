"""Explora vagas canceladas no InHire."""
import csv, io, os, requests

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

# 1. Status possiveis das vagas
print("=== STATUS DAS VAGAS (inhire_job_details) ===")
rows = nekt_query("""
    SELECT status, COUNT(*) AS total
    FROM "nekt_trusted"."inhire_job_details"
    GROUP BY status
    ORDER BY total DESC
""")
for r in rows:
    print(f"  {r['status']}: {r['total']}")

# 2. Vagas canceladas recentes
print("\n=== VAGAS CANCELADAS (ultimas 30) ===")
rows = nekt_query("""
    SELECT name, status,
           CAST(createdat AS VARCHAR) AS criada,
           CAST(updatedat AS VARCHAR) AS atualizada,
           u.name AS recrutador
    FROM "nekt_trusted"."inhire_job_details" j
    LEFT JOIN "nekt_trusted"."inhire_users" u ON j.userid = u.id
    WHERE j.status = 'canceled'
    ORDER BY j.updatedat DESC
    LIMIT 30
""")
for r in rows:
    print(f"  {r['atualizada'][:10]} | {r['recrutador'] or '?'} | {r['name'][:50]}")

# 3. Vagas canceladas por mes (2026)
print("\n=== CANCELADAS POR MES (2026) ===")
rows = nekt_query("""
    SELECT DATE_FORMAT(CAST(updatedat AS TIMESTAMP), '%Y-%m') AS mes,
           COUNT(*) AS total
    FROM "nekt_trusted"."inhire_job_details"
    WHERE status = 'canceled'
      AND YEAR(CAST(updatedat AS TIMESTAMP)) >= 2026
    GROUP BY DATE_FORMAT(CAST(updatedat AS TIMESTAMP), '%Y-%m')
    ORDER BY mes
""")
for r in rows:
    print(f"  {r['mes']}: {r['total']}")

# 4. Canceladas por dia (marco 2026)
print("\n=== CANCELADAS POR DIA (marco 2026) ===")
rows = nekt_query("""
    SELECT DATE(CAST(updatedat AS TIMESTAMP)) AS dia,
           COUNT(*) AS total
    FROM "nekt_trusted"."inhire_job_details"
    WHERE status = 'canceled'
      AND CAST(updatedat AS TIMESTAMP) >= TIMESTAMP '2026-03-01'
    GROUP BY DATE(CAST(updatedat AS TIMESTAMP))
    ORDER BY dia
""")
for r in rows:
    print(f"  {r['dia']}: {r['total']}")

# 5. Colunas disponiveis
print("\n=== COLUNAS inhire_job_details ===")
rows = nekt_query('SELECT * FROM "nekt_trusted"."inhire_job_details" LIMIT 1')
if rows:
    print(f"  {list(rows[0].keys())}")

# 6. Tem campo canceldat ou closedat?
print("\n=== CAMPOS DE DATA (amostra vaga cancelada) ===")
rows = nekt_query("""
    SELECT name, status,
           CAST(createdat AS VARCHAR) AS createdat,
           CAST(updatedat AS VARCHAR) AS updatedat,
           CAST(closedat AS VARCHAR) AS closedat,
           CAST(hiredat AS VARCHAR) AS hiredat
    FROM "nekt_trusted"."inhire_job_details"
    WHERE status = 'canceled'
    ORDER BY updatedat DESC
    LIMIT 5
""")
for r in rows:
    print(f"  {r['name'][:40]} | created={r['createdat'][:10]} | updated={r['updatedat'][:10]} | closed={r['closedat'][:10] if r['closedat'] else 'null'} | hired={r['hiredat'][:10] if r['hiredat'] else 'null'}")
