"""Explora a vaga Banco de Talentos no InHire."""
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

# 1. Buscar vagas com "banco" ou "talento" no nome
print("=== VAGAS COM 'banco' ou 'talento' ===")
rows = nekt_query("""
    SELECT id, name, CAST(istalentpool AS VARCHAR) AS is_pool,
           CAST(isactive AS VARCHAR) AS is_active, status
    FROM "nekt_trusted"."inhire_job_details"
    WHERE LOWER(name) LIKE '%banco%' OR LOWER(name) LIKE '%talento%'
    ORDER BY name
""")
for r in rows:
    print(f"  {r['name']} | id={r['id'][:12]}... | pool={r['is_pool']} | active={r['is_active']} | status={r['status']}")

# 2. Contar talentos na ABA (istalentpool=true)
print("\n=== CONTAGEM ABA (istalentpool=true) ===")
rows = nekt_query("""
    SELECT COUNT(DISTINCT t.talentid) AS total
    FROM "nekt_trusted"."inhire_job_talents" t
    JOIN "nekt_trusted"."inhire_job_details" j ON SUBSTR(t.id, 1, 36) = j.id
    WHERE CAST(j.istalentpool AS VARCHAR) = 'true'
""")
print(f"  Total: {rows[0]['total']}")

# 3. Contar talentos na VAGA "Banco de Talentos" (por nome)
print("\n=== CONTAGEM VAGA (nome LIKE '%Banco de Talento%') ===")
rows = nekt_query("""
    SELECT j.name, COUNT(DISTINCT t.talentid) AS total
    FROM "nekt_trusted"."inhire_job_talents" t
    JOIN "nekt_trusted"."inhire_job_details" j ON SUBSTR(t.id, 1, 36) = j.id
    WHERE LOWER(j.name) LIKE '%banco de talento%'
    GROUP BY j.name
    ORDER BY total DESC
""")
for r in rows:
    print(f"  {r['name']}: {r['total']}")

# 4. Ver colunas de inhire_job_talents
print("\n=== COLUNAS inhire_job_talents ===")
rows = nekt_query('SELECT * FROM "nekt_trusted"."inhire_job_talents" LIMIT 1')
if rows:
    print(f"  {list(rows[0].keys())}")

# 5. Talentos na vaga BT por data de criacao (para backfill)
print("\n=== TALENTOS NA VAGA BT POR DATA ===")
rows = nekt_query("""
    SELECT DATE(CAST(t.createdat AS TIMESTAMP)) AS dia,
           COUNT(DISTINCT t.talentid) AS novos
    FROM "nekt_trusted"."inhire_job_talents" t
    JOIN "nekt_trusted"."inhire_job_details" j ON SUBSTR(t.id, 1, 36) = j.id
    WHERE LOWER(j.name) LIKE '%banco de talento%'
    GROUP BY DATE(CAST(t.createdat AS TIMESTAMP))
    ORDER BY dia DESC
    LIMIT 30
""")
for r in rows:
    print(f"  {r['dia']}: +{r['novos']}")

# 6. Snapshot acumulado - total de talentos na vaga BT ate cada dia
print("\n=== SNAPSHOT ACUMULADO (total ate cada dia) ===")
rows = nekt_query("""
    SELECT DATE(CAST(t.createdat AS TIMESTAMP)) AS dia,
           COUNT(DISTINCT t.talentid) AS novos
    FROM "nekt_trusted"."inhire_job_talents" t
    JOIN "nekt_trusted"."inhire_job_details" j ON SUBSTR(t.id, 1, 36) = j.id
    WHERE LOWER(j.name) LIKE '%banco de talento%'
    GROUP BY DATE(CAST(t.createdat AS TIMESTAMP))
    ORDER BY dia
""")
acum = 0
for r in rows:
    acum += int(r['novos'])
    print(f"  {r['dia']}: {acum} (acumulado)")
