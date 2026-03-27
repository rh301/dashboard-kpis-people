"""Explora as tabelas certas do pipe 305643176."""
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

TABLES = [
    '"nekt_silver"."pipefy_all_cards_305643176_colunas_expandidas"',
    '"nekt_trusted"."pipefy_all_cards_305643176"',
]

for t in TABLES:
    print(f"\n=== {t} ===")

    # Contagem
    try:
        rows = nekt_query(f"SELECT COUNT(*) AS total FROM {t}")
        print(f"  Total: {rows[0]['total']} cards")
    except Exception as e:
        print(f"  ERRO count: {e}")
        continue

    # Colunas
    try:
        rows = nekt_query(f"SELECT * FROM {t} LIMIT 1")
        if rows:
            print(f"  Colunas: {list(rows[0].keys())}")
    except Exception as e:
        print(f"  ERRO cols: {e}")
        continue

    # Fases
    try:
        phase_col = None
        for k in rows[0].keys():
            if 'phase' in k.lower():
                phase_col = k
                break
        if not phase_col:
            phase_col = 'currentphasename'

        phase_rows = nekt_query(f'SELECT {phase_col}, COUNT(*) AS total FROM {t} GROUP BY {phase_col} ORDER BY total DESC')
        print(f"  Fases (col={phase_col}):")
        for r in phase_rows:
            print(f"    {r[phase_col]}: {r['total']}")
    except Exception as e:
        print(f"  ERRO fases: {e}")

    # Cards recentes
    try:
        recent = nekt_query(f"SELECT * FROM {t} WHERE createdat >= TIMESTAMP '2026-03-20' ORDER BY createdat DESC LIMIT 10")
        print(f"\n  Cards recentes (desde 20/03):")
        for r in recent:
            title = r.get('title', '?')[:40]
            createdat = r.get('createdat', '?')[:16]
            phase = r.get(phase_col, '?')[:30] if phase_col in r else '?'
            print(f"    {createdat} | {phase} | {title}")
    except Exception as e:
        print(f"  ERRO recentes: {e}")

    # Abertos hoje
    try:
        abertos = nekt_query(f"""
            SELECT COUNT(*) AS em_aberto FROM {t}
            WHERE {phase_col} NOT LIKE '%onclu%'
              AND {phase_col} NOT LIKE '%ancel%'
        """)
        print(f"\n  Em aberto (nao concluido/cancelado): {abertos[0]['em_aberto']}")
    except Exception as e:
        print(f"  ERRO abertos: {e}")
