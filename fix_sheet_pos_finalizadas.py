"""
fix_sheet_pos_finalizadas.py -- Preenche historico de Posicoes Finalizadas.
"""
import csv, io, json, os, sys
from datetime import datetime
import gspread, requests
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

GSHEETS_CREDS_JSON = os.environ.get("GSHEETS_CREDS_JSON", "")
GSHEETS_SPREADSHEET_ID = "1LbFqZEWsj8edh8O0Q7fGpcam4rJH4a6Qp7qBtHvlpv0"
GSHEETS_TAB_NAME = os.environ.get("GSHEETS_TAB_NAME", "teste KPI")
NEKT_API_URL = "https://api.nekt.ai/api/v1/sql-query/"
NEKT_API_KEY = os.environ.get("NEKT_API_KEY", "")

ROW_PF_TOTAL = 18
ROW_PF_CLARA = 19
ROW_PF_JONAS = 20
ROW_PF_JULIA = 21


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


# Buscar posicoes finalizadas
print("Buscando posicoes finalizadas...")
pf_data = {}
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
  AND p.hiredat >= TIMESTAMP '2026-01-29'
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
    dt = r["data"][:10]
    rec = r["recrutador"]
    total = int(r["total"])
    if dt not in pf_data:
        pf_data[dt] = {"total": 0, "Clara": 0, "Jonas": 0, "Julia": 0}
    pf_data[dt][rec] = pf_data[dt].get(rec, 0) + total
    pf_data[dt]["total"] += total

print(f"  {len(pf_data)} dias com posicoes finalizadas")
for dt in sorted(pf_data):
    d = pf_data[dt]
    print(f"  {dt}: total={d['total']} Clara={d['Clara']} Jonas={d['Jonas']} Julia={d['Julia']}")

# Conectar planilha
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
if os.path.isfile(GSHEETS_CREDS_JSON):
    creds = Credentials.from_service_account_file(GSHEETS_CREDS_JSON, scopes=scopes)
else:
    creds = Credentials.from_service_account_info(json.loads(GSHEETS_CREDS_JSON), scopes=scopes)

gc = gspread.authorize(creds)
ws = gc.open_by_key(GSHEETS_SPREADSHEET_ID).worksheet(GSHEETS_TAB_NAME)
vals = ws.get_all_values()

# Encontra datas
date_row = None
for idx, row in enumerate(vals[:5]):
    for cell in row[1:]:
        if cell.strip() and '/' in cell and len(cell.strip()) == 10:
            date_row = row
            break
    if date_row:
        break

col_dates = {}
for i in range(1, len(date_row)):
    v = date_row[i].strip()
    if v:
        try:
            dt = datetime.strptime(v, "%d/%m/%Y")
            col_dates[i] = dt
        except ValueError:
            pass

# Preencher
cells = []
for col_idx, dt in sorted(col_dates.items()):
    is_weekend = dt.weekday() >= 5
    dt_iso = dt.strftime("%Y-%m-%d")

    for row_idx, field in [(ROW_PF_TOTAL, "total"), (ROW_PF_CLARA, "Clara"), (ROW_PF_JONAS, "Jonas"), (ROW_PF_JULIA, "Julia")]:
        if is_weekend:
            new_val = "-"
        else:
            new_val = pf_data.get(dt_iso, {"total": 0, "Clara": 0, "Jonas": 0, "Julia": 0})[field]

        old_val = vals[row_idx][col_idx] if row_idx < len(vals) and col_idx < len(vals[row_idx]) else ""
        if str(new_val) != str(old_val):
            cells.append({"range": rowcol_to_a1(row_idx + 1, col_idx + 1), "values": [[new_val]]})
            print(f"  [{dt.strftime('%d/%m')}] Row {row_idx}: {old_val or '(vazio)'} -> {new_val}")

print(f"\nTotal: {len(cells)} celulas")
if cells:
    ws.batch_update(cells, value_input_option="USER_ENTERED")
    print("Aplicado!")
