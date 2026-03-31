"""
fix_sheet_vagas_canceladas.py -- Preenche historico de Vagas Canceladas
na planilha. Conta vagas canceladas no InHire por dia (updatedat).

Uso:
    python fix_sheet_vagas_canceladas.py          # dry-run
    python fix_sheet_vagas_canceladas.py --apply  # aplica
"""

import csv as _csv
import io as _io
import json
import os
import sys
from datetime import datetime

import gspread
import requests
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

GSHEETS_CREDS_JSON = os.environ.get("GSHEETS_CREDS_JSON", "")
GSHEETS_SPREADSHEET_ID = "1LbFqZEWsj8edh8O0Q7fGpcam4rJH4a6Qp7qBtHvlpv0"
GSHEETS_TAB_NAME = os.environ.get("GSHEETS_TAB_NAME", "teste KPI")
NEKT_API_URL = "https://api.nekt.ai/api/v1/sql-query/"
NEKT_API_KEY = os.environ.get("NEKT_API_KEY", "")
ROW_VAGAS_CANCELADAS = 4  # 0-indexed


def nekt_query(sql):
    resp = requests.post(NEKT_API_URL, json={"sql": sql, "mode": "csv"},
                         headers={"x-api-key": NEKT_API_KEY, "Content-Type": "application/json"},
                         timeout=120)
    if resp.status_code != 200:
        raise Exception(f"{resp.status_code} {resp.reason}: {resp.text[:300]}")
    body = resp.json()
    if body.get("state") != "SUCCEEDED":
        raise Exception(f"Query failed: {body}")
    urls = body.get("presigned_urls", [])
    if not urls:
        return []
    rows = []
    for url in urls:
        csv_resp = requests.get(url, timeout=60)
        csv_resp.raise_for_status()
        reader = _csv.DictReader(_io.StringIO(csv_resp.text))
        rows.extend(list(reader))
    return rows


# --- Conectar planilha ---
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
if os.path.isfile(GSHEETS_CREDS_JSON):
    creds = Credentials.from_service_account_file(GSHEETS_CREDS_JSON, scopes=scopes)
else:
    creds = Credentials.from_service_account_info(json.loads(GSHEETS_CREDS_JSON), scopes=scopes)

gc = gspread.authorize(creds)
sh = gc.open_by_key(GSHEETS_SPREADSHEET_ID)
ws = sh.worksheet(GSHEETS_TAB_NAME)
all_vals = ws.get_all_values()

# Encontra datas na planilha
date_row = None
for idx, row in enumerate(all_vals[:5]):
    for cell in row[1:]:
        if cell.strip() and '/' in cell and len(cell.strip()) == 10:
            date_row = row
            break
    if date_row:
        break

col_dates = {}
for i in range(1, len(date_row)):
    val = date_row[i].strip()
    if val:
        try:
            dt = datetime.strptime(val, "%d/%m/%Y")
            col_dates[i] = dt
        except ValueError:
            pass

weekday_dates = {k: v for k, v in col_dates.items() if v.weekday() < 5}
date_list = sorted(weekday_dates.values())

print(f"Dias uteis na planilha: {[d.strftime('%d/%m') for d in date_list]}")

# --- Buscar vagas canceladas por dia ---
print("\nBuscando vagas canceladas por dia...")

canc_rows = nekt_query("""
    SELECT DATE(CAST(updatedat AS TIMESTAMP)) AS dia,
           COUNT(DISTINCT id) AS canceladas
    FROM "nekt_trusted"."inhire_job_details"
    WHERE status = 'canceled'
      AND CAST(updatedat AS TIMESTAMP) >= TIMESTAMP '2025-01-01'
    GROUP BY DATE(CAST(updatedat AS TIMESTAMP))
    ORDER BY dia
""")

# Montar mapa dia -> canceladas
canc_map = {}
for r in canc_rows:
    canc_map[r["dia"][:10]] = int(r["canceladas"])

print(f"  {len(canc_rows)} dias com cancelamentos encontrados")

# --- Calcular valores para cada dia ---
print("\nCalculando valores...")
corrections = []
for col_idx, dt in sorted(weekday_dates.items()):
    dia_iso = dt.strftime("%Y-%m-%d")
    new_val = canc_map.get(dia_iso, 0)
    old_val = all_vals[ROW_VAGAS_CANCELADAS][col_idx] if ROW_VAGAS_CANCELADAS < len(all_vals) and col_idx < len(all_vals[ROW_VAGAS_CANCELADAS]) else ""
    marker = " <<<" if str(new_val) != str(old_val) else ""
    print(f"  [{dt.strftime('%d/%m')}] Vagas Canceladas: {old_val or '(vazio)'} -> {new_val}{marker}")
    if str(new_val) != str(old_val):
        corrections.append((col_idx, old_val, new_val))

# Fins de semana -> "-"
for col_idx, dt in sorted(col_dates.items()):
    if dt.weekday() >= 5:
        old_val = all_vals[ROW_VAGAS_CANCELADAS][col_idx] if ROW_VAGAS_CANCELADAS < len(all_vals) and col_idx < len(all_vals[ROW_VAGAS_CANCELADAS]) else ""
        if old_val != "-":
            corrections.append((col_idx, old_val, "-"))
            print(f"  [{dt.strftime('%d/%m')}] FDS: {old_val or '(vazio)'} -> -")

print(f"\nCorrecoes: {len(corrections)}")

if "--apply" not in sys.argv:
    print("\nModo DRY-RUN. Para aplicar, rode com --apply")
    sys.exit(0)

cells = [{"range": rowcol_to_a1(ROW_VAGAS_CANCELADAS + 1, ci + 1), "values": [[nv]]} for ci, _, nv in corrections]
ws.batch_update(cells, value_input_option="USER_ENTERED")
print(f"Pronto! {len(cells)} celulas corrigidas.")
