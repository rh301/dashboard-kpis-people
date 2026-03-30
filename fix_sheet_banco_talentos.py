"""
fix_sheet_banco_talentos.py -- Corrige historico de Banco de Talentos
na planilha. Calcula snapshot diario (acumulado de talentos adicionados
a vaga "Banco de Talentos" ate cada dia).

Uso:
    python fix_sheet_banco_talentos.py          # dry-run
    python fix_sheet_banco_talentos.py --apply  # aplica
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
ROW_BANCO_TALENTOS = 5  # 0-indexed


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

# --- Buscar talentos da vaga "Banco de Talentos" com data de criacao ---
print("\nBuscando talentos da vaga Banco de Talentos...")

talentos = nekt_query("""
    SELECT DISTINCT t.talentid,
           DATE(CAST(t.createdat AS TIMESTAMP)) AS data_add
    FROM "nekt_trusted"."inhire_job_talents" t
    JOIN "nekt_trusted"."inhire_job_details" j ON SUBSTR(t.id, 1, 36) = j.id
    WHERE TRIM(j.name) = 'Banco de Talentos'
    ORDER BY data_add
""")
print(f"  {len(talentos)} talentos encontrados")


# --- Calcular snapshot para cada dia (acumulado ate o dia) ---
def total_ate_dia(dia_iso):
    count = 0
    for t in talentos:
        if t["data_add"] and t["data_add"] <= dia_iso:
            count += 1
    return count


print("\nCalculando snapshots...")
corrections = []
for col_idx, dt in sorted(weekday_dates.items()):
    dia_iso = dt.strftime("%Y-%m-%d")
    new_val = total_ate_dia(dia_iso)
    old_val = all_vals[ROW_BANCO_TALENTOS][col_idx] if ROW_BANCO_TALENTOS < len(all_vals) and col_idx < len(all_vals[ROW_BANCO_TALENTOS]) else ""
    marker = " <<<" if str(new_val) != str(old_val) else ""
    print(f"  [{dt.strftime('%d/%m')}] Banco Talentos: {old_val or '(vazio)'} -> {new_val}{marker}")
    if str(new_val) != str(old_val):
        corrections.append((col_idx, old_val, new_val))

# Fins de semana -> "-"
for col_idx, dt in sorted(col_dates.items()):
    if dt.weekday() >= 5:
        old_val = all_vals[ROW_BANCO_TALENTOS][col_idx] if ROW_BANCO_TALENTOS < len(all_vals) and col_idx < len(all_vals[ROW_BANCO_TALENTOS]) else ""
        if old_val != "-":
            corrections.append((col_idx, old_val, "-"))
            print(f"  [{dt.strftime('%d/%m')}] FDS: {old_val or '(vazio)'} -> -")

print(f"\nCorrecoes: {len(corrections)}")

if "--apply" not in sys.argv:
    print("\nModo DRY-RUN. Para aplicar, rode com --apply")
    sys.exit(0)

cells = [{"range": rowcol_to_a1(ROW_BANCO_TALENTOS + 1, ci + 1), "values": [[nv]]} for ci, _, nv in corrections]
ws.batch_update(cells, value_input_option="USER_ENTERED")
print(f"Pronto! {len(cells)} celulas corrigidas.")
