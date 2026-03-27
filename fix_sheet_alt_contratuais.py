"""
fix_sheet_alt_contratuais.py -- Preenche historico de Alteracoes Contratuais
em aberto na planilha. Calcula snapshot diario via Nekt API.

Uso:
    GSHEETS_CREDS_JSON=path/to/sa.json NEKT_API_KEY=xxx python fix_sheet_alt_contratuais.py
    GSHEETS_CREDS_JSON=path/to/sa.json NEKT_API_KEY=xxx python fix_sheet_alt_contratuais.py --apply
"""

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
ROW_ALT_CONTRATUAIS = 38  # 0-indexed


def nekt_query(sql):
    import csv as _csv
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
        import io as _io
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

# Encontra datas
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

# Filtrar apenas dias uteis
weekday_dates = {k: v for k, v in col_dates.items() if v.weekday() < 5}
date_list = sorted(weekday_dates.values())

print(f"Dias uteis na planilha: {[d.strftime('%d/%m') for d in date_list]}")

# --- Primeiro, ver a estrutura do pipe ---
print("\nExplorando pipe 305643176...")

TABLE = '"nekt_silver"."pipefy_all_cards_305643176_colunas_expandidas"'

# Buscar fases
print(f"  Tabela: {TABLE}")
rows = nekt_query(f"SELECT current_phase_name, COUNT(*) AS total FROM {TABLE} GROUP BY current_phase_name ORDER BY total DESC")
print("  Fases:")
for r in rows:
    print(f"    {r['current_phase_name']}: {r['total']}")

# --- Buscar todos os cards ---
print("\nBuscando cards...")

cards = nekt_query(f"""
    SELECT
        DATE(created_at) AS data_criacao,
        DATE(finished_at) AS data_conclusao,
        current_phase_name AS fase
    FROM {TABLE}
    ORDER BY created_at
""")
print(f"  {len(cards)} cards encontrados")

# --- Calcular snapshot para cada dia ---
def em_aberto_no_dia(dia_iso):
    count = 0
    for c in cards:
        criacao = c["data_criacao"]
        conclusao = c["data_conclusao"]
        if criacao and criacao <= dia_iso:
            if not conclusao or conclusao == "" or conclusao > dia_iso:
                count += 1
    return count

print("\nCalculando snapshots...")
corrections = []
for col_idx, dt in sorted(weekday_dates.items()):
    dia_iso = dt.strftime("%Y-%m-%d")
    new_val = em_aberto_no_dia(dia_iso)
    old_val = all_vals[ROW_ALT_CONTRATUAIS][col_idx] if ROW_ALT_CONTRATUAIS < len(all_vals) and col_idx < len(all_vals[ROW_ALT_CONTRATUAIS]) else ""
    marker = " <<<" if str(new_val) != str(old_val) else ""
    print(f"  [{dt.strftime('%d/%m')}] Alt Contratuais em Aberto: {old_val or '(vazio)'} -> {new_val}{marker}")
    if str(new_val) != str(old_val):
        corrections.append((col_idx, old_val, new_val))

# Fins de semana -> "-"
for col_idx, dt in sorted(col_dates.items()):
    if dt.weekday() >= 5:
        old_val = all_vals[ROW_ALT_CONTRATUAIS][col_idx] if ROW_ALT_CONTRATUAIS < len(all_vals) and col_idx < len(all_vals[ROW_ALT_CONTRATUAIS]) else ""
        if old_val != "-":
            corrections.append((col_idx, old_val, "-"))
            print(f"  [{dt.strftime('%d/%m')}] FDS: {old_val or '(vazio)'} -> -")

print(f"\nCorrecoes: {len(corrections)}")

if "--apply" not in sys.argv:
    print("\nModo DRY-RUN. Para aplicar, rode com --apply")
    sys.exit(0)

cells = [{"range": rowcol_to_a1(ROW_ALT_CONTRATUAIS + 1, ci + 1), "values": [[nv]]} for ci, _, nv in corrections]
ws.batch_update(cells, value_input_option="USER_ENTERED")
print(f"Pronto! {len(cells)} celulas corrigidas.")
