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
    resp = requests.post(NEKT_API_URL, json={"sql_query": sql},
                         headers={"x-api-key": NEKT_API_KEY}, timeout=60)
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") == "failed":
        raise Exception(body.get("error", "Query failed"))
    cols = body["columns"]
    return [dict(zip(cols, row)) for row in body["data"]]


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
try:
    # Tentar tabela nekt_trusted
    rows = nekt_query("""
        SELECT currentphasename, COUNT(*) AS total
        FROM "nekt_trusted"."migracao_de_contratos_all_cards_305643176"
        GROUP BY currentphasename
        ORDER BY total DESC
    """)
    print("Tabela: nekt_trusted.migracao_de_contratos_all_cards_305643176")
    for r in rows:
        print(f"  {r['currentphasename']}: {r['total']}")
    TABLE = '"nekt_trusted"."migracao_de_contratos_all_cards_305643176"'
except Exception as e:
    print(f"  nekt_trusted falhou: {e}")
    try:
        rows = nekt_query("""
            SELECT currentphasename, COUNT(*) AS total
            FROM "nekt_service"."pipefy_migracao_de_contratosall_cards_305643176"
            GROUP BY currentphasename
            ORDER BY total DESC
        """)
        print("Tabela: nekt_service.pipefy_migracao_de_contratosall_cards_305643176")
        for r in rows:
            print(f"  {r['currentphasename']}: {r['total']}")
        TABLE = '"nekt_service"."pipefy_migracao_de_contratosall_cards_305643176"'
    except Exception as e2:
        print(f"  nekt_service tambem falhou: {e2}")
        sys.exit(1)

# Identificar fases finais
FASES_FINAIS = []
for r in rows:
    fase = r["currentphasename"]
    fl = fase.lower()
    if "conclu" in fl or "cancel" in fl or "finaliz" in fl:
        FASES_FINAIS.append(fase)

print(f"\nFases finais detectadas: {FASES_FINAIS}")

# --- Buscar todos os cards com createdat e finishedat ---
print("\nBuscando cards...")
cards = nekt_query(f"""
    SELECT
        DATE(createdat) AS data_criacao,
        DATE(finishedat) AS data_conclusao,
        currentphasename
    FROM {TABLE}
    ORDER BY createdat
""")
print(f"  {len(cards)} cards encontrados")

# --- Calcular snapshot para cada dia ---
def em_aberto_no_dia(dia_iso):
    count = 0
    for c in cards:
        criacao = c["data_criacao"]
        conclusao = c["data_conclusao"]
        fase = c["currentphasename"]
        if criacao and criacao <= dia_iso:
            if conclusao is None or conclusao == "" or conclusao > dia_iso:
                # Card ainda nao concluido nesse dia
                count += 1
            # Se ja concluido mas fase nao e final, pode ser reaberto
            # Simplificacao: usa data de conclusao como proxy
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
