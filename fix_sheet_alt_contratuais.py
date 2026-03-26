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

TABLE = '"nekt_trusted"."migracao_de_contratos_all_cards_305643176"'

# Primeiro descobrir as colunas
print(f"  Listando colunas de {TABLE}...")
try:
    cols = nekt_query(f"SELECT * FROM {TABLE} LIMIT 1")
    if cols:
        print(f"  Colunas: {list(cols[0].keys())}")
    else:
        print("  Tabela vazia")
except Exception as e:
    print(f"  Erro: {e}")
    # Tentar tabela alternativa
    TABLE = '"nekt_service"."pipefy_migracao_de_contratosall_cards_305643176"'
    print(f"\n  Tentando {TABLE}...")
    try:
        cols = nekt_query(f"SELECT * FROM {TABLE} LIMIT 1")
        if cols:
            print(f"  Colunas: {list(cols[0].keys())}")
    except Exception as e2:
        print(f"  Tambem falhou: {e2}")
        sys.exit(1)

# Detectar nome da coluna de fase
sample = cols[0] if cols else {}
phase_col = None
for k in sample.keys():
    if "phase" in k.lower() or "fase" in k.lower() or "status" in k.lower():
        phase_col = k
        break

if not phase_col:
    print(f"  AVISO: coluna de fase nao encontrada. Colunas: {list(sample.keys())}")
    # Tentar current_phase_name (padrao pipefy silver)
    phase_col = "current_phase_name"
    print(f"  Tentando '{phase_col}'...")

print(f"  Coluna de fase: {phase_col}")

# Agora buscar fases
rows = nekt_query(f'SELECT "{phase_col}", COUNT(*) AS total FROM {TABLE} GROUP BY "{phase_col}" ORDER BY total DESC')
print("\n  Fases encontradas:")
for r in rows:
    print(f"    {r[phase_col]}: {r['total']}")

# Identificar fases finais
FASES_FINAIS = []
for r in rows:
    fase = r[phase_col]
    if fase:
        fl = fase.lower()
        if "conclu" in fl or "cancel" in fl or "finaliz" in fl:
            FASES_FINAIS.append(fase)

print(f"\nFases finais detectadas: {FASES_FINAIS}")

# --- Buscar todos os cards com createdat e finishedat ---
print("\nBuscando cards...")

# Detectar coluna de data de criacao e conclusao
create_col = None
finish_col = None
for k in sample.keys():
    kl = k.lower()
    if "createdat" in kl or "created_at" in kl or "data_criacao" in kl:
        create_col = k
    if "finishedat" in kl or "finished_at" in kl or "data_conclusao" in kl:
        finish_col = k

if not create_col:
    create_col = "createdat"
if not finish_col:
    finish_col = "finishedat"

print(f"  Colunas: criacao={create_col}, conclusao={finish_col}, fase={phase_col}")

cards = nekt_query(f"""
    SELECT
        DATE("{create_col}") AS data_criacao,
        DATE("{finish_col}") AS data_conclusao,
        "{phase_col}" AS fase
    FROM {TABLE}
    ORDER BY "{create_col}"
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
