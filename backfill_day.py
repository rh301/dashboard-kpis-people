"""
backfill_day.py -- Preenche uma data especifica na planilha de KPIs.
Reutiliza a logica do sync.py mas para TARGET_DATE.

Uso:
    TARGET_DATE=2026-03-26 NEKT_API_KEY=xxx GSHEETS_CREDS_JSON=xxx python backfill_day.py
"""

import os
import sys
from datetime import datetime, timedelta, timezone

# Importa tudo do sync.py
sys.path.insert(0, os.path.dirname(__file__))
from sync import (
    nekt_query, fetch_medalhas, fetch_onboarding,
    transformar, update_google_sheets,
    BRT, NEKT_API_KEY,
)

TARGET_DATE = os.environ.get("TARGET_DATE", "")
if not TARGET_DATE:
    print("ERRO: TARGET_DATE nao definida (formato YYYY-MM-DD)")
    sys.exit(1)

target = datetime.strptime(TARGET_DATE, "%Y-%m-%d").date()
print(f"[backfill] Data alvo: {target}")

is_weekend = target.weekday() >= 5
if is_weekend:
    print(f"[backfill] Fim de semana - sera preenchido com '-'")

# Monkey-patch data_referencia_brt para retornar a data alvo
import sync
sync.data_referencia_brt = lambda: target

# Coletar dados
from sync import (
    SQL_ENTREVISTAS, SQL_CONTRATACOES_MES, SQL_DESLIGAMENTOS, SQL_CANCELADAS,
    SQL_DEPARTAMENTOS, SQL_HC_EMPRESA, SQL_HEADCOUNT, SQL_SUP_NOVOS, SQL_SUP_FIN,
    SQL_SUP_ABERTOS, SQL_ALT_CONTRATUAIS, SQL_ADMISSOES, SQL_POS_ABERTAS,
    SQL_ACIMA_SLA, SQL_SLA_MEDIO, SQL_VAGAS_EMPRESA, SQL_VAGAS_DETALHE,
    SQL_BANCO_TALENTOS, SQL_PCT_ACEITOS,
)

queries = {
    "entrevistas": SQL_ENTREVISTAS,
    "contratacoes_mes": SQL_CONTRATACOES_MES,
    "desligamentos": SQL_DESLIGAMENTOS,
    "canceladas": SQL_CANCELADAS,
    "departamentos": SQL_DEPARTAMENTOS,
    "hc_empresa": SQL_HC_EMPRESA,
    "headcount": SQL_HEADCOUNT,
    "sup_novos": SQL_SUP_NOVOS,
    "sup_fin": SQL_SUP_FIN,
    "sup_abertos": SQL_SUP_ABERTOS,
    "alt_contratuais": SQL_ALT_CONTRATUAIS,
    "admissoes": SQL_ADMISSOES,
    "pos_abertas": SQL_POS_ABERTAS,
    "acima_sla": SQL_ACIMA_SLA,
    "sla_medio": SQL_SLA_MEDIO,
    "vagas_empresa": SQL_VAGAS_EMPRESA,
    "vagas_detalhe": SQL_VAGAS_DETALHE,
    "banco_talentos": SQL_BANCO_TALENTOS,
    "pct_aceitos": SQL_PCT_ACEITOS,
}

dados = {}
errors = []
for nome, sql in queries.items():
    try:
        print(f"[backfill] {nome}...")
        dados[nome] = nekt_query(sql)
    except Exception as e:
        print(f"[ERRO] Query '{nome}' falhou: {e}", file=sys.stderr)
        dados[nome] = []
        errors.append({"query": nome, "error": str(e)})

# Medalhas
try:
    dados["medalhas"] = fetch_medalhas()
except Exception as e:
    print(f"[ERRO] Medalhas falhou: {e}", file=sys.stderr)
    dados["medalhas"] = []
    errors.append({"query": "medalhas", "error": str(e)})

# Onboarding
try:
    dados["onboarding"] = fetch_onboarding()
except Exception as e:
    print(f"[ERRO] Onboarding falhou: {e}", file=sys.stderr)
    dados["onboarding"] = {}
    errors.append({"query": "onboarding", "error": str(e)})

transformado = transformar(dados)

# Atualizar planilha
try:
    update_google_sheets(transformado)
except Exception as e:
    print(f"[ERRO] Planilha falhou: {e}", file=sys.stderr)
    sys.exit(1)

if errors:
    print(f"[backfill] ATENCAO: {len(errors)} erros: {[e['query'] for e in errors]}")

print(f"[backfill] Concluido para {target}")
