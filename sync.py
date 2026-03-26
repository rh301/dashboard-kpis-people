"""
sync.py — Busca KPIs do Nekt via API REST e gera public/data.js
Executado pelo GitHub Actions diariamente as 22h BRT (01:00 UTC).

Uso manual:
    NEKT_API_KEY=xxx python sync.py

Dependencias:
    pip install requests
"""

import csv
import io
import json
import os
import re
import sys
import tempfile
from datetime import date, datetime, timezone, timedelta

# Timezone BRT (UTC-3) — garante data correta quando roda no GitHub Actions (UTC)
BRT = timezone(timedelta(hours=-3))


def data_referencia_brt() -> date:
    """Retorna a data BRT de referencia para o sync.

    O cron roda as 22h BRT (01:00 UTC), mas o GitHub Actions pode atrasar
    ate ~05:00 UTC (02:00 BRT do dia seguinte). Nesse caso, a data de
    referencia deve ser o dia anterior (o dia que acabou as 22h).

    Regra: se o horario BRT for entre 00:00 e 06:00, usa ontem.
    """
    agora_brt = datetime.now(BRT)
    if agora_brt.hour < 6:
        return (agora_brt - timedelta(days=1)).date()
    return agora_brt.date()

import requests

# ============================================================
# CONFIGURACAO
# ============================================================

NEKT_API_URL = "https://api.nekt.ai/api/v1/sql-query/"
NEKT_API_KEY = os.environ.get("NEKT_API_KEY", "")
DATA_JS_PATH = os.path.join(os.path.dirname(__file__), "public", "data.js")
HISTORY_JSON_PATH = os.path.join(os.path.dirname(__file__), "public", "history.json")

# Google Sheets
GSHEETS_CREDS_JSON = os.environ.get("GSHEETS_CREDS_JSON", "")
GSHEETS_SPREADSHEET_ID = "1LbFqZEWsj8edh8O0Q7fGpcam4rJH4a6Qp7qBtHvlpv0"
GSHEETS_TAB_NAME = os.environ.get("GSHEETS_TAB_NAME", "teste KPI")

# Medalhas — Google Sheets publico (CSV export)
MEDALHAS_SHEET_ID = "1tolIf1eKRMLyYIWwWjB8D3-82YkK1uOPOGmlZXzDfNs"
MEDALHAS_GID = "647381004"
MEDALHAS_CSV_URL = f"https://docs.google.com/spreadsheets/d/{MEDALHAS_SHEET_ID}/export?format=csv&gid={MEDALHAS_GID}"

# Etapas de entrevista
ETAPAS_ENTREVISTA = (
    "Entrevista de Fit Cultural",
    "Entrevista Técnica",
    "Entrevista Final",
    "Entrevista Final com Diretoria",
    "Entrevista Individual",
    "Entrevista Técnica + Role Play",
    "Entrevista Técnica + Role-Play",
    "Entrevista Técnica + Role-play",
    "Entrevista Técnica + Live Case",
    "Role Play",
    "Role Play + Aprofundamento",
    "Role-play",
    "Dinâmica de Fit Cultural",
    "Dinâmica de Grupo",
    "Fit Cultural",
    "DISC + Entrevista de Fit Cultural",
    "Entrevista de Fit Cultural e Técnica",
    "Entrevista de Fit Cultural/Técnica",
    "Entrevista de Fit cultural",
    "Desafio Prático",
)

RECRUTADORES_NOMES = {
    "Clara Alcantara Ferraz Cury": "Clara",
    "Jonas Trajano dos Santos": "Jonas",
    "Júlia Nardes de Alcântara Cotrim": "Júlia",
    "Mario Lopes de Andrade": "Mario",
}

RECRUTADOR_IDS = {
    "8e68aa32-1214-4c69-870b-626d1515bfe1": "Clara",
    "9fe49d18-58ce-4225-aa46-0536ca9bfca8": "Jonas",
    "47baa32f-5986-418f-b42f-d55c168f4a4c": "Júlia",
    "8722b94a-7758-421a-bc2a-3c932fe6e715": "Mario",
}


# ============================================================
# NEKT API
# ============================================================


def nekt_query(sql: str) -> list[dict]:
    """Executa SQL no Nekt e retorna lista de dicts."""
    if not NEKT_API_KEY:
        print("[ERRO] NEKT_API_KEY nao definida", file=sys.stderr)
        sys.exit(1)

    resp = requests.post(
        NEKT_API_URL,
        headers={"x-api-key": NEKT_API_KEY, "Content-Type": "application/json"},
        json={"sql": sql, "mode": "csv"},
        timeout=120,
    )
    resp.raise_for_status()
    body = resp.json()

    if body.get("state") != "SUCCEEDED":
        print(f"[ERRO] Query falhou: {body}", file=sys.stderr)
        sys.exit(1)

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


# ============================================================
# QUERIES
# ============================================================

# --- R&S: Entrevistas por dia e recrutador ---
SQL_ENTREVISTAS = f"""
SELECT
  updatedat_date AS data,
  CASE
    WHEN recruiter_name = 'Clara Alcantara Ferraz Cury' THEN 'Clara'
    WHEN recruiter_name = 'Jonas Trajano dos Santos' THEN 'Jonas'
    WHEN recruiter_name = 'Júlia Nardes de Alcântara Cotrim' THEN 'Júlia'
    WHEN recruiter_name = 'Mario Lopes de Andrade' THEN 'Mario'
    ELSE 'Outro'
  END AS recrutador,
  COUNT(DISTINCT talentid) AS entrevistas
FROM "nekt_silver"."silver_fato_candidatos"
WHERE stage_name IN ({','.join(f"'{e}'" for e in ETAPAS_ENTREVISTA)})
  AND CAST(updatedat_date AS VARCHAR) >= '2026-01-29'
GROUP BY 1, 2
ORDER BY 1, 2
"""

# --- R&S: Contratacoes por mes e recrutador ---
SQL_CONTRATACOES_MES = """
SELECT
  DATE_TRUNC('month', p.hiredat) AS mes,
  CASE
    WHEN j.recruiterid = '8e68aa32-1214-4c69-870b-626d1515bfe1' THEN 'Clara'
    WHEN j.recruiterid = '9fe49d18-58ce-4225-aa46-0536ca9bfca8' THEN 'Jonas'
    WHEN j.recruiterid = '47baa32f-5986-418f-b42f-d55c168f4a4c' THEN 'Julia'
    WHEN j.recruiterid = '8722b94a-7758-421a-bc2a-3c932fe6e715' THEN 'Mario'
    ELSE 'Outro'
  END AS recrutador,
  COUNT(*) AS contratacoes
FROM "nekt_trusted"."inhire_positions" p
JOIN "nekt_trusted"."inhire_jobs" j ON p.jobid = j.id
WHERE p.hiredat IS NOT NULL
  AND p.hiredat >= TIMESTAMP '2025-01-01'
GROUP BY 1, 2
ORDER BY 1, 2
"""

# --- Cultura: Desligamentos por dia (Pipefy pipe 305642527) ---
SQL_DESLIGAMENTOS = """
SELECT
  DATE_FORMAT(DATE_PARSE(data_do_desligamento, '%d/%m/%Y'), '%Y-%m-%d') AS data_desligamento,
  COUNT(*) AS total,
  SUM(CASE WHEN modalidade_do_desligamento = 'Iniciativa da Empresa' THEN 1 ELSE 0 END) AS forcados,
  SUM(CASE WHEN modalidade_do_desligamento = 'Iniciativa do Seazoner' THEN 1 ELSE 0 END) AS voluntarios
FROM "nekt_silver"."all_cards_305642527_colunas_expandidas"
WHERE title != 'Teste'
  AND currentphasename IN ('Desligamento Realizado', 'Realizando Desligamento')
  AND data_do_desligamento IS NOT NULL
  AND data_do_desligamento != ''
  AND DATE_PARSE(data_do_desligamento, '%d/%m/%Y') >= DATE '2025-01-01'
GROUP BY DATE_FORMAT(DATE_PARSE(data_do_desligamento, '%d/%m/%Y'), '%Y-%m-%d')
ORDER BY data_desligamento
"""

# --- R&S: Vagas canceladas por mes ---
SQL_CANCELADAS = """
SELECT
  DATE_FORMAT(DATE(j.updatedat), '%Y-%m') AS mes,
  COUNT(DISTINCT j.id) AS vagas_canceladas
FROM "nekt_trusted"."inhire_jobs" j
WHERE j.status = 'canceled'
  AND j.updatedat >= TIMESTAMP '2025-01-01'
GROUP BY DATE_FORMAT(DATE(j.updatedat), '%Y-%m')
ORDER BY mes
"""

# --- Headcount: departamentos top 15 ---
SQL_DEPARTAMENTOS = """
SELECT
  e.department.name AS departamento,
  COUNT(*) AS headcount
FROM "nekt_trusted"."convenia_employees" e
GROUP BY e.department.name
ORDER BY headcount DESC
LIMIT 15
"""

# --- Headcount: por empresa ---
SQL_HC_EMPRESA = """
SELECT
  e.cost_center.name AS empresa,
  COUNT(*) AS headcount
FROM "nekt_trusted"."convenia_employees" e
GROUP BY e.cost_center.name
ORDER BY headcount DESC
"""

# --- Headcount total ---
SQL_HEADCOUNT = """
SELECT
  COUNT(*) AS headcount_total,
  COUNT(CASE WHEN status = 'Ativo' THEN 1 END) AS ativos,
  COUNT(CASE WHEN status = 'Em férias' THEN 1 END) AS em_ferias
FROM "nekt_trusted"."convenia_employees"
"""

# --- DP: Suportes People (em aberto — snapshot) ---
SQL_SUP_ABERTOS = """
SELECT COUNT(*) AS em_aberto
FROM "nekt_silver"."silver_pipefy_suporte_people"
WHERE current_phase_name NOT IN ('Concluído', 'Cancelado')
"""

# --- DP: Suportes People (novos por dia) ---
SQL_SUP_NOVOS = """
SELECT
  DATE(CAST(data_criacao AS TIMESTAMP)) AS data,
  COUNT(*) AS novos
FROM "nekt_silver"."silver_pipefy_suporte_people"
WHERE CAST(data_criacao AS TIMESTAMP) >= TIMESTAMP '2025-01-01'
GROUP BY 1
ORDER BY 1
"""

# --- DP: Suportes People (finalizados por dia) ---
SQL_SUP_FIN = """
SELECT
  DATE(CAST(data_conclusao AS TIMESTAMP)) AS data,
  COUNT(*) AS finalizados
FROM "nekt_silver"."silver_pipefy_suporte_people"
WHERE data_conclusao IS NOT NULL
  AND CAST(data_conclusao AS VARCHAR) != ''
  AND CAST(data_conclusao AS TIMESTAMP) >= TIMESTAMP '2025-01-01'
GROUP BY 1
ORDER BY 1
"""

# --- DP: Admissoes (status) ---
SQL_ADMISSOES = """
SELECT
  currentphasename,
  COUNT(*) AS total
FROM "nekt_service"."pipefy_all_cards_303470834_colunas_expandidas"
GROUP BY currentphasename
ORDER BY total DESC
"""

# --- R&S: Vagas abertas (resumo por recrutador) ---
SQL_POS_ABERTAS = """
SELECT
  u.name AS recrutador,
  COUNT(DISTINCT j.id) AS vagas,
  SUM(CASE WHEN p.hiredat IS NULL THEN 1 ELSE 0 END) AS posicoes_abertas
FROM "nekt_trusted"."inhire_job_details" j
LEFT JOIN "nekt_trusted"."inhire_usersusers" u ON j.recruiterid = u.id
LEFT JOIN "nekt_trusted"."inhire_positions" p ON p.jobid = j.id
WHERE j.status = 'open'
  AND j.recruiterid IS NOT NULL AND j.recruiterid != ''
  AND LOWER(j.name) NOT LIKE '%vaga teste%' AND LOWER(j.name) NOT LIKE '%vaga modelo%'
GROUP BY u.name
ORDER BY posicoes_abertas DESC
"""

# --- R&S: Vagas acima do SLA ---
SQL_ACIMA_SLA = """
SELECT
  COALESCE(u.name, 'Sem recrutador') AS recrutador,
  COUNT(DISTINCT j.id) AS vagas_acima_sla
FROM "nekt_trusted"."inhire_job_details" j
LEFT JOIN "nekt_trusted"."inhire_usersusers" u ON j.recruiterid = u.id
WHERE j.status = 'open'
  AND CAST(j.sla AS BIGINT) / 86400000 > CAST(j.sladaysgoal AS INTEGER)
  AND LOWER(j.name) NOT LIKE '%vaga teste%' AND LOWER(j.name) NOT LIKE '%vaga modelo%'
GROUP BY u.name
ORDER BY vagas_acima_sla DESC
"""

# --- R&S: SLA medio ---
SQL_SLA_MEDIO = """
SELECT ROUND(AVG(CAST(j.sla AS DOUBLE) / 86400000), 2) AS sla_medio_dias
FROM "nekt_trusted"."inhire_job_details" j
WHERE j.status = 'open'
  AND j.recruiterid IS NOT NULL AND j.recruiterid != ''
  AND LOWER(j.name) NOT LIKE '%vaga teste%' AND LOWER(j.name) NOT LIKE '%vaga modelo%'
"""

# --- R&S: Vagas por empresa ---
SQL_VAGAS_EMPRESA = """
SELECT
  j.tenantclient.name AS empresa,
  COUNT(DISTINCT j.id) AS vagas_abertas
FROM "nekt_trusted"."inhire_job_details" j
WHERE j.status = 'open'
  AND j.recruiterid IS NOT NULL AND j.recruiterid != ''
  AND LOWER(j.name) NOT LIKE '%vaga teste%' AND LOWER(j.name) NOT LIKE '%vaga modelo%'
GROUP BY j.tenantclient.name
"""

# --- R&S: Detalhe vagas abertas ---
SQL_VAGAS_DETALHE = """
SELECT
  j.name AS vaga,
  u.name AS recrutador,
  CAST(j.sla AS BIGINT) / 86400000 AS sla_dias,
  j.sladaysgoal AS sla_meta_dias,
  j.tenantclient.name AS empresa,
  COUNT(CASE WHEN p.hiredat IS NULL THEN 1 END) AS posicoes_abertas,
  COUNT(CASE WHEN p.hiredat IS NOT NULL THEN 1 END) AS posicoes_preenchidas,
  MIN(j.createdat) AS data_criacao,
  mgr.name AS gestor,
  MAX(req.justificativa_requisitions) AS observacao
FROM "nekt_trusted"."inhire_job_details" j
LEFT JOIN "nekt_trusted"."inhire_usersusers" u ON j.recruiterid = u.id
LEFT JOIN "nekt_trusted"."inhire_positions" p ON p.jobid = j.id
LEFT JOIN "nekt_trusted"."inhire_usersusers" mgr ON j.managerid = mgr.id
LEFT JOIN "nekt_silver"."inhire_lista_requisitions" req ON p.requisitionid = req.id
WHERE j.status = 'open'
  AND j.recruiterid IS NOT NULL AND j.recruiterid != ''
  AND LOWER(j.name) NOT LIKE '%vaga teste%' AND LOWER(j.name) NOT LIKE '%vaga modelo%'
GROUP BY j.name, u.name, j.sla, j.sladaysgoal, j.tenantclient.name, mgr.name
ORDER BY u.name, j.name
"""

# --- Banco de talentos ---
SQL_BANCO_TALENTOS = """
SELECT COUNT(DISTINCT t.talentid) AS total_banco_talentos
FROM "nekt_trusted"."inhire_job_talents" t
JOIN "nekt_trusted"."inhire_job_details" j ON SUBSTR(t.id, 1, 36) = j.id
WHERE CAST(j.istalentpool AS VARCHAR) = 'true'
"""

# --- % Aceitos entrevista final ---
SQL_PCT_ACEITOS = """
SELECT
  COUNT(DISTINCT talentid) AS total_entrevista_final,
  COUNT(DISTINCT CASE WHEN status = 'active' THEN talentid END) AS aceitos,
  ROUND(
    CAST(COUNT(DISTINCT CASE WHEN status = 'active' THEN talentid END) AS DOUBLE)
    / NULLIF(COUNT(DISTINCT talentid), 0) * 100, 2
  ) AS pct_aceitos
FROM "nekt_silver"."silver_fato_candidatos"
WHERE stage_name IN ('Entrevista Final', 'Entrevista Final com Diretoria')
  AND CAST(updatedat_date AS VARCHAR) >= '2026-01-29'
"""


# ============================================================
# COLETA
# ============================================================


def coletar_dados() -> dict:
    """Executa todas as queries e retorna dados brutos."""
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
    for nome, sql in queries.items():
        print(f"[sync] {nome}...")
        dados[nome] = nekt_query(sql)

    return dados


def fetch_medalhas() -> list[dict]:
    """Busca medalhas validadas do Google Sheets publico (CSV).
    Retorna lista de dicts com chaves: data, habilidade."""
    print("[sync] medalhas (Google Sheets CSV)...")
    try:
        resp = requests.get(MEDALHAS_CSV_URL, timeout=30, allow_redirects=True)
        resp.raise_for_status()
        reader = csv.reader(io.StringIO(resp.text))
        header = next(reader)
        # Encontrar indices das colunas
        col_data = None
        col_status = None
        col_habilidade = None
        for i, h in enumerate(header):
            hl = h.strip().lower()
            if hl == "data":
                col_data = i
            elif "aceito" in hl or "rejeitar" in hl:
                col_status = i
            elif "habilidade" in hl:
                col_habilidade = i

        if col_data is None or col_status is None:
            print("[sync] AVISO: colunas de medalhas nao encontradas")
            return []

        medalhas = []
        for row in reader:
            if len(row) <= max(col_data, col_status):
                continue
            status = row[col_status].strip()
            if status != "Aceitar":
                continue
            data_str = row[col_data].strip()
            if not data_str:
                continue
            # Data pode vir como dd/mm/yyyy ou yyyy-mm-dd
            try:
                if "/" in data_str:
                    dt = datetime.strptime(data_str, "%d/%m/%Y")
                else:
                    dt = datetime.strptime(data_str[:10], "%Y-%m-%d")
                medalhas.append({
                    "data": dt.strftime("%Y-%m-%d"),
                    "habilidade": row[col_habilidade].strip() if col_habilidade is not None and col_habilidade < len(row) else "",
                })
            except ValueError:
                continue

        print(f"[sync]   {len(medalhas)} medalhas validadas")
        return medalhas
    except Exception as e:
        print(f"[sync] ERRO ao buscar medalhas: {e}")
        return []


# ============================================================
# TRANSFORMACAO
# ============================================================


def mes_label(iso: str) -> str:
    """2025-01-01 00:00:00.000 UTC -> Jan/25"""
    nomes = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
             "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    parts = iso[:10].split("-")
    m = int(parts[1])
    y = parts[0][2:]
    return f"{nomes[m - 1]}/{y}"


def transformar(dados: dict) -> dict:
    """Transforma dados brutos no formato que o dashboard espera."""
    hoje = data_referencia_brt().isoformat()

    # --- Entrevistas: tuples [date, recruiter, count] ---
    entrevistas_raw = []
    for r in dados["entrevistas"]:
        entrevistas_raw.append([r["data"][:10], r["recrutador"], int(r["entrevistas"])])

    # --- Contratacoes por mes ---
    meses_set = set()
    cont_map = {}  # {mes: {rec: count}}
    for r in dados["contratacoes_mes"]:
        m = mes_label(r["mes"])
        meses_set.add(r["mes"][:10])
        cont_map.setdefault(m, {"Clara": 0, "Jonas": 0, "Outro": 0})
        rec = r["recrutador"]
        if rec in ("Clara", "Jonas"):
            cont_map[m][rec] += int(r["contratacoes"])
        else:
            cont_map[m]["Outro"] += int(r["contratacoes"])

    meses_sorted = sorted(meses_set)
    cont_meses = [mes_label(m) for m in meses_sorted]
    cont_clara = [cont_map.get(m, {}).get("Clara", 0) for m in cont_meses]
    cont_jonas = [cont_map.get(m, {}).get("Jonas", 0) for m in cont_meses]
    cont_outro = [cont_map.get(m, {}).get("Outro", 0) for m in cont_meses]

    # --- Desligamentos: tuples [date, total, forcados, voluntarios] ---
    deslig_raw = []
    for r in dados["desligamentos"]:
        deslig_raw.append([
            r["data_desligamento"][:10],
            int(r["total"]),
            int(r["forcados"]),
            int(r["voluntarios"]),
        ])

    # --- Canceladas por mes ---
    canc_meses = []
    canc_vals = []
    for r in dados["canceladas"]:
        canc_meses.append(mes_label(r["mes"] + "-01"))
        canc_vals.append(int(r["vagas_canceladas"]))

    # --- Departamentos ---
    depts = [r["departamento"] for r in dados["departamentos"]]
    dept_vals = [int(r["headcount"]) for r in dados["departamentos"]]

    # --- Suportes: tuples [date, count] ---
    sup_novos_raw = [[r["data"][:10], int(r["novos"])] for r in dados["sup_novos"]]
    sup_fin_raw = [[r["data"][:10], int(r["finalizados"])] for r in dados["sup_fin"]]
    sup_abertos = int(dados["sup_abertos"][0]["em_aberto"]) if dados["sup_abertos"] else 0

    # --- Medalhas: tuples [date, count] ---
    medalhas_raw = []
    medalhas_por_dia = {}
    for m in dados.get("medalhas", []):
        dt = m["data"]
        medalhas_por_dia[dt] = medalhas_por_dia.get(dt, 0) + 1
    for dt in sorted(medalhas_por_dia):
        medalhas_raw.append([dt, medalhas_por_dia[dt]])
    medalhas_total = sum(v for v in medalhas_por_dia.values())

    # --- Headcount ---
    hc = dados["headcount"][0] if dados["headcount"] else {}
    hc_total = int(hc.get("headcount_total", 0))
    hc_ativos = int(hc.get("ativos", 0))
    hc_ferias = int(hc.get("em_ferias", 0))

    hc_emp = {}
    for r in dados["hc_empresa"]:
        hc_emp[r["empresa"]] = int(r["headcount"])

    # --- Admissoes ---
    adm = {}
    for r in dados["admissoes"]:
        adm[r["currentphasename"]] = int(r["total"])
    adm_total = sum(adm.values())
    adm_concluidas = adm.get("Admissões concluídas", 0)
    adm_canceladas = adm.get("Admissões canceladas", 0)
    adm_aberto = adm_total - adm_concluidas - adm_canceladas

    # --- Posicoes abertas ---
    pos_por_rec = {}
    pos_total = 0
    vagas_por_rec = {}
    for r in dados["pos_abertas"]:
        nome = RECRUTADORES_NOMES.get(r["recrutador"], r["recrutador"])
        pos = int(r["posicoes_abertas"])
        vagas = int(r["vagas"])
        pos_por_rec[nome] = pos
        vagas_por_rec[nome] = vagas
        pos_total += pos

    # --- SLA ---
    sla_por_rec = {}
    sla_total = 0
    for r in dados["acima_sla"]:
        nome = RECRUTADORES_NOMES.get(r["recrutador"], r["recrutador"])
        v = int(r["vagas_acima_sla"])
        sla_por_rec[nome] = v
        sla_total += v

    sla_medio = float(dados["sla_medio"][0]["sla_medio_dias"]) if dados["sla_medio"] else 0

    # --- Vagas por empresa ---
    vagas_emp = {}
    for r in dados["vagas_empresa"]:
        vagas_emp[r["empresa"]] = int(r["vagas_abertas"])

    # --- Detalhe vagas abertas ---
    vagas_detalhe = []
    for r in dados["vagas_detalhe"]:
        nome_rec = RECRUTADORES_NOMES.get(r["recrutador"], r["recrutador"])
        sla_d = int(r["sla_dias"])
        sla_m = int(r["sla_meta_dias"])
        # Extrair prioridade do nome da vaga via regex
        vaga_nome = r["vaga"]
        prio_match = re.search(r'\[\s*(Máxima|Média|Baixa)\s*\]', vaga_nome, re.IGNORECASE)
        prioridade = prio_match.group(1).capitalize() if prio_match else "Média"
        # Limpar nome removendo tag de prioridade
        vaga_limpo = re.sub(r'\[\s*(?:Máxima|Média|Baixa)\s*\]\s*', '', vaga_nome, flags=re.IGNORECASE).strip()

        # Extrair ID(s) Cadeira da observacao (justificativa da requisicao)
        obs = r.get("observacao", "") or ""
        cadeira_matches = re.findall(r'[A-Z]{2,10}-?\d{2,4}', obs)
        id_cadeira = ", ".join(cadeira_matches) if cadeira_matches else ""

        vagas_detalhe.append({
            "vaga": vaga_limpo,
            "recrutador": nome_rec,
            "sla_dias": sla_d,
            "sla_meta": sla_m,
            "acima_sla": sla_d > sla_m,
            "empresa": r["empresa"] or "",
            "prioridade": prioridade,
            "posicoes_abertas": int(r["posicoes_abertas"]),
            "data_criacao": r["data_criacao"][:10] if r.get("data_criacao") else "",
            "gestor": r.get("gestor") or "",
            "id_cadeira": id_cadeira,
        })

    # --- Banco de talentos ---
    bt = int(dados["banco_talentos"][0]["total_banco_talentos"]) if dados["banco_talentos"] else 0

    # --- % Aceitos ---
    pct = dados["pct_aceitos"][0] if dados["pct_aceitos"] else {}
    pct_aceitos = float(pct.get("pct_aceitos", 0))

    return {
        "entrevistas_raw": entrevistas_raw,
        "cont_meses": cont_meses,
        "cont_clara": cont_clara,
        "cont_jonas": cont_jonas,
        "cont_outro": cont_outro,
        "deslig_raw": deslig_raw,
        "canc_meses": canc_meses,
        "canc_vals": canc_vals,
        "depts": depts,
        "dept_vals": dept_vals,
        "sup_novos_raw": sup_novos_raw,
        "sup_fin_raw": sup_fin_raw,
        "medalhas_raw": medalhas_raw,
        "vagas_detalhe": vagas_detalhe,
        "resumo": {
            "posAbertas": pos_total,
            "posAcimaSla": sla_total,
            "slaMedio": sla_medio,
            "pctAceitosFinal": pct_aceitos,
            "bancotalentos": bt,
            "headcount": hc_total,
            "hcAtivos": hc_ativos,
            "hcFerias": hc_ferias,
            "hcSZS": hc_emp.get("Seazone Serviços", 0),
            "hcSZI": hc_emp.get("Seazone Investimentos", 0),
            "hcGO": hc_emp.get("SZN Gestão de Obras", 0),
            "hcHolding": hc_emp.get("Seazone Holding", 0),
            "vagasSZS": vagas_emp.get("Seazone Serviços", 0),
            "vagasSZI": vagas_emp.get("Seazone Investimentos", 0),
            "vagasGO": vagas_emp.get("Seazone Gestao de Obras", 0) or vagas_emp.get("Seazone Gestão de Obras", 0) or vagas_emp.get("SZN Gestão de Obras", 0) or vagas_emp.get("SZN Gestao de Obras", 0),
            "supAbertos": sup_abertos,
            "admTotal": adm_total,
            "admAberto": adm_aberto,
            "admConcluidas": adm_concluidas,
            "admCanceladas": adm_canceladas,
            "medalhasTotal": medalhas_total,
            "clara": {"posAbertas": pos_por_rec.get("Clara", 0), "acimaSla": sla_por_rec.get("Clara", 0), "vagas": vagas_por_rec.get("Clara", 0)},
            "jonas": {"posAbertas": pos_por_rec.get("Jonas", 0), "acimaSla": sla_por_rec.get("Jonas", 0), "vagas": vagas_por_rec.get("Jonas", 0)},
            "julia": {"posAbertas": pos_por_rec.get("Júlia", 0), "acimaSla": sla_por_rec.get("Júlia", 0), "vagas": vagas_por_rec.get("Júlia", 0)},
            "mario": {"posAbertas": pos_por_rec.get("Mario", 0), "acimaSla": sla_por_rec.get("Mario", 0), "vagas": vagas_por_rec.get("Mario", 0)},
        },
        "hoje": hoje,
    }


# ============================================================
# GERA data.js
# ============================================================


def build_datajs(t: dict) -> str:
    """Gera o conteudo do data.js."""
    agora = datetime.now(BRT).strftime("%d/%m/%Y")
    js = lambda v: json.dumps(v, ensure_ascii=False)

    return f"""// data.js — KPIs People Inhire
// Gerado automaticamente pelo sync.py
// Ultima atualizacao: {agora}

window.KPI_DATA = {{
  ultima_atualizacao: '{agora}',
  todayISO: '{t["hoje"]}',

  // Entrevistas: [data_iso, recrutador, quantidade]
  entrevistasRaw: {js(t['entrevistas_raw'])},

  // Contratacoes por mes
  contMeses: {js(t['cont_meses'])},
  contClara: {js(t['cont_clara'])},
  contJonas: {js(t['cont_jonas'])},
  contOutro: {js(t['cont_outro'])},

  // Desligamentos: [data_iso, total, forcados, voluntarios]
  desligamentosRaw: {js(t['deslig_raw'])},

  // Vagas canceladas por mes
  cancMeses: {js(t['canc_meses'])},
  cancVals: {js(t['canc_vals'])},

  // Departamentos top 15
  depts: {js(t['depts'])},
  deptVals: {js(t['dept_vals'])},

  // Suportes People: [data_iso, quantidade]
  supNovosRaw: {js(t['sup_novos_raw'])},
  supFinRaw: {js(t['sup_fin_raw'])},

  // Detalhe vagas abertas
  vagasDetalhe: {js(t['vagas_detalhe'])},

  // Resumo (snapshot atual)
  resumo: {js(t['resumo'])},

  // Historico de execucoes
  history: {js(t.get('history', []))}
}};
"""


# ============================================================
# HISTORY TRACKING
# ============================================================


def update_history(transformado: dict, errors: list[dict]) -> list[dict]:
    """Appends a new entry to history.json and returns the history list."""
    # Read existing history
    history = []
    if os.path.exists(HISTORY_JSON_PATH):
        try:
            with open(HISTORY_JSON_PATH, "r", encoding="utf-8") as f:
                history = json.load(f)
        except (json.JSONDecodeError, IOError):
            history = []

    resumo = transformado.get("resumo", {})
    entry = {
        "data_hora": datetime.now(BRT).strftime("%d/%m/%Y %H:%M"),
        "status": "success" if len(errors) == 0 else "error",
        "posicoes_abertas": resumo.get("posAbertas", 0),
        "acima_sla": resumo.get("posAcimaSla", 0),
        "sla_medio": resumo.get("slaMedio", 0),
        "headcount": resumo.get("headcount", 0),
        "hc_ativos": resumo.get("hcAtivos", 0),
        "hc_ferias": resumo.get("hcFerias", 0),
        "banco_talentos": resumo.get("bancotalentos", 0),
        "pct_aceitos": resumo.get("pctAceitosFinal", 0),
        "adm_aberto": resumo.get("admAberto", 0),
        "adm_concluidas": resumo.get("admConcluidas", 0),
        "adm_canceladas": resumo.get("admCanceladas", 0),
        "erros": len(errors),
        "erros_detalhe": errors,
    }

    history.append(entry)

    # Keep max 365 entries
    if len(history) > 365:
        history = history[-365:]

    # Write back
    os.makedirs(os.path.dirname(HISTORY_JSON_PATH), exist_ok=True)
    with open(HISTORY_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    return history


# ============================================================
# GOOGLE SHEETS
# ============================================================


def update_google_sheets(transformado: dict):
    """Escreve KPIs do dia na planilha Google Sheets."""
    if not GSHEETS_CREDS_JSON:
        print("[sheets] GSHEETS_CREDS_JSON nao definida, pulando")
        return

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("[sheets] gspread/google-auth nao instalados, pulando")
        return

    # Credenciais via JSON string (GitHub secret) ou arquivo local
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if os.path.isfile(GSHEETS_CREDS_JSON):
        creds = Credentials.from_service_account_file(GSHEETS_CREDS_JSON, scopes=scopes)
    else:
        creds_dict = json.loads(GSHEETS_CREDS_JSON)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSHEETS_SPREADSHEET_ID)
    ws = sh.worksheet(GSHEETS_TAB_NAME)

    hoje = data_referencia_brt()
    hoje_str = hoje.strftime("%d/%m/%Y")
    hoje_iso = hoje.isoformat()
    resumo = transformado["resumo"]

    # --- Valores diarios das series temporais ---
    def sum_date(raw, dt):
        return sum(r[-1] for r in raw if r[0] == dt)

    def sum_date_rec(raw, dt, rec):
        return sum(r[2] for r in raw if r[0] == dt and r[1] == rec)

    ent_total = sum_date(transformado["entrevistas_raw"], hoje_iso)
    ent_clara = sum_date_rec(transformado["entrevistas_raw"], hoje_iso, "Clara")
    ent_jonas = sum_date_rec(transformado["entrevistas_raw"], hoje_iso, "Jonas")
    ent_julia = sum_date_rec(transformado["entrevistas_raw"], hoje_iso, "Júlia")

    deslig = [r for r in transformado["deslig_raw"] if r[0] == hoje_iso]
    deslig_total = sum(r[1] for r in deslig)
    deslig_forc = sum(r[2] for r in deslig)
    deslig_vol = sum(r[3] for r in deslig)

    sup_novos = sum_date(transformado["sup_novos_raw"], hoje_iso)
    sup_fin = sum_date(transformado["sup_fin_raw"], hoje_iso)
    medalhas_dia = sum_date(transformado["medalhas_raw"], hoje_iso)

    # --- Mapeamento: linha (0-indexed) -> valor ---
    values_map = {
        1: hoje_str,                                # Data
        5: resumo["bancotalentos"],                 # Banco de Talentos
        8: resumo["posAbertas"],                    # Posicoes Abertas Total
        9: resumo["clara"]["posAbertas"],           # Posicoes Abertas Clara
        10: resumo["jonas"]["posAbertas"],          # Posicoes Abertas Jonas
        11: resumo["julia"]["posAbertas"],          # Posicoes Abertas Julia
        12: resumo["posAcimaSla"],                  # Acima SLA Total
        13: resumo["clara"]["acimaSla"],            # Acima SLA Clara
        14: resumo["jonas"]["acimaSla"],            # Acima SLA Jonas
        15: resumo["julia"]["acimaSla"],            # Acima SLA Julia
        16: resumo["vagasSZS"],                     # Vagas SZS
        17: resumo["vagasSZI"],                     # Vagas SZI
        22: ent_total,                              # Entrevistas Total
        23: ent_clara,                              # Entrevistas Clara
        24: ent_jonas,                              # Entrevistas Jonas
        25: ent_julia,                              # Entrevistas Julia
        26: str(resumo["pctAceitosFinal"]).replace(".", ",") + "%",
        34: sup_novos,                              # Suportes Novos
        35: resumo["supAbertos"],                   # Suportes em Aberto
        36: sup_fin,                                # Suportes Finalizados
        37: resumo["admAberto"],                    # Admissoes em aberto
        43: deslig_total,                           # Desligamentos Total
        44: deslig_forc,                            # Desligamentos Forcados
        45: deslig_vol,                             # Desligamentos Voluntarios
        46: medalhas_dia,                             # Medalhas Validadas
    }

    # --- Encontrar coluna destino ---
    all_vals = ws.get_all_values()
    max_cols = max(len(r) for r in all_vals) if all_vals else 1

    # Se hoje ja tem coluna, sobrescreve
    row1 = all_vals[1] if len(all_vals) > 1 else []
    col_idx = None
    for c, val in enumerate(row1):
        if val == hoje_str:
            col_idx = c
            break

    # Senao, proxima coluna vazia
    if col_idx is None:
        col_idx = 1
        for c in range(1, max_cols + 1):
            has_value = any(c < len(r) and r[c].strip() for r in all_vals)
            if not has_value:
                col_idx = c
                break
            col_idx = c + 1

    # --- Escrever ---
    from gspread.utils import rowcol_to_a1

    cells = []
    for row_idx, value in sorted(values_map.items()):
        cell = rowcol_to_a1(row_idx + 1, col_idx + 1)
        cells.append({"range": cell, "values": [[value]]})

    ws.batch_update(cells, value_input_option="USER_ENTERED")

    col_letter = chr(65 + col_idx) if col_idx < 26 else chr(64 + col_idx // 26) + chr(65 + col_idx % 26)
    print(f"[sheets] {len(cells)} KPIs escritos na coluna {col_letter} ({hoje_str}) da aba '{GSHEETS_TAB_NAME}'")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    print(f"[sync] Iniciando — {datetime.now(BRT).strftime('%d/%m/%Y %H:%M')}")

    # Collect data with per-query error handling
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
            print(f"[sync] {nome}...")
            dados[nome] = nekt_query(sql)
        except Exception as e:
            print(f"[ERRO] Query '{nome}' falhou: {e}", file=sys.stderr)
            dados[nome] = []
            errors.append({"query": nome, "error": str(e)})

    # Medalhas vem do Google Sheets, nao do Nekt
    try:
        dados["medalhas"] = fetch_medalhas()
    except Exception as e:
        print(f"[ERRO] Medalhas falhou: {e}", file=sys.stderr)
        dados["medalhas"] = []
        errors.append({"query": "medalhas", "error": str(e)})

    transformado = transformar(dados)

    # Update history and attach to output
    history = update_history(transformado, errors)
    transformado["history"] = history

    content = build_datajs(transformado)

    with open(DATA_JS_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    r = transformado["resumo"]
    print(f"[sync] data.js atualizado em: {DATA_JS_PATH}")
    print(f"[sync] Resumo: {r['posAbertas']} pos abertas, {r['posAcimaSla']} acima SLA, HC {r['headcount']}")
    if errors:
        print(f"[sync] ATENCAO: {len(errors)} queries falharam: {[e['query'] for e in errors]}")

    # Atualizar Google Sheets
    try:
        update_google_sheets(transformado)
    except Exception as e:
        print(f"[sheets] ERRO ao atualizar planilha: {e}", file=sys.stderr)

    print("[sync] Concluido.")
