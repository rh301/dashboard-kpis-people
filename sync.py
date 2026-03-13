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
from datetime import date, datetime

import requests

# ============================================================
# CONFIGURACAO
# ============================================================

NEKT_API_URL = "https://api.nekt.ai/api/v1/sql-query/"
NEKT_API_KEY = os.environ.get("NEKT_API_KEY", "")
DATA_JS_PATH = os.path.join(os.path.dirname(__file__), "public", "data.js")
HISTORY_JSON_PATH = os.path.join(os.path.dirname(__file__), "public", "history.json")

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

# --- Cultura: Desligamentos por dia (historico completo) ---
SQL_DESLIGAMENTOS = """
SELECT
  DATE(dismissal_date) AS data_desligamento,
  COUNT(*) AS total,
  SUM(CASE WHEN CAST(dismissal_type_id AS VARCHAR) IN ('1','2','18','14') THEN 1 ELSE 0 END) AS forcados,
  SUM(CASE WHEN CAST(dismissal_type_id AS VARCHAR) IN ('3','19','6','13') THEN 1 ELSE 0 END) AS voluntarios
FROM "nekt_silver"."bd_rh_convenia_dismissed_normalizado"
WHERE dismissal_date >= '2025-01-01'
GROUP BY DATE(dismissal_date)
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
    hoje = date.today().isoformat()

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
            "admTotal": adm_total,
            "admAberto": adm_aberto,
            "admConcluidas": adm_concluidas,
            "admCanceladas": adm_canceladas,
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
    agora = datetime.now().strftime("%d/%m/%Y")
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
        "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M"),
        "status": "success" if len(errors) == 0 else "error",
        "posicoes_abertas": resumo.get("posAbertas", 0),
        "headcount": resumo.get("headcount", 0),
        "sla_medio": resumo.get("slaMedio", 0),
        "cfo_atualizado": False,
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
# MAIN
# ============================================================

if __name__ == "__main__":
    print(f"[sync] Iniciando — {datetime.now().strftime('%d/%m/%Y %H:%M')}")

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
    print("[sync] Concluido.")
