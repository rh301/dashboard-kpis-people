# Dashboard KPIs People — Seazone

Dashboard web com KPIs do time de People (R&S, DP, Cultura), atualizado automaticamente todo dia às 22h.

## Arquitetura

```
GitHub Actions (cron 22h BRT)
  └── sync.py → Nekt API (SQL) → public/data.js
Vercel (auto-deploy)
  └── public/dashboard.html + data.js
```

## Setup

### 1. Secrets do GitHub

| Secret | Uso |
|--------|-----|
| `NEKT_API_KEY` | API key do Nekt para executar queries SQL |

### 2. Vercel

Conecte o repo ao Vercel com:
- **Framework Preset**: Other
- **Root Directory**: `public`
- **Build Command**: (vazio)
- **Output Directory**: `.`

### 3. Execução manual

```bash
NEKT_API_KEY=xxx python sync.py
```

## Estrutura

```
├── sync.py                    # Script de sincronização (GitHub Actions)
├── requirements.txt
├── .github/workflows/sync.yml # Workflow diário
└── public/
    ├── dashboard.html         # Dashboard estático
    └── data.js                # Dados gerados (auto-commit)
```
