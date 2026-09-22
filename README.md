# Grupo ABR — Margin Processor

Serviço de ingestão para as planilhas de Margem e Gestão da Produção.

## Fluxo
1. Apps Script envia o XLSX para a pasta correta no Google Drive.
2. Apps Script chama `POST /v1/process` e envia um token OAuth temporário do Drive.
3. O serviço baixa o XLSX diretamente do Google Drive.
4. `MARGEM`: processa `BD`, `BD_Meta`, `TD_Meta`, `Apoio` e `Tabela de Preço`.
5. `GESTAO_PRODUCAO`: processa `Gestao da ProdV6`.
6. Grava no Supabase/Postgres.
7. Ativa a nova importação somente quando o processamento termina.
8. O cruzamento fica disponível em `reconciliacao_faturamento` e `pendencias_cruzamento`.

## Render
- Runtime: Python
- Build: `pip install -r requirements.txt`
- Start: `uvicorn app:app --host 0.0.0.0 --port $PORT`

## Variáveis
- `DATABASE_URL`: connection string Postgres do projeto Supabase.

## Endpoints
- `GET /health`
- `POST /v1/process`
- `GET /v1/jobs/{job_id}`
- `GET /v1/imports/active`
- `GET /v1/reconciliation/summary`
- `GET /v1/reconciliation/pending`
