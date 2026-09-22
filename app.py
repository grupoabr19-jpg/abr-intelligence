from __future__ import annotations

import hashlib
import os
import tempfile
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
import psycopg
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from psycopg.types.json import Jsonb

from xlsx_stream import (
    XlsxStream, api_field_name, col_letter, excel_date, normalize_invoice,
    normalize_key, numeric, raw_from_row,
)

DATABASE_URL = os.environ.get('DATABASE_URL', '').strip()
APP_NAME = 'Grupo ABR Margin Processor'

MIRROR_URL = os.environ.get('MIRROR_URL', 'https://yppxtnrpvxeqmdqzsrnj.supabase.co/functions/v1/ingestao-sheets').strip()
MIRROR_TOKEN = os.environ.get('MIRROR_TOKEN', '').strip()
MIRROR_TOKEN_LOCATION = os.environ.get('MIRROR_TOKEN_LOCATION', 'body').strip().lower()
MIRROR_TOKEN_FIELD = os.environ.get('MIRROR_TOKEN_FIELD', 'token').strip() or 'token'
try:
    MIRROR_BATCH_SIZE = max(100, min(2000, int(os.environ.get('MIRROR_BATCH_SIZE', '500'))))
except ValueError:
    MIRROR_BATCH_SIZE = 500

app = FastAPI(title=APP_NAME, version='1.0.0')


class ProcessRequest(BaseModel):
    import_id: str
    source_type: str = Field(pattern='^(MARGEM|GESTAO_PRODUCAO)$')
    drive_file_id: str
    drive_file_name: str
    drive_file_size_bytes: int | None = None
    drive_access_token: str
    spreadsheet_id: str | None = None
    drive_folder_id: str | None = None


class MirrorContext(BaseModel):
    import_id: str
    source_type: str = Field(pattern='^(MARGEM|GESTAO_PRODUCAO)$')


class MirrorReplayRequest(BaseModel):
    source_type: str = Field(default='ALL', pattern='^(ALL|MARGEM|GESTAO_PRODUCAO)$')
    force: bool = False


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError('DATABASE_URL não configurada.')
    # As cargas XLSX usam COPY de dezenas de milhares de linhas.
    # O Supabase deste projeto usa statement_timeout=2min por padrão,
    # portanto o processador abre suas próprias sessões com 15 minutos.
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=20,
        options='-c statement_timeout=900000 -c lock_timeout=30000'
    )


def utcnow():
    return datetime.now(timezone.utc)


def update_job(job_id: str, *, status=None, progress=None, message=None, rows_processed=None, finished=False):
    sets = ['updated_at = now()']
    params = []
    if status is not None:
        sets.append('status = %s'); params.append(status)
    if progress is not None:
        sets.append('progress = %s'); params.append(progress)
    if message is not None:
        sets.append('message = %s'); params.append(message)
    if rows_processed is not None:
        sets.append('rows_processed = %s'); params.append(rows_processed)
    if finished:
        sets.append('finished_at = now()')
    params.append(job_id)
    with db_conn() as conn:
        conn.execute(f"update ingest.jobs set {', '.join(sets)} where job_id = %s", params)
        conn.commit()


def upsert_import(payload: ProcessRequest, job_id: str, sha256: str | None = None, status='PROCESSING', message=None):
    metadata = {
        'spreadsheet_id': payload.spreadsheet_id,
        'drive_folder_id': payload.drive_folder_id,
    }
    with db_conn() as conn:
        conn.execute(
            """
            insert into ingest.importacoes (
              import_id, source_type, file_name, drive_file_id, file_size_bytes,
              sha256, job_id, status, is_active, processor_message, metadata,
              processing_started_at, uploaded_at
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,false,%s,%s,now(),now())
            on conflict (import_id) do update set
              source_type = excluded.source_type,
              file_name = excluded.file_name,
              drive_file_id = excluded.drive_file_id,
              file_size_bytes = excluded.file_size_bytes,
              sha256 = coalesce(excluded.sha256, ingest.importacoes.sha256),
              job_id = excluded.job_id,
              status = excluded.status,
              processor_message = excluded.processor_message,
              metadata = ingest.importacoes.metadata || excluded.metadata,
              processing_started_at = coalesce(ingest.importacoes.processing_started_at, now())
            """,
            (
                payload.import_id, payload.source_type, payload.drive_file_name,
                payload.drive_file_id, payload.drive_file_size_bytes or 0,
                sha256, job_id, status, message, Jsonb(metadata),
            )
        )
        conn.commit()


def download_drive_file(payload: ProcessRequest, job_id: str, dest: Path):
    update_job(job_id, status='DOWNLOADING', progress=3, message='Baixando XLSX do Google Drive...')
    url = f'https://www.googleapis.com/drive/v3/files/{payload.drive_file_id}?alt=media&supportsAllDrives=true'
    headers = {'Authorization': f'Bearer {payload.drive_access_token}'}
    sha = hashlib.sha256()
    total = 0
    with httpx.Client(timeout=None, follow_redirects=True) as client:
        with client.stream('GET', url, headers=headers) as resp:
            resp.raise_for_status()
            with dest.open('wb') as fh:
                for chunk in resp.iter_bytes(1024 * 1024):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    sha.update(chunk)
                    total += len(chunk)
    if payload.drive_file_size_bytes and total != payload.drive_file_size_bytes:
        raise RuntimeError(
            f'Tamanho baixado ({total}) difere do Drive ({payload.drive_file_size_bytes}).'
        )
    return sha.hexdigest(), total


def detect_duplicate(source_type: str, sha256: str, import_id: str):
    with db_conn() as conn:
        row = conn.execute(
            """
            select import_id from ingest.importacoes
            where source_type = %s and sha256 = %s and status = 'READY' and import_id <> %s
            order by completed_at desc nulls last limit 1
            """,
            (source_type, sha256, import_id)
        ).fetchone()
        return row[0] if row else None


def populate_dictionary(conn, import_id: str, sheet: str, source_table: str, headers: dict[int, str | None], expose=True):
    conn.execute('delete from ingest.field_dictionary where import_id=%s and source_sheet=%s', (import_id, sheet))
    rows = []
    for idx in sorted(headers):
        header = headers[idx]
        if header is None:
            continue
        rows.append((
            import_id, sheet, source_table, idx, header,
            api_field_name(header, f'col_{idx}'), 'PRIMARY', None,
            False, False, expose, None
        ))
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into ingest.field_dictionary (
              import_id, source_sheet, source_table, source_column, original_header,
              api_field, classification, detected_type, has_formula, has_cached_value,
              expose_api, notes
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (import_id, source_sheet, source_column) do update set
              original_header=excluded.original_header,
              api_field=excluded.api_field,
              classification=excluded.classification,
              expose_api=excluded.expose_api
            """, rows
        )


def company_key_from_margin(value):
    key = normalize_key(value)
    if key in ('STEEL', 'STEEL MATRIZ', 'STEEL MINAS'):
        return 'STEEL MATRIZ'
    return key


def company_key_from_production(value):
    key = normalize_key(value)
    if key == 'STEEL MINAS':
        return 'STEEL MATRIZ'
    return key


def process_margin(path: Path, payload: ProcessRequest, job_id: str):
    with XlsxStream(path) as xlsx:
        required = {'BD', 'BD_Meta'}
        found_sheets = set(xlsx.sheet_names())
        missing = required - found_sheets
        if missing:
            summary_signature = {'Por família', 'Por vendedor', 'Meta'}
            if summary_signature.issubset(found_sheets):
                raise RuntimeError(
                    'Arquivo de RESUMO detectado, não a base detalhada de Margem. '
                    'Para o cruzamento com Gestão da Produção, envie o XLSX completo que contém '
                    'as abas BD e BD_Meta. Abas encontradas: ' + ', '.join(sorted(found_sheets))
                )
            raise RuntimeError(
                'Formato de Margem incompatível. O arquivo completo precisa conter BD e BD_Meta. '
                'Abas encontradas: ' + ', '.join(sorted(found_sheets))
            )

        bd_headers = xlsx.header_map('BD', 2)
        meta_headers = xlsx.header_map('BD_Meta', 1)
        bd_bounds = xlsx.table_row_bounds('Realizado')
        meta_bounds = xlsx.table_row_bounds('Meta')
        bd_last_row = bd_bounds[1] if bd_bounds else None
        meta_last_row = meta_bounds[1] if meta_bounds else None

        with db_conn() as conn:
            conn.execute('delete from core.realizado where import_id=%s', (payload.import_id,))
            conn.execute('delete from core.meta where import_id=%s', (payload.import_id,))
            conn.execute('delete from ingest.raw_excel where import_id=%s', (payload.import_id,))
            populate_dictionary(conn, payload.import_id, 'BD', 'Realizado', bd_headers, True)
            populate_dictionary(conn, payload.import_id, 'BD_Meta', 'Meta', meta_headers, True)
            conn.commit()

        realized_cols = (
            'import_id','source_row','company_key','invoice_number_key','product_key','client_key',
            'data_emissao','empresa','numero_nf','codigo_produto','produto','cod_cliente','cliente',
            'vendedor','regiao','mercado','quantidade_nf','receita_bruta','quantidade_devolvida',
            'devolucoes','fretes','mcii','raw_data'
        )
        realized_sql = 'copy core.realizado (' + ','.join(realized_cols) + ') from stdin'

        count_real = 0
        realized_batch = []
        with db_conn() as conn:
            for row_num, v, formulas in xlsx.iter_rows('BD'):
                if row_num < 3:
                    continue
                if bd_last_row is not None and row_num > bd_last_row:
                    break
                if not v:
                    continue
                raw = raw_from_row(v, bd_headers)
                realized_batch.append((
                    payload.import_id, row_num,
                    company_key_from_margin(v.get(20)),
                    normalize_invoice(v.get(9)),
                    normalize_key(v.get(6)),
                    normalize_key(v.get(12)),
                    excel_date(v.get(3)),
                    None if v.get(20) is None else str(v.get(20)),
                    None if v.get(9) is None else str(v.get(9)),
                    None if v.get(6) is None else str(v.get(6)),
                    None if v.get(7) is None else str(v.get(7)),
                    None if v.get(11) is None else str(v.get(11)),
                    None if v.get(12) is None else str(v.get(12)),
                    None if v.get(19) is None else str(v.get(19)),
                    None if v.get(18) is None else str(v.get(18)),
                    None if v.get(88) is None else str(v.get(88)),
                    numeric(v.get(25)), numeric(v.get(26)), numeric(v.get(34)),
                    numeric(v.get(35)), numeric(v.get(46)), numeric(v.get(47)),
                    Jsonb(raw)
                ))
                count_real += 1

                if len(realized_batch) >= 4000:
                    with conn.cursor() as cur, cur.copy(realized_sql) as cp:
                        for item in realized_batch:
                            cp.write_row(item)
                    conn.commit()
                    realized_batch.clear()
                    update_job(
                        job_id,
                        progress=min(55, 20 + count_real / 1800),
                        rows_processed=count_real,
                        message=f'BD: {count_real:,} linhas processadas'.replace(',', '.')
                    )

            if realized_batch:
                with conn.cursor() as cur, cur.copy(realized_sql) as cp:
                    for item in realized_batch:
                        cp.write_row(item)
                conn.commit()
                realized_batch.clear()

        meta_cols = (
            'import_id','source_row','ano_mes','unidade','mercado','deposito','codigo_produto',
            'descricao_material','um','peso_produto','familia','subgrupo','classe','espessura',
            'fornecedor_base','regiao','vendedor','volume','preco_bruto','desconto_comercial',
            'preco_bruto_venda','receita_bruta','impostos_venda','impostos','receita_liquida',
            'custo_mp','total_imposto','custo_liquido','custo_insumos','custo_variavel','mci',
            'mci_pct','frete_pct','comissao_pct','frete','comissao','despesas_variaveis','mcii',
            'mcii_pct','mod_un','mod','mciii','mciii_pct','raw_data'
        )
        meta_sql = 'copy core.meta (' + ','.join(meta_cols) + ') from stdin'
        count_meta = 0
        meta_batch = []
        with db_conn() as conn:
            for row_num, v, formulas in xlsx.iter_rows('BD_Meta'):
                if row_num < 2:
                    continue
                if meta_last_row is not None and row_num > meta_last_row:
                    break
                if not v:
                    continue
                raw = raw_from_row(v, meta_headers)
                meta_batch.append((
                    payload.import_id, row_num,
                    str(v.get(1)) if v.get(1) is not None else None,
                    str(v.get(2)) if v.get(2) is not None else None,
                    str(v.get(3)) if v.get(3) is not None else None,
                    str(v.get(4)) if v.get(4) is not None else None,
                    str(v.get(5)) if v.get(5) is not None else None,
                    str(v.get(6)) if v.get(6) is not None else None,
                    str(v.get(7)) if v.get(7) is not None else None,
                    numeric(v.get(8)),
                    str(v.get(9)) if v.get(9) is not None else None,
                    str(v.get(10)) if v.get(10) is not None else None,
                    str(v.get(11)) if v.get(11) is not None else None,
                    str(v.get(12)) if v.get(12) is not None else None,
                    str(v.get(13)) if v.get(13) is not None else None,
                    str(v.get(14)) if v.get(14) is not None else None,
                    str(v.get(15)) if v.get(15) is not None else None,
                    numeric(v.get(16)), numeric(v.get(17)), numeric(v.get(18)), numeric(v.get(19)),
                    numeric(v.get(20)), numeric(v.get(21)), numeric(v.get(22)), numeric(v.get(23)),
                    numeric(v.get(24)), numeric(v.get(29)), numeric(v.get(30)), numeric(v.get(31)),
                    numeric(v.get(32)), numeric(v.get(33)), numeric(v.get(34)), numeric(v.get(35)),
                    numeric(v.get(36)), numeric(v.get(37)), numeric(v.get(38)), numeric(v.get(39)),
                    numeric(v.get(40)), numeric(v.get(41)), numeric(v.get(42)), numeric(v.get(43)),
                    numeric(v.get(44)), numeric(v.get(45)), Jsonb(raw)
                ))
                count_meta += 1

                if len(meta_batch) >= 4000:
                    with conn.cursor() as cur, cur.copy(meta_sql) as cp:
                        for item in meta_batch:
                            cp.write_row(item)
                    conn.commit()
                    meta_batch.clear()
                    update_job(
                        job_id,
                        progress=min(82, 58 + count_meta / 2300),
                        rows_processed=count_real + count_meta,
                        message=f'BD_Meta: {count_meta:,} linhas processadas'.replace(',', '.')
                    )

            if meta_batch:
                with conn.cursor() as cur, cur.copy(meta_sql) as cp:
                    for item in meta_batch:
                        cp.write_row(item)
                conn.commit()
                meta_batch.clear()

        aux_counts = {}
        for sheet in ('TD_Meta', 'Apoio', 'Tabela de Preço'):
            if sheet not in xlsx.sheet_names():
                continue
            aux_count = 0
            copy_sql = 'copy ingest.raw_excel (import_id,sheet_name,row_number,row_data,formula_data,cached_values) from stdin'
            with db_conn() as conn:
                with conn.cursor() as cur, cur.copy(copy_sql) as cp:
                    for row_num, v, formulas in xlsx.iter_rows(sheet):
                        if not v:
                            continue
                        cp.write_row((
                            payload.import_id, sheet, row_num,
                            Jsonb(raw_from_row(v, None)),
                            Jsonb({str(k): val for k, val in formulas.items()}) if formulas else None,
                            None,
                        ))
                        aux_count += 1
                conn.commit()
            aux_counts[sheet] = aux_count

        return {'BD': count_real, 'BD_Meta': count_meta, **aux_counts}


def process_production(path: Path, payload: ProcessRequest, job_id: str):
    with XlsxStream(path) as xlsx:
        sheet = 'Gestao da ProdV6'
        if sheet not in xlsx.sheet_names():
            raise RuntimeError(f'Aba obrigatória ausente: {sheet}')
        headers = xlsx.header_map(sheet, 1)
        prod_bounds = xlsx.table_row_bounds('Tabela_Gestao_da_ProdV6')
        prod_last_row = prod_bounds[1] if prod_bounds else None

        with db_conn() as conn:
            conn.execute('delete from core.gestao_producao where import_id=%s', (payload.import_id,))
            populate_dictionary(conn, payload.import_id, sheet, 'Tabela_Gestao_da_ProdV6', headers, True)
            conn.commit()

        cols = (
            'import_id','source_row','company_key','invoice_number_key','product_key','client_key',
            'data_entrada_pedido','data_documento_pedido','pedido','data_aprovacao','canal','vendedor',
            'regiao','unidade','cliente','cidade','uf','tipo_frete','item_pa','familia','descricao_pa',
            'status_linha','qtd_pedido','peso_unit','peso','preco_apos_desconto','total','forma_pagamento',
            'condicao_pagamento','data_entrega_pv','cod_mp','posicao_mp','qtd_alocada_pv','lotes_alocados',
            'qtd_real_produzir','qtd_mp_saldo_deposito','estoque_pa','data_criacao_op','posicao_op','op',
            'qtd_apontada_op','data_inicio_op','status_producao','data_entrega_op','data_fechamento_op',
            'status_op','qtd_picking','peso_total_picking','num_picking','data_picking','hora_picking',
            'num_draft','data_draft','hora_draft','status','data_nota_fiscal','nf','peso_nf','fat_rs',
            'romaneio','data_criacao_romaneio','data_entrega_romaneio','transportadora','valor_frete','raw_data'
        )
        copy_sql = 'copy core.gestao_producao (' + ','.join(cols) + ') from stdin'
        count = 0
        prod_batch = []
        with db_conn() as conn:
            for row_num, v, formulas in xlsx.iter_rows(sheet):
                if row_num < 2:
                    continue
                if prod_last_row is not None and row_num > prod_last_row:
                    break
                if not v:
                    continue
                raw = raw_from_row(v, headers)
                prod_batch.append((
                    payload.import_id, row_num,
                    company_key_from_production(v.get(9)), normalize_invoice(v.get(52)),
                    normalize_key(v.get(14)), normalize_key(v.get(10)),
                    excel_date(v.get(1)), excel_date(v.get(2)), str(v.get(3)) if v.get(3) is not None else None,
                    excel_date(v.get(4)), str(v.get(6)) if v.get(6) is not None else None,
                    str(v.get(7)) if v.get(7) is not None else None,
                    str(v.get(8)) if v.get(8) is not None else None,
                    str(v.get(9)) if v.get(9) is not None else None,
                    str(v.get(10)) if v.get(10) is not None else None,
                    str(v.get(11)) if v.get(11) is not None else None,
                    str(v.get(12)) if v.get(12) is not None else None,
                    str(v.get(13)) if v.get(13) is not None else None,
                    str(v.get(14)) if v.get(14) is not None else None,
                    str(v.get(15)) if v.get(15) is not None else None,
                    str(v.get(16)) if v.get(16) is not None else None,
                    str(v.get(17)) if v.get(17) is not None else None,
                    numeric(v.get(18)), numeric(v.get(19)), numeric(v.get(20)), numeric(v.get(21)), numeric(v.get(22)),
                    str(v.get(23)) if v.get(23) is not None else None,
                    str(v.get(24)) if v.get(24) is not None else None,
                    excel_date(v.get(25)), str(v.get(26)) if v.get(26) is not None else None,
                    str(v.get(27)) if v.get(27) is not None else None,
                    numeric(v.get(28)), str(v.get(29)) if v.get(29) is not None else None,
                    numeric(v.get(30)), numeric(v.get(31)), numeric(v.get(32)), excel_date(v.get(33)),
                    str(v.get(34)) if v.get(34) is not None else None,
                    str(v.get(35)) if v.get(35) is not None else None,
                    numeric(v.get(36)), excel_date(v.get(37)), str(v.get(38)) if v.get(38) is not None else None,
                    excel_date(v.get(39)), excel_date(v.get(40)), str(v.get(41)) if v.get(41) is not None else None,
                    numeric(v.get(42)), numeric(v.get(43)), str(v.get(44)) if v.get(44) is not None else None,
                    excel_date(v.get(45)), str(v.get(46)) if v.get(46) is not None else None,
                    str(v.get(47)) if v.get(47) is not None else None,
                    excel_date(v.get(48)), str(v.get(49)) if v.get(49) is not None else None,
                    str(v.get(50)) if v.get(50) is not None else None,
                    excel_date(v.get(51)), str(v.get(52)) if v.get(52) is not None else None,
                    numeric(v.get(53)), numeric(v.get(54)), str(v.get(55)) if v.get(55) is not None else None,
                    excel_date(v.get(56)), excel_date(v.get(57)), str(v.get(58)) if v.get(58) is not None else None,
                    numeric(v.get(59)), Jsonb(raw)
                ))
                count += 1

                if len(prod_batch) >= 4000:
                    with conn.cursor() as cur, cur.copy(copy_sql) as cp:
                        for item in prod_batch:
                            cp.write_row(item)
                    conn.commit()
                    prod_batch.clear()
                    update_job(
                        job_id,
                        progress=min(88, 22 + count / 700),
                        rows_processed=count,
                        message=f'Gestão da Produção: {count:,} linhas processadas'.replace(',', '.')
                    )

            if prod_batch:
                with conn.cursor() as cur, cur.copy(copy_sql) as cp:
                    for item in prod_batch:
                        cp.write_row(item)
                conn.commit()
                prod_batch.clear()

        return {sheet: count}


def activate_import(payload: ProcessRequest, sha256: str, row_counts: dict):
    sheet_names = list(row_counts.keys())
    with db_conn() as conn:
        with conn.transaction():
            conn.execute(
                'update ingest.importacoes set is_active=false where source_type=%s and is_active=true',
                (payload.source_type,)
            )
            conn.execute(
                """
                update ingest.importacoes set
                  sha256=%s, status='READY', is_active=true,
                  sheets=%s, row_counts=%s, processor_message='Processamento concluído',
                  completed_at=now(), activated_at=now()
                where import_id=%s
                """,
                (sha256, Jsonb({'processed': sheet_names}), Jsonb(row_counts), payload.import_id)
            )



def mirror_configured():
    return bool(MIRROR_URL and MIRROR_TOKEN)


def _mirror_record(import_id: str, source_type: str, dataset: str, batch_number: int,
                   batch_id: str, row_count: int, status: str, *,
                   attempts: int = 0, http_status: int | None = None,
                   response_text: str | None = None, error_message: str | None = None,
                   sent: bool = False):
    with db_conn() as conn:
        conn.execute(
            """
            insert into ingest.outbound_sync (
              import_id, source_type, dataset, batch_number, batch_id, row_count,
              status, attempts, http_status, response_text, error_message, sent_at
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                      case when %s then now() else null end)
            on conflict (batch_id) do update set
              status=excluded.status,
              attempts=excluded.attempts,
              http_status=excluded.http_status,
              response_text=excluded.response_text,
              error_message=excluded.error_message,
              sent_at=case when excluded.status='SENT' then now() else ingest.outbound_sync.sent_at end,
              updated_at=now()
            """,
            (
                import_id, source_type, dataset, batch_number, batch_id, row_count,
                status, attempts, http_status, response_text, error_message, sent
            )
        )
        conn.commit()


def _mirror_batch_sent(batch_id: str) -> bool:
    with db_conn() as conn:
        row = conn.execute(
            "select status from ingest.outbound_sync where batch_id=%s",
            (batch_id,)
        ).fetchone()
    return bool(row and row[0] == 'SENT')


def _mirror_send_batch(payload: ProcessRequest | MirrorContext, dataset: str, headers: list[str],
                       rows: list[list[object]], batch_number: int,
                       start_row: int, is_last_batch: bool, force: bool = False):
    if not mirror_configured():
        return {'status': 'SKIPPED', 'reason': 'MIRROR_URL/MIRROR_TOKEN não configurados'}

    batch_id = f'{payload.import_id}:{dataset}:{batch_number}'
    if not force and _mirror_batch_sent(batch_id):
        return {'status': 'SKIPPED', 'reason': 'batch already sent', 'batch_id': batch_id}

    _mirror_record(
        payload.import_id, payload.source_type, dataset, batch_number,
        batch_id, len(rows), 'SENDING', attempts=1
    )

    body = {
        'sheetName': dataset,
        'headers': headers,
        'rows': rows,
        'importId': payload.import_id,
        'sourceType': payload.source_type,
        'batchNumber': batch_number,
        'startRow': start_row,
        'isLastBatch': is_last_batch,
        'mode': 'append'
    }

    request_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    if MIRROR_TOKEN_LOCATION == 'header':
        request_headers[MIRROR_TOKEN_FIELD] = MIRROR_TOKEN
    else:
        body[MIRROR_TOKEN_FIELD] = MIRROR_TOKEN

    try:
        with httpx.Client(timeout=90.0, follow_redirects=True) as client:
            response = client.post(MIRROR_URL, json=body, headers=request_headers)

        response_text = response.text[:4000]

        if response.status_code < 200 or response.status_code >= 300:
            _mirror_record(
                payload.import_id, payload.source_type, dataset, batch_number,
                batch_id, len(rows), 'ERROR', attempts=1,
                http_status=response.status_code,
                response_text=response_text,
                error_message=f'HTTP {response.status_code}'
            )
            raise RuntimeError(
                f'Espelhamento {dataset} lote {batch_number}: HTTP {response.status_code}'
            )

        try:
            result = response.json()
        except Exception:
            result = {}

        if isinstance(result, dict) and (
            result.get('error') or str(result.get('status', '')).lower() == 'error'
        ):
            message = str(result.get('error') or result.get('message') or 'Destino recusou o lote')
            _mirror_record(
                payload.import_id, payload.source_type, dataset, batch_number,
                batch_id, len(rows), 'ERROR', attempts=1,
                http_status=response.status_code,
                response_text=response_text,
                error_message=message
            )
            raise RuntimeError(
                f'Espelhamento {dataset} lote {batch_number}: {message}'
            )

        _mirror_record(
            payload.import_id, payload.source_type, dataset, batch_number,
            batch_id, len(rows), 'SENT', attempts=1,
            http_status=response.status_code,
            response_text=response_text,
            sent=True
        )
        return {'status': 'SENT', 'http_status': response.status_code}

    except Exception as exc:
        # Se já houve registro de erro acima, este upsert apenas garante a mensagem final.
        _mirror_record(
            payload.import_id, payload.source_type, dataset, batch_number,
            batch_id, len(rows), 'ERROR', attempts=1,
            error_message=str(exc)[:2000]
        )
        raise


def _sheet_mirror_spec(source_type: str):
    if source_type == 'MARGEM':
        return [
            ('BD', 2, 3, 'Realizado'),
            ('BD_Meta', 1, 2, 'Meta'),
            ('TD_Meta', 1, 2, None),
            ('Apoio', 1, 2, None),
            ('Tabela de Preço', 2, 3, None),
        ]
    return [
        ('Gestao da ProdV6', 1, 2, 'Tabela_Gestao_da_ProdV6'),
    ]


def mirror_xlsx_to_secondary(path: Path, payload: ProcessRequest, job_id: str):
    if not mirror_configured():
        return {
            'configured': False,
            'status': 'SKIPPED',
            'message': 'MIRROR_URL/MIRROR_TOKEN não configurados'
        }

    sent_batches = 0
    sent_rows = 0

    with XlsxStream(path) as xlsx:
        available = set(xlsx.sheet_names())

        for dataset, header_row, first_data_row, table_name in _sheet_mirror_spec(payload.source_type):
            if dataset not in available:
                continue

            headers_map = xlsx.header_map(dataset, header_row)
            if not headers_map:
                continue

            max_col = max(headers_map)
            headers = [
                str(headers_map.get(i) or f'COL_{i}')
                for i in range(1, max_col + 1)
            ]

            last_row = None
            if table_name:
                bounds = xlsx.table_row_bounds(table_name)
                if bounds:
                    last_row = bounds[1]

            batch: list[list[object]] = []
            batch_number = 1
            batch_start_row = first_data_row

            for row_num, values, formulas in xlsx.iter_rows(dataset):
                if row_num < first_data_row:
                    continue
                if last_row is not None and row_num > last_row:
                    break
                if not values:
                    continue

                row = [values.get(i) for i in range(1, max_col + 1)]
                batch.append(row)

                if len(batch) >= MIRROR_BATCH_SIZE:
                    _mirror_send_batch(
                        payload, dataset, headers, batch,
                        batch_number, batch_start_row, False
                    )
                    sent_batches += 1
                    sent_rows += len(batch)
                    batch_number += 1
                    batch_start_row = row_num + 1
                    batch = []

                    update_job(
                        job_id,
                        progress=99,
                        message=f'Espelhando {dataset}: {sent_rows:,} linhas enviadas'.replace(',', '.')
                    )

            if batch:
                _mirror_send_batch(
                    payload, dataset, headers, batch,
                    batch_number, batch_start_row, True
                )
                sent_batches += 1
                sent_rows += len(batch)

    return {
        'configured': True,
        'status': 'SENT',
        'batches': sent_batches,
        'rows': sent_rows
    }


def _update_mirror_job(job_id: str, *, status=None, progress=None, message=None,
                       total_batches_delta=0, sent_batches_delta=0,
                       skipped_batches_delta=0, error_batches_delta=0,
                       total_rows_delta=0, sent_rows_delta=0, finished=False):
    fields = ['updated_at=now()']
    params = []

    if status is not None:
        fields.append('status=%s')
        params.append(status)
    if progress is not None:
        fields.append('progress=%s')
        params.append(progress)
    if message is not None:
        fields.append('message=%s')
        params.append(message)

    for field, delta in [
        ('total_batches', total_batches_delta),
        ('sent_batches', sent_batches_delta),
        ('skipped_batches', skipped_batches_delta),
        ('error_batches', error_batches_delta),
        ('total_rows', total_rows_delta),
        ('sent_rows', sent_rows_delta),
    ]:
        if delta:
            fields.append(f'{field}={field}+%s')
            params.append(delta)

    if finished:
        fields.append('finished_at=now()')

    params.append(job_id)
    with db_conn() as conn:
        conn.execute(
            f"update ingest.mirror_jobs set {', '.join(fields)} where job_id=%s",
            params
        )
        conn.commit()


def _dictionary_layout(import_id: str, dataset: str):
    with db_conn() as conn:
        items = conn.execute(
            """
            select source_column, original_header
            from ingest.field_dictionary
            where import_id=%s and source_sheet=%s
            order by source_column
            """,
            (import_id, dataset)
        ).fetchall()

    if not items:
        return [], []

    max_col = max(int(item[0]) for item in items)
    header_by_col = {int(col): header for col, header in items}
    headers = []
    raw_keys = []
    used = set()

    for idx in range(1, max_col + 1):
        header = header_by_col.get(idx)
        headers.append(str(header) if header not in (None, '') else f'COL_{idx}')

        if header not in (None, ''):
            key = str(header)
            if key in used:
                key = f'{key} [{col_letter(idx)}]'
        else:
            key = col_letter(idx)

        used.add(key)
        raw_keys.append(key)

    return headers, raw_keys


def _replay_core_dataset(context: MirrorContext, dataset: str, table_name: str,
                         replay_job_id: str, force: bool):
    headers, raw_keys = _dictionary_layout(context.import_id, dataset)
    if not headers:
        return

    batch_number = 1
    last_source_row = 0

    while True:
        with db_conn() as conn:
            records = conn.execute(
                f"""
                select source_row, raw_data
                from {table_name}
                where import_id=%s and source_row > %s
                order by source_row
                limit %s
                """,
                (context.import_id, last_source_row, MIRROR_BATCH_SIZE)
            ).fetchall()

        if not records:
            break

        rows = []
        for source_row, raw_data in records:
            raw = raw_data or {}
            rows.append([raw.get(key) for key in raw_keys])

        start_row = int(records[0][0])
        last_source_row = int(records[-1][0])
        is_last = len(records) < MIRROR_BATCH_SIZE

        result = _mirror_send_batch(
            context, dataset, headers, rows,
            batch_number, start_row, is_last, force=force
        )

        skipped = result.get('status') == 'SKIPPED'
        _update_mirror_job(
            replay_job_id,
            status='PROCESSING',
            message=f'{context.source_type} / {dataset}: lote {batch_number}',
            total_batches_delta=1,
            sent_batches_delta=0 if skipped else 1,
            skipped_batches_delta=1 if skipped else 0,
            total_rows_delta=len(rows),
            sent_rows_delta=0 if skipped else len(rows)
        )

        batch_number += 1
        if is_last:
            break


def _raw_layout(import_id: str, dataset: str, header_row: int):
    with db_conn() as conn:
        row = conn.execute(
            """
            select row_data
            from ingest.raw_excel
            where import_id=%s and sheet_name=%s and row_number=%s
            """,
            (import_id, dataset, header_row)
        ).fetchone()

    if not row or not row[0]:
        return [], []

    raw = row[0]
    pairs = []
    for key, value in raw.items():
        if not isinstance(key, str) or not key.isalpha():
            continue
        idx = 0
        for ch in key.upper():
            idx = idx * 26 + ord(ch) - 64
        pairs.append((idx, key, value))

    if not pairs:
        return [], []

    pairs.sort(key=lambda x: x[0])
    max_col = pairs[-1][0]
    value_by_col = {idx: value for idx, _, value in pairs}
    key_by_col = {idx: key for idx, key, _ in pairs}

    headers = [
        str(value_by_col.get(idx)) if value_by_col.get(idx) not in (None, '')
        else f'COL_{idx}'
        for idx in range(1, max_col + 1)
    ]
    raw_keys = [key_by_col.get(idx, col_letter(idx)) for idx in range(1, max_col + 1)]
    return headers, raw_keys


def _replay_raw_dataset(context: MirrorContext, dataset: str, header_row: int,
                        first_data_row: int, replay_job_id: str, force: bool):
    headers, raw_keys = _raw_layout(context.import_id, dataset, header_row)
    if not headers:
        return

    batch_number = 1
    last_row = first_data_row - 1

    while True:
        with db_conn() as conn:
            records = conn.execute(
                """
                select row_number, row_data
                from ingest.raw_excel
                where import_id=%s and sheet_name=%s and row_number > %s
                order by row_number
                limit %s
                """,
                (context.import_id, dataset, last_row, MIRROR_BATCH_SIZE)
            ).fetchall()

        if not records:
            break

        rows = []
        for row_number, row_data in records:
            raw = row_data or {}
            rows.append([raw.get(key) for key in raw_keys])

        start_row = int(records[0][0])
        last_row = int(records[-1][0])
        is_last = len(records) < MIRROR_BATCH_SIZE

        result = _mirror_send_batch(
            context, dataset, headers, rows,
            batch_number, start_row, is_last, force=force
        )

        skipped = result.get('status') == 'SKIPPED'
        _update_mirror_job(
            replay_job_id,
            status='PROCESSING',
            message=f'{context.source_type} / {dataset}: lote {batch_number}',
            total_batches_delta=1,
            sent_batches_delta=0 if skipped else 1,
            skipped_batches_delta=1 if skipped else 0,
            total_rows_delta=len(rows),
            sent_rows_delta=0 if skipped else len(rows)
        )

        batch_number += 1
        if is_last:
            break


def replay_active_mirror_job(job_id: str, requested_source: str, force: bool):
    try:
        if not mirror_configured():
            raise RuntimeError('MIRROR_URL ou MIRROR_TOKEN nao configurado no Render.')

        _update_mirror_job(
            job_id,
            status='PROCESSING',
            progress=2,
            message='Localizando bases ativas...'
        )

        query = """
            select import_id, source_type
            from ingest.importacoes
            where is_active=true and status='READY'
        """
        params = ()
        if requested_source != 'ALL':
            query += " and source_type=%s"
            params = (requested_source,)
        query += " order by source_type"

        with db_conn() as conn:
            active = conn.execute(query, params).fetchall()

        if not active:
            raise RuntimeError('Nenhuma importacao READY/ativa encontrada.')

        total_sources = len(active)

        for idx, (import_id, source_type) in enumerate(active, start=1):
            context = MirrorContext(import_id=import_id, source_type=source_type)

            if source_type == 'MARGEM':
                _replay_core_dataset(context, 'BD', 'core.realizado', job_id, force)
                _replay_core_dataset(context, 'BD_Meta', 'core.meta', job_id, force)
                _replay_raw_dataset(context, 'TD_Meta', 1, 2, job_id, force)
                _replay_raw_dataset(context, 'Apoio', 1, 2, job_id, force)
                _replay_raw_dataset(context, 'Tabela de Preço', 2, 3, job_id, force)
            else:
                _replay_core_dataset(
                    context, 'Gestao da ProdV6', 'core.gestao_producao',
                    job_id, force
                )

            _update_mirror_job(
                job_id,
                status='PROCESSING',
                progress=min(98, 5 + (idx / total_sources) * 90),
                message=f'{source_type} concluido no espelhamento.'
            )

        _update_mirror_job(
            job_id,
            status='READY',
            progress=100,
            message='Bases ativas reenviadas ao espelho.',
            finished=True
        )

    except Exception as exc:
        _update_mirror_job(
            job_id,
            status='ERROR',
            progress=100,
            message=f'{type(exc).__name__}: {exc}',
            error_batches_delta=1,
            finished=True
        )
        traceback.print_exc()


def process_job(job_id: str, payload_dict: dict):
    payload = ProcessRequest(**payload_dict)
    tmp_path = Path(tempfile.gettempdir()) / f'abr_{job_id}.xlsx'
    try:
        update_job(job_id, status='DOWNLOADING', progress=1, message='Iniciando processamento...', rows_processed=0)
        sha256, actual_size = download_drive_file(payload, job_id, tmp_path)
        upsert_import(payload, job_id, sha256=sha256, status='PROCESSING', message='Arquivo baixado; processando.')

        duplicate_of = detect_duplicate(payload.source_type, sha256, payload.import_id)
        if duplicate_of:
            with db_conn() as conn:
                conn.execute(
                    """
                    update ingest.importacoes set status='DUPLICATE', sha256=%s,
                      processor_message=%s, completed_at=now(), metadata=metadata || %s
                    where import_id=%s
                    """,
                    (sha256, f'Arquivo idêntico à importação {duplicate_of}.', Jsonb({'duplicate_of': duplicate_of}), payload.import_id)
                )
                conn.commit()
            update_job(job_id, status='DUPLICATE', progress=100,
                       message=f'Arquivo já processado em {duplicate_of}.', finished=True)
            return

        update_job(job_id, status='PROCESSING', progress=18, message='Lendo estrutura do XLSX...')
        if payload.source_type == 'MARGEM':
            row_counts = process_margin(tmp_path, payload, job_id)
        else:
            row_counts = process_production(tmp_path, payload, job_id)

        update_job(job_id, status='VALIDATING', progress=92, message='Validando e ativando nova versão...',
                   rows_processed=sum(row_counts.values()))
        if not row_counts or sum(row_counts.values()) <= 0:
            raise RuntimeError('Nenhuma linha válida foi processada.')

        activate_import(payload, sha256, row_counts)

        mirror_message = 'Espelhamento secundário não configurado.'
        try:
            mirror_result = mirror_xlsx_to_secondary(tmp_path, payload, job_id)
            if mirror_result.get('status') == 'SENT':
                mirror_message = (
                    f" Espelhamento concluído: {mirror_result.get('rows', 0):,} linhas "
                    f"em {mirror_result.get('batches', 0)} lotes."
                ).replace(',', '.')
            else:
                mirror_message = ' Espelhamento secundário ignorado.'
        except Exception as mirror_exc:
            # A carga principal já está validada e ativa. Falha no destino secundário
            # não desfaz o Supabase principal; fica registrada em ingest.outbound_sync.
            mirror_message = f' Espelhamento secundário com erro: {mirror_exc}'

        update_job(
            job_id,
            status='READY',
            progress=100,
            message='Base atualizada e ativa.' + mirror_message,
            rows_processed=sum(row_counts.values()),
            finished=True
        )
    except Exception as exc:
        err = f'{type(exc).__name__}: {exc}'
        try:
            # Como a carga é confirmada em lotes, qualquer job que termine em
            # ERROR tem seus registros parciais removidos antes de ser marcado.
            with db_conn() as conn:
                conn.execute('delete from core.realizado where import_id=%s', (payload.import_id,))
                conn.execute('delete from core.meta where import_id=%s', (payload.import_id,))
                conn.execute('delete from core.gestao_producao where import_id=%s', (payload.import_id,))
                conn.execute('delete from ingest.raw_excel where import_id=%s', (payload.import_id,))
                conn.execute('delete from ingest.field_dictionary where import_id=%s', (payload.import_id,))
                conn.commit()

            upsert_import(payload, job_id, status='ERROR', message=err)
            with db_conn() as conn:
                conn.execute(
                    "update ingest.importacoes set status='ERROR', is_active=false, processor_message=%s, completed_at=now() where import_id=%s",
                    (err, payload.import_id)
                )
                conn.commit()
        except Exception:
            pass
        try:
            update_job(job_id, status='ERROR', progress=100, message=err, finished=True)
        except Exception:
            pass
        traceback.print_exc()
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


@app.post('/v1/mirror/replay-active', status_code=202)
def replay_active_mirror(payload: MirrorReplayRequest, background_tasks: BackgroundTasks):
    if not mirror_configured():
        raise HTTPException(
            503,
            {
                'message': 'Espelhamento nao configurado.',
                'mirror_url_configured': bool(MIRROR_URL),
                'mirror_token_configured': bool(MIRROR_TOKEN),
                'required_env': ['MIRROR_TOKEN'],
            }
        )

    job_id = str(uuid.uuid4())

    with db_conn() as conn:
        conn.execute(
            """
            insert into ingest.mirror_jobs (
              job_id, requested_source, status, progress, message, started_at
            ) values (%s,%s,'QUEUED',0,'Na fila para reenvio ao espelho',now())
            """,
            (job_id, payload.source_type)
        )
        conn.commit()

    background_tasks.add_task(
        replay_active_mirror_job,
        job_id,
        payload.source_type,
        payload.force
    )

    return {
        'job_id': job_id,
        'status': 'QUEUED',
        'source_type': payload.source_type,
        'force': payload.force
    }


@app.get('/v1/mirror/jobs/{job_id}')
def mirror_job_status(job_id: str):
    with db_conn() as conn:
        row = conn.execute(
            """
            select job_id, requested_source, status, progress, message,
                   total_batches, sent_batches, skipped_batches, error_batches,
                   total_rows, sent_rows, created_at, started_at, finished_at, updated_at
            from ingest.mirror_jobs
            where job_id=%s
            """,
            (job_id,)
        ).fetchone()

    if not row:
        raise HTTPException(404, 'Job de espelhamento nao encontrado.')

    keys = [
        'job_id','requested_source','status','progress','message',
        'total_batches','sent_batches','skipped_batches','error_batches',
        'total_rows','sent_rows','created_at','started_at','finished_at','updated_at'
    ]
    return dict(zip(keys, row))


@app.get('/v1/mirror/summary')
def mirror_summary():
    with db_conn() as conn:
        rows = conn.execute(
            """
            select status, dataset, count(*)::bigint as batches,
                   coalesce(sum(row_count),0)::bigint as rows
            from ingest.outbound_sync
            group by status, dataset
            order by dataset, status
            """
        ).fetchall()

    return {
        'configured': mirror_configured(),
        'batches': [
            {
                'status': row[0],
                'dataset': row[1],
                'batches': row[2],
                'rows': row[3]
            }
            for row in rows
        ]
    }


@app.get('/health')
def health():
    status = 'ok'
    db = False
    try:
        with db_conn() as conn:
            conn.execute('select 1').fetchone()
        db = True
    except Exception:
        status = 'degraded'
    return {
        'service': APP_NAME,
        'status': status,
        'database': db,
        'mirror_url_configured': bool(MIRROR_URL),
        'mirror_token_configured': bool(MIRROR_TOKEN),
        'mirror_token_location': MIRROR_TOKEN_LOCATION,
        'mirror_token_field': MIRROR_TOKEN_FIELD,
        'mirror_batch_size': MIRROR_BATCH_SIZE,
    }


@app.post('/v1/process', status_code=202)
def process(payload: ProcessRequest, background_tasks: BackgroundTasks):
    if not payload.drive_file_name.lower().endswith('.xlsx'):
        raise HTTPException(400, 'Apenas arquivos .xlsx são aceitos.')
    job_id = str(uuid.uuid4())
    try:
        with db_conn() as conn:
            conn.execute(
                """
                insert into ingest.jobs (job_id, import_id, source_type, status, progress, message, started_at)
                values (%s,%s,%s,'QUEUED',0,'Na fila para processamento',now())
                """,
                (job_id, payload.import_id, payload.source_type)
            )
            conn.commit()
    except Exception as exc:
        raise HTTPException(500, f'Falha ao registrar job: {exc}')
    background_tasks.add_task(process_job, job_id, payload.model_dump())
    return {'job_id': job_id, 'status': 'QUEUED', 'import_id': payload.import_id}


@app.get('/v1/jobs/{job_id}')
def job_status(job_id: str):
    with db_conn() as conn:
        row = conn.execute(
            """
            select job_id, import_id, source_type, status, progress, message, rows_processed,
                   created_at, started_at, finished_at, updated_at
            from ingest.jobs where job_id=%s
            """, (job_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, 'Job não encontrado.')
    keys = ['job_id','import_id','source_type','status','progress','message','rows_processed',
            'created_at','started_at','finished_at','updated_at']
    return dict(zip(keys, row))


@app.get('/v1/imports/active')
def active_imports():
    with db_conn() as conn:
        rows = conn.execute(
            """
            select source_type, import_id, file_name, status, row_counts, activated_at
            from ingest.importacoes where is_active=true order by source_type
            """
        ).fetchall()
    return [
        {'source_type': r[0], 'import_id': r[1], 'file_name': r[2], 'status': r[3],
         'row_counts': r[4], 'activated_at': r[5]}
        for r in rows
    ]


@app.get('/v1/reconciliation/summary')
def reconciliation_summary():
    with db_conn() as conn:
        s = conn.execute(
            'select anulados, divergentes, so_gestao, so_margem, total_pendencias, total_cruzamento from public.reconciliation_summary'
        ).fetchone()
        active = conn.execute(
            "select source_type, import_id from ingest.importacoes where is_active=true and status='READY'"
        ).fetchall()
    return {
        'anulados': s[0] if s else 0,
        'divergentes': s[1] if s else 0,
        'so_gestao': s[2] if s else 0,
        'so_margem': s[3] if s else 0,
        'total_pendencias': s[4] if s else 0,
        'total_cruzamento': s[5] if s else 0,
        'active_imports': {r[0]: r[1] for r in active},
        'updated_at': utcnow().isoformat(),
    }


@app.get('/v1/reconciliation/pending')
def reconciliation_pending(limit: int = Query(10000, ge=1, le=20000), offset: int = Query(0, ge=0)):
    with db_conn() as conn:
        rows = conn.execute(
            """
            select status_cruzamento, unidade_empresa, nf, codigo_produto, produto, cliente,
                   peso_producao, peso_margem, saldo_peso,
                   faturamento_producao, faturamento_margem, saldo_faturamento,
                   linhas_producao, linhas_margem
            from public.pendencias_cruzamento
            order by status_cruzamento, unidade_empresa, nf, codigo_produto
            limit %s offset %s
            """, (limit, offset)
        ).fetchall()
    keys = [
        'status_cruzamento','empresa_unidade','nf','codigo_produto','produto','cliente',
        'peso_gestao','peso_margem','saldo_peso','faturamento_gestao','faturamento_margem',
        'saldo_faturamento','linhas_gestao','linhas_margem'
    ]
    return {'rows': [dict(zip(keys, r)) for r in rows], 'limit': limit, 'offset': offset}
