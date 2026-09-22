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
    XlsxStream, api_field_name, excel_date, normalize_invoice,
    normalize_key, numeric, raw_from_row,
)

DATABASE_URL = os.environ.get('DATABASE_URL', '').strip()
APP_NAME = 'Grupo ABR Margin Processor'

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


def db_conn():
    if not DATABASE_URL:
        raise RuntimeError('DATABASE_URL não configurada.')
    return psycopg.connect(DATABASE_URL, connect_timeout=20)


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
        missing = required - set(xlsx.sheet_names())
        if missing:
            raise RuntimeError('Abas obrigatórias ausentes: ' + ', '.join(sorted(missing)))

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
        with db_conn() as conn:
            with conn.cursor() as cur, cur.copy(realized_sql) as cp:
                for row_num, v, formulas in xlsx.iter_rows('BD'):
                    if row_num < 3:
                        continue
                    if bd_last_row is not None and row_num > bd_last_row:
                        break
                    if not v:
                        continue
                    raw = raw_from_row(v, bd_headers)
                    cp.write_row((
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
                    if count_real % 5000 == 0:
                        update_job(job_id, progress=min(55, 20 + count_real / 1800), rows_processed=count_real,
                                   message=f'BD: {count_real:,} linhas processadas'.replace(',', '.'))
            conn.commit()

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
        with db_conn() as conn:
            with conn.cursor() as cur, cur.copy(meta_sql) as cp:
                for row_num, v, formulas in xlsx.iter_rows('BD_Meta'):
                    if row_num < 2:
                        continue
                    if meta_last_row is not None and row_num > meta_last_row:
                        break
                    if not v:
                        continue
                    raw = raw_from_row(v, meta_headers)
                    cp.write_row((
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
                    if count_meta % 7000 == 0:
                        update_job(job_id, progress=min(82, 58 + count_meta / 2300),
                                   rows_processed=count_real + count_meta,
                                   message=f'BD_Meta: {count_meta:,} linhas processadas'.replace(',', '.'))
            conn.commit()

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
        with db_conn() as conn:
            with conn.cursor() as cur, cur.copy(copy_sql) as cp:
                for row_num, v, formulas in xlsx.iter_rows(sheet):
                    if row_num < 2:
                        continue
                    if prod_last_row is not None and row_num > prod_last_row:
                        break
                    if not v:
                        continue
                    raw = raw_from_row(v, headers)
                    cp.write_row((
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
                    if count % 5000 == 0:
                        update_job(job_id, progress=min(88, 22 + count / 700), rows_processed=count,
                                   message=f'Gestão da Produção: {count:,} linhas processadas'.replace(',', '.'))
            conn.commit()
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
        update_job(job_id, status='READY', progress=100, message='Base atualizada e ativa.',
                   rows_processed=sum(row_counts.values()), finished=True)
    except Exception as exc:
        err = f'{type(exc).__name__}: {exc}'
        try:
            upsert_import(payload, job_id, status='ERROR', message=err)
            with db_conn() as conn:
                conn.execute(
                    "update ingest.importacoes set status='ERROR', processor_message=%s, completed_at=now() where import_id=%s",
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
    return {'service': APP_NAME, 'status': status, 'database': db}


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
