from __future__ import annotations

import re
import unicodedata
import zipfile
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
import xml.etree.ElementTree as ET

MAIN_NS = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
REL_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
PKG_REL_NS = 'http://schemas.openxmlformats.org/package/2006/relationships'
NS = {'a': MAIN_NS, 'r': REL_NS, 'p': PKG_REL_NS}


def col_num(letters: str) -> int:
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n


def col_letter(n: int) -> str:
    out = []
    while n:
        n, rem = divmod(n - 1, 26)
        out.append(chr(65 + rem))
    return ''.join(reversed(out))


def normalize_text(value) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r'\s+', ' ', s).upper().strip()
    return s or None


def normalize_key(value) -> str | None:
    s = normalize_text(value)
    if not s:
        return None
    s = re.sub(r'[^A-Z0-9]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s or None


def normalize_invoice(value) -> str | None:
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return ('%.15g' % value).strip()
    s = str(value).strip()
    if not s:
        return None
    if re.fullmatch(r'\d+\.0+', s):
        return s.split('.')[0]
    return s.upper()


def numeric(value):
    if value is None or value == '':
        return None
    if isinstance(value, bool):
        return Decimal(int(value))
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    s = str(value).strip().replace(' ', '')
    if not s:
        return None
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    elif ',' in s:
        s = s.replace('.', '').replace(',', '.')
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def excel_date(value):
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        serial = float(value)
        return date(1899, 12, 30) + timedelta(days=int(serial))
    except Exception:
        s = str(value).strip()
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                pass
        return None


def api_field_name(header: str | None, fallback: str) -> str:
    if not header:
        return fallback.lower()
    s = unicodedata.normalize('NFKD', str(header))
    s = ''.join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s or fallback.lower()


class XlsxStream:
    def __init__(self, path: str | Path):
        self.path = str(path)
        self.zf = zipfile.ZipFile(self.path)
        self.shared_strings = self._load_shared_strings()
        self.sheet_paths = self._load_sheet_paths()
        self.table_refs = self._load_table_refs()

    def close(self):
        self.zf.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _load_shared_strings(self):
        if 'xl/sharedStrings.xml' not in self.zf.namelist():
            return []
        root = ET.fromstring(self.zf.read('xl/sharedStrings.xml'))
        out = []
        for si in root.findall(f'{{{MAIN_NS}}}si'):
            texts = []
            for t in si.iter(f'{{{MAIN_NS}}}t'):
                texts.append(t.text or '')
            out.append(''.join(texts))
        return out

    def _load_sheet_paths(self):
        wb = ET.fromstring(self.zf.read('xl/workbook.xml'))
        rels = ET.fromstring(self.zf.read('xl/_rels/workbook.xml.rels'))
        relmap = {r.attrib['Id']: r.attrib['Target'] for r in rels}
        out = {}
        sheets = wb.find(f'{{{MAIN_NS}}}sheets')
        for s in sheets:
            name = s.attrib['name']
            rid = s.attrib[f'{{{REL_NS}}}id']
            target = relmap[rid]
            if target.startswith('/'):
                target = target.lstrip('/')
            elif not target.startswith('xl/'):
                target = 'xl/' + target
            target = target.replace('xl/../', '')
            out[name] = target
        return out

    def _load_table_refs(self):
        refs = {}
        for name in self.zf.namelist():
            if not (name.startswith('xl/tables/table') and name.endswith('.xml')):
                continue
            try:
                root = ET.fromstring(self.zf.read(name))
                table_name = root.attrib.get('name') or root.attrib.get('displayName')
                ref = root.attrib.get('ref')
                if table_name and ref:
                    refs[table_name] = ref
            except Exception:
                continue
        return refs

    def table_ref(self, table_name: str):
        return self.table_refs.get(table_name)

    def table_row_bounds(self, table_name: str):
        ref = self.table_ref(table_name)
        if not ref:
            return None
        m = re.match(r'[A-Z]+(\d+):[A-Z]+(\d+)$', ref)
        if not m:
            return None
        return int(m.group(1)), int(m.group(2))

    def sheet_names(self):
        return list(self.sheet_paths)

    def _cell_value(self, cell):
        t = cell.attrib.get('t')
        if t == 'inlineStr':
            inline = cell.find(f'{{{MAIN_NS}}}is')
            if inline is None:
                return None
            return ''.join(x.text or '' for x in inline.iter(f'{{{MAIN_NS}}}t'))

        v = cell.find(f'{{{MAIN_NS}}}v')
        if v is None or v.text is None:
            return None
        raw = v.text
        if t == 's':
            idx = int(raw)
            return self.shared_strings[idx] if 0 <= idx < len(self.shared_strings) else raw
        if t == 'b':
            return raw == '1'
        if t in ('str', 'e'):
            return raw
        try:
            if any(ch in raw for ch in '.Ee'):
                return float(raw)
            return int(raw)
        except ValueError:
            return raw

    def iter_rows(self, sheet_name: str):
        path = self.sheet_paths[sheet_name]
        with self.zf.open(path) as fh:
            for event, elem in ET.iterparse(fh, events=('end',)):
                if elem.tag != f'{{{MAIN_NS}}}row':
                    continue
                row_num = int(elem.attrib.get('r', '0'))
                values = {}
                formulas = {}
                for c in elem.findall(f'{{{MAIN_NS}}}c'):
                    ref = c.attrib.get('r', '')
                    m = re.match(r'([A-Z]+)(\d+)', ref)
                    if not m:
                        continue
                    idx = col_num(m.group(1))
                    values[idx] = self._cell_value(c)
                    f = c.find(f'{{{MAIN_NS}}}f')
                    if f is not None:
                        formulas[idx] = f.text or ''
                yield row_num, values, formulas
                elem.clear()

    def get_row(self, sheet_name: str, target_row: int):
        for row_num, values, formulas in self.iter_rows(sheet_name):
            if row_num == target_row:
                return values, formulas
            if row_num > target_row:
                break
        return {}, {}

    def header_map(self, sheet_name: str, header_row: int):
        values, _ = self.get_row(sheet_name, header_row)
        return {idx: (None if val is None else str(val)) for idx, val in values.items()}


def raw_from_row(values: dict[int, object], headers: dict[int, str | None] | None = None):
    out = {}
    used = set()
    for idx in sorted(values):
        value = values[idx]
        if value is None:
            continue
        if headers is None:
            key = col_letter(idx)
        else:
            base = headers.get(idx) or col_letter(idx)
            key = str(base)
            if key in used:
                key = f'{key} [{col_letter(idx)}]'
        used.add(key)
        out[key] = value
    return out
