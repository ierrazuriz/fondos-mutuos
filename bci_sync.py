"""
BCI Gmail Sync — corre desde Node.js via child_process.
Lee email de BCI, descarga ZIP, parsea PDFs, guarda en SQLite.
Token OAuth leído desde env var GOOGLE_TOKEN_JSON.
"""
import io
import os
import sys
import json
import zipfile
import base64
import datetime
import sqlite3
import re
import tempfile

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# ─── Config ─────────────────────────────────────────────────────────────────
DB_PATH       = os.environ.get('DB_PATH', os.path.join(os.path.dirname(__file__), 'data.db'))
REMITENTE     = 'soportecomercialcb@bci.cl'
TOKEN_ENV_VAR = 'GOOGLE_TOKEN_JSON'

# ─── Auth ────────────────────────────────────────────────────────────────────
def get_creds():
    token_json = os.environ.get(TOKEN_ENV_VAR)
    if not token_json:
        # fallback: archivo local (desarrollo)
        local = os.path.join(os.path.dirname(__file__), '..', 'bci-cartolas', 'token.json')
        if os.path.exists(local):
            with open(local) as f:
                token_json = f.read()
        else:
            raise RuntimeError(f'Variable de entorno {TOKEN_ENV_VAR} no definida y no existe token local.')

    tmp = tempfile.NamedTemporaryFile(suffix='.json', delete=False, mode='w')
    tmp.write(token_json)
    tmp.close()

    creds = Credentials.from_authorized_user_file(tmp.name)
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        # Imprimir token renovado para que Node.js pueda actualizarlo si quiere
        print('UPDATED_TOKEN:' + creds.to_json(), flush=True)

    os.unlink(tmp.name)
    return creds

# ─── PDF Parser ──────────────────────────────────────────────────────────────
def num(s):
    if not s:
        return 0
    s = str(s).strip().replace('$', '').strip()
    if not s or s == '-':
        return 0
    if ',' in s:
        s = s.replace('.', '').replace(',', '.')
    else:
        s = s.replace('.', '')
    try:
        return float(s)
    except ValueError:
        return 0

def parse_bci_pdf(path):
    import pdfplumber
    pages = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or '')

    p1 = pages[0]
    r = {
        'archivo': os.path.basename(path),
        'rut': '', 'cliente': '', 'fecha': '',
        'valor_uf': 0, 'valor_usd': 0, 'valor_eur': 0,
        'patrimonio_clp': 0, 'patrimonio_uf': 0, 'patrimonio_usd': 0,
        'caja': 0, 'renta_variable': 0, 'renta_fija': 0, 'op_liquidar': 0,
        'flujos': {},
        'acciones': [], 'cfi': [],
        'simultaneas': [], 'forwards': [],
        'movimientos_clp': [],
        'saldo_clp_inicial': 0, 'saldo_clp_final': 0,
        'total_forwards_resultado': 0,
        'total_simultaneas_resultado': 0,
    }

    m = re.search(r'Cliente:\s+(.+?)\s+RUT:\s+([\d.]+-[\dkK])', p1)
    if m:
        r['cliente'] = m.group(1).strip()
        r['rut'] = m.group(2).strip()
    m = re.search(r'Fecha Emisi.n:\s*(\d{2}/\d{2}/\d{4})', p1)
    if m:
        r['fecha'] = m.group(1)
    for key, pat in [
        ('valor_uf',  r'Valor UF[:\s]+\$\s*([\d.,]+)'),
        ('valor_usd', r'Valor USD[:\s]+\$\s*([\d.,]+)'),
        ('valor_eur', r'Valor EUR[:\s]+\$\s*([\d.,]+)'),
    ]:
        m = re.search(pat, p1)
        if m:
            r[key] = num(m.group(1))
    m = re.search(r'Pesos \(CLP\)\s+([-\d.,]+)', p1)
    if m:
        r['patrimonio_clp'] = num(m.group(1))
    m = re.search(r'Unid\. Fomento \(UF\)\s+([-\d.,]+)', p1)
    if m:
        r['patrimonio_uf'] = num(m.group(1))
    m = re.search(r'D.lar \(USD\)\s+([-\d.,]+)', p1)
    if m:
        r['patrimonio_usd'] = num(m.group(1))
    for key, pat in [
        ('caja', r'Caja\s+([-\d.,]+)'),
        ('renta_variable', r'Renta Variable\s+([-\d.,]+)'),
        ('renta_fija', r'Renta Fija\s+([-\d.,]+)'),
        ('op_liquidar', r'Operaciones por Liquidar\s+([-\d.,]+)'),
    ]:
        m = re.search(pat, p1)
        if m:
            r[key] = num(m.group(1))
    for prod in ['Dividendos', 'Cupones', 'Pactos', 'Dep.sitos', 'Simult.neas', 'Forwards', 'Futuros']:
        m = re.search(rf'{prod}\s+([-\d.,]+)\s+([-\d.,]+)', p1)
        if m:
            label = prod.replace('Dep.sitos','Depósitos').replace('Simult.neas','Simultáneas')
            r['flujos'][label] = {'0_30': num(m.group(1)), '31_90': num(m.group(2))}

    market = None
    for page_text in pages[1:]:
        lines = page_text.split('\n')
        if 'Detalle Cartera Renta Variable' in page_text:
            for i, line in enumerate(lines):
                line = line.strip()
                if 'Mercado: Acciones' in line:
                    market = 'acciones'; continue
                if 'Mercado: CFI' in line:
                    market = 'cfi'; continue
                m = re.match(
                    r'^([A-Z][A-Z0-9\-]+)\s+Activo:\s+([-\d.,]+)\s+([-\d.,]+)\s+([-\d.,]+)\s+([-\d.,]+)\s+([-\d.,]+)\s+([-\d.,]+)\s+([-\d.,]+)\s+([-\d.,]+)',
                    line)
                if m and market in ('acciones', 'cfi'):
                    instr = {
                        'ticker': m.group(1), 'nombre': '',  'sector': '',
                        'libre': num(m.group(2)), 'en_garantia': num(m.group(4)),
                        'a_plazo': num(m.group(5)), 'precio_compra': num(m.group(6)),
                        'precio_ultimo': num(m.group(7)), 'valor_mercado': num(m.group(8)),
                        'dividendos': num(m.group(9)),
                    }
                    if i+1 < len(lines):
                        m2 = re.search(r'Rubro:\s*(.*?)\s*Pasivo:', lines[i+1])
                        if m2:
                            instr['sector'] = m2.group(1).strip()
                    if i+2 < len(lines):
                        instr['nombre'] = lines[i+2].strip()
                    (r['acciones'] if market == 'acciones' else r['cfi']).append(instr)

        if 'Detalle Operaciones Vigentes Simult' in page_text:
            for line in lines:
                m = re.match(
                    r'^([A-Z][A-Z0-9\-]+)\s+([\d.,]+)\s+(\d+)d.as\s+([\d.,]+)%'
                    r'Venta Contado:\s*([\d\-/]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([-\d.,]+)',
                    line.strip())
                if m:
                    r['simultaneas'].append({
                        'instrumento': m.group(1), 'cantidad': num(m.group(2)),
                        'plazo_dias': int(m.group(3)), 'tasa': num(m.group(4)),
                        'fecha_venta_contado': m.group(5), 'precio_venta': num(m.group(6)),
                        'monto': num(m.group(7)), 'monto_amortizado': num(m.group(8)),
                        'valor_mercado': num(m.group(9)), 'resultado': num(m.group(10)),
                    })
            m = re.search(r'\+\s*([\d.,]+)\s*$', page_text, re.MULTILINE)
            if m:
                r['total_simultaneas_resultado'] = num(m.group(1))

        if 'Detalle Cartera Vigente FORWARD' in page_text:
            for line in lines:
                m = re.match(
                    r'^(\d+)\s+(Venta|Compra)\s+Seguro de Cambio\s+Nominal\s+([\d.,]+)\s+(\w+/\w+)'
                    r'.*?(\d{2}-\d{2}-\d{4}).*?Total\s+(\d+)\s+([\d.,]+)\s+([-\d.,]+)\s+([-\d.,]+)',
                    line.strip())
                if m:
                    r['forwards'].append({
                        'folio': m.group(1), 'tipo': m.group(2),
                        'monto_nominal': num(m.group(3)), 'moneda': m.group(4),
                        'fecha_inicio': m.group(5), 'plazo_total': int(m.group(6)),
                        'tc_cierre': num(m.group(7)), 'resultado_fwd': num(m.group(8)),
                        'valor_razonable': num(m.group(9)),
                    })
            m = re.search(r'Resultado\s+([-\d.,]+)', page_text)
            if m:
                r['total_forwards_resultado'] = num(m.group(1))

        if 'Movimientos de caja pesos' in page_text:
            m = re.search(r'Saldo Inicial del Periodo\s+([-\d.,]+)', page_text)
            if m:
                r['saldo_clp_inicial'] = num(m.group(1))
            m = re.search(r'Saldo Final del Periodo\s+([-\d.,]+)', page_text)
            if m:
                r['saldo_clp_final'] = num(m.group(1))
            for line in lines:
                m = re.match(
                    r'^(\d{2}/\d{2}/\d{4})\s+(\d+)\s+(.+?)\s+([\d.,]+)\s+(\d+)\s+([-\d.,]+)$',
                    line.strip())
                if m:
                    r['movimientos_clp'].append({
                        'fecha': m.group(1), 'ref': m.group(2),
                        'operacion': m.group(3).strip(),
                        'abono': num(m.group(4)), 'cargo': num(m.group(5)),
                        'saldo': num(m.group(6)),
                    })
    return r

# ─── DB ──────────────────────────────────────────────────────────────────────
def save_to_db(data):
    con = sqlite3.connect(DB_PATH)
    con.execute('''
        CREATE TABLE IF NOT EXISTS bci_cartolas (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            synced_at TEXT NOT NULL,
            fuente    TEXT,
            data      TEXT NOT NULL
        )
    ''')
    con.execute(
        'INSERT INTO bci_cartolas (synced_at, fuente, data) VALUES (?,?,?)',
        (data['sync_at'], data['fuente'], json.dumps(data, ensure_ascii=False))
    )
    con.commit()
    con.close()

# ─── Gmail ───────────────────────────────────────────────────────────────────
def find_latest_email(gmail):
    q = f'from:{REMITENTE} has:attachment filename:zip subject:Cartola'
    result = gmail.users().messages().list(userId='me', q=q, maxResults=1).execute()
    msgs = result.get('messages', [])
    return msgs[0]['id'] if msgs else None

def download_zip(gmail, msg_id):
    msg = gmail.users().messages().get(userId='me', id=msg_id, format='full').execute()
    subject = next((h['value'] for h in msg['payload']['headers'] if h['name']=='Subject'), '')
    print(f'Email: {subject}', flush=True)

    def find_parts(parts):
        for part in parts:
            fn = part.get('filename','')
            if fn.lower().endswith('.zip'):
                att_id = part['body'].get('attachmentId')
                if att_id:
                    att = gmail.users().messages().attachments().get(
                        userId='me', messageId=msg_id, id=att_id).execute()
                    return base64.urlsafe_b64decode(att['data']), fn
            if 'parts' in part:
                r = find_parts(part['parts'])
                if r[0]: return r
        return None, None

    return find_parts(msg['payload'].get('parts', [msg['payload']]))

# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    print(f'[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] BCI sync start', flush=True)
    creds = get_creds()
    gmail = build('gmail', 'v1', credentials=creds)

    msg_id = find_latest_email(gmail)
    if not msg_id:
        print('ERROR: No se encontró email BCI', flush=True)
        sys.exit(1)

    zip_data, fuente = download_zip(gmail, msg_id)
    if not zip_data:
        print('ERROR: No se encontró ZIP en el email', flush=True)
        sys.exit(1)

    print(f'ZIP: {fuente} ({len(zip_data)//1024}KB)', flush=True)
    sociedades = []
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        pdfs = sorted(n for n in z.namelist() if n.lower().endswith('.pdf'))
        print(f'PDFs: {pdfs}', flush=True)
        for pdf_name in pdfs:
            tmp = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
            tmp.write(z.read(pdf_name))
            tmp.close()
            try:
                soc = parse_bci_pdf(tmp.name)
                sociedades.append(soc)
                print(f'  {soc["rut"]} {soc["cliente"]} | CLP {soc["patrimonio_clp"]:,.0f}', flush=True)
            except Exception as e:
                print(f'  ERROR {pdf_name}: {e}', flush=True)
            finally:
                os.unlink(tmp.name)

    output = {
        'sync_at': datetime.datetime.now().isoformat(),
        'fuente': fuente,
        'sociedades': sociedades,
    }
    save_to_db(output)
    # También escribir JSON al stdout para que Node.js lo lea si quiere
    print('BCI_DATA:' + json.dumps(output, ensure_ascii=False), flush=True)
    print(f'OK: {len(sociedades)} sociedades guardadas en DB', flush=True)

if __name__ == '__main__':
    main()
