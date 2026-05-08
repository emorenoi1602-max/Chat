"""
update_dashboard.py
───────────────────────────────────────────────────
1. Extrae RAW_DATA del index.html actual (histórico)
2. Lee todos los .xlsx de /data (datos nuevos)
3. Detecta mercado desde prefijo del agente (MX-, US-, etc.)
4. Merge inteligente por (fecha + ulp + mercado)
5. Inyecta el resultado en index.html
"""

import os, re, json, glob
import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────
DATA_DIR  = "data"
DASHBOARD = "index.html"
PARTICLES = {"DE", "DEL", "LA", "LAS", "LOS", "EL", "Y"}

# ── NOMBRES FIJOS POR CÓDIGO ULP ─────────────────────────────────
# Formato: 'ATLCOLBAQ###': 'Nombre Apellido'
# Agregar nuevos asesores aquí cuando ingresen:
NAME_FIXES = {
    # Mercado MX - grupo nuevo abril 2026
    'ATLCOLBAQ314': 'Andrés Ruíz',
    'ATLCOLBAQ315': 'Luisa Castillo',
    'ATLCOLBAQ316': 'Pedro Piñeres',
    'ATLCOLBAQ317': 'Sharly Salcedo',
    'ATLCOLBAQ318': 'Cristina Vargas',
    'ATLCOLBAQ319': 'Juan Calderón',
    # Mercado US
    'ATLCOLBAQ292': 'Yineth Palomino',
    'ATLCOLBAQ293': 'Mireya Velasquez',
    'ATLCOLBAQ295': 'Andrea Mendez',
    'ATLCOLBAQ296': 'Emmanuel León',
    'ATLCOLBAQ297': 'Nicolas Mancilla',
    'ATLCOLBAQ298': 'Luis Caballero',
    'ATLCOLBAQ299': 'Ray Carlier',
    'ATLCOLBAQ300': 'Paula Rodriguez',
    'ATLCOLBAQ301': 'Juan Puello',
    'ATLCOLBAQ302': 'Mabelys Matute',
    'ATLCOLBAQ304': 'Alberto Miranda',
    'ATLCOLBAQ305': 'Lina Fernández',
    'ATLCOLBAQ306': 'Jean Chavez',
    'ATLCOLBAQ307': 'José Herrera',
    'ATLCOLBAQ308': 'Carlos Atencio',
    'ATLCOLBAQ309': 'Edgardo Garrido',
    'ATLCOLBAQ310': 'Vanessa Rua',
    'ATLCOLBAQ320': 'Johelis Barragan',
    'ATLCOLBAQ321': 'Wendy Pelaez',
    'ATLCOLBAQ322': 'Viviana Padilla',
    'ATLCOLBAQ323': 'Nancy Garizabalo',
    'ATLCOLBAQ324': 'Miguel Aldana',
    'ATLCOLBAQ325': 'Fabián Ramirez',
    'ATLCOLBAQ326': 'Melanie Hurtado',
    'ATLCOLBAQ328': 'Cristian Betancourt',
    'ATLCOLBAQ329': 'Alicia Troconis',
    'ATLCOLBAQ330': 'Valeria Reyes',
    'ATLCOLBAQ331': 'Gardenia Fontalvo',
    'ATLCOLBAQ332': 'Alex Urueta',
    'ATLCOLBAQ333': 'Alberto Barrios',
    'ATLCOLBAQ334': 'Erilyn Barrios',
    'ATLCOLBAQ335': 'Karina Bolivar',
    'ATLCOLBAQ336': 'Roberto Cardona',
    'ATLCOLBAQ337': 'Jessaint Escorcia',
    'ATLCOLBAQ338': 'Daniela Mozo',
    'ATLCOLBAQ339': 'Angelo Altamiranda',
    'ATLCOLBAQ340': 'Nallely Hernández',
    'ATLCOLBAQ341': 'Stefania Luna',
    'ATLCOLBAQ342': 'Ricardo Leal',
    'ATLCOLBAQ343': 'Carlos Marquez',
    'ATLCOLBAQ344': 'Carlos Ortega',
    'ATLCOLBAQ345': 'Johannys Padilla',
    'ATLCOLBAQ346': 'Giovanna Hernández',
    'ATLCOLBAQ347': 'Henry Leon',
    'ATLCOLBAQ348': 'Michelle Pacheco',
    'ATLCOLBAQ349': 'Alejandra Parejo',
    # → Nuevos ingresos van aquí:
    # 'ATLCOLBAQ350': 'Nombre Apellido',
}
# ─────────────────────────────────────────────────────────────────


def detect_market(agent_name: str, ulp_code: str = '') -> str:
    """
    Detecta el mercado desde el prefijo del nombre del agente.
    Ejemplos: 'MX-ATLCOLBAQ001-...' → 'MX'
              'US-ATLCOLBAQ307-...' → 'US'
    Si no hay prefijo reconocible, devuelve 'MX' por defecto.
    """
    raw = str(agent_name).strip()
    m = re.match(r'^([A-Z]{2,3})-', raw)
    if m:
        return m.group(1)
    return 'MX'


def parse_name(agent_name: str, ulp_code: str = '') -> str:
    """
    Extrae primer nombre + primer apellido desde el nombre completo del agente.
    Soporta cualquier prefijo de mercado: MX-, US-, etc.
    Si el código ULP está en NAME_FIXES, usa ese nombre directamente.
    """
    # Primero verificar si el código ULP tiene nombre fijo
    code = str(ulp_code).strip()
    if code in NAME_FIXES:
        return NAME_FIXES[code]

    raw = str(agent_name).strip()

    # Regex genérico: cualquier prefijo de mercado (2-3 letras)
    m = re.match(
        r'^[A-Z]{2,3}-[A-Z0-9]+-(.+)-(?:Colombia|Mexico|Peru|Ecuador|Venezuela)$',
        raw, re.IGNORECASE
    )
    if not m:
        # Intentar por código en el nombre
        code_in_name = re.search(r'(ATLCOLBAQ\d+)', raw, re.IGNORECASE)
        if code_in_name and code_in_name.group(1).upper() in NAME_FIXES:
            return NAME_FIXES[code_in_name.group(1).upper()]
        return raw

    name = re.sub(r'[\s\u00A0]+', ' ', m.group(1)).strip()
    words = name.split()
    if not words:
        return raw

    cap = lambda w: w[0].upper() + w[1:].lower()
    first = cap(words[0])
    if len(words) == 1:
        return first
    if len(words) == 2:
        return first + ' ' + cap(words[1])

    # Buscar primer apellido (saltando partículas: de, del, la, etc.)
    idx = 2
    while idx < len(words) and words[idx].upper() in PARTICLES:
        idx += 1
    if idx >= len(words):
        idx = len(words) - 1

    return first + ' ' + cap(words[idx])


def iso_week(d: pd.Timestamp) -> int:
    return int(d.isocalendar().week)


def extract_historical(html: str) -> list:
    """Extrae el RAW_DATA embebido en el HTML actual como base histórica."""
    m = re.search(r'const RAW_DATA=(\[.*?\]);', html, flags=re.DOTALL)
    if not m:
        print('  ⚠️  No se encontró RAW_DATA en el HTML — iniciando desde cero.')
        return []
    try:
        records = json.loads(m.group(1))
        # Garantizar que todos los registros históricos tengan mk
        for r in records:
            if 'mk' not in r:
                r['mk'] = 'MX'
        dates = sorted(set(r['d'] for r in records))
        mk_counts = {}
        for r in records:
            mk_counts[r['mk']] = mk_counts.get(r['mk'], 0) + 1
        print(f'  📚 Histórico: {len(records):,} registros ({dates[0]} → {dates[-1]})')
        for mk, cnt in sorted(mk_counts.items()):
            print(f'     {mk}: {cnt:,} registros')
        return records
    except Exception as e:
        print(f'  ⚠️  Error al parsear histórico: {e}')
        return []


def load_excel(path: str) -> pd.DataFrame:
    """Carga un Excel y extrae mercado, código ULP y nombre desde Agent Name."""
    print(f'  📂 Leyendo: {os.path.basename(path)}')
    df = pd.read_excel(path, parse_dates=['Date'])
    df = df[df['Served chats'] > 0].copy()

    # Extraer mercado y código ULP
    df['mk']    = df['Agent Name'].apply(detect_market)
    df['ulp']   = df.get('ULP Account', pd.Series([''] * len(df))).fillna('').str.strip()

    # Si no hay columna ULP Account, extraer del nombre
    if df['ulp'].eq('').all():
        df['ulp'] = df['Agent Name'].str.extract(r'-(ATLCOLBAQ\d+)-', flags=re.IGNORECASE)[0].fillna('')

    df['short_name'] = df.apply(
        lambda row: parse_name(row['Agent Name'], row['ulp']), axis=1
    )
    df['date_str'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['week']     = df['Date'].apply(iso_week)
    df['month']    = df['Date'].dt.month

    mk_counts = df.groupby('mk').size().to_dict()
    print(f'     → {len(df)} registros | {df["date_str"].min()} → {df["date_str"].max()}')
    for mk, cnt in sorted(mk_counts.items()):
        print(f'        {mk}: {cnt} registros')
    return df


def df_to_records(df: pd.DataFrame) -> list:
    """Convierte DataFrame a lista de registros JSON para el dashboard."""
    records = []
    for _, row in df.iterrows():
        records.append({
            'd':   row['date_str'],
            'w':   int(row['week']),
            'm':   int(row['month']),
            'mk':  row['mk'],                    # ← mercado MX / US
            'ulp': row['ulp'],                   # ← código ATLCOLBAQ
            'a':   row['short_name'],
            'sc':  int(row['Served chats']),
            'arc': int(row.get('Assigned&Replied Chat', 0) or 0),
            'bc':  int(row['Bad chats']),
            'ed':  round(float(row['Engagement duration（min）']), 2),
            'ot':  round(float(row['Online Time（H）']), 3),
            'st':  round(float(row['Serving Time（H）']), 3),
            's1':  int(row['is30s1ServedID']),
            'n1':  int(row['not30s1ServedID']),
            's2':  int(row['30s2 Served Engagements']),
            'n2':  int(row['Non-30s2 Served Engagements']),
            'me':  int(row['Missed Engagements']),
        })
    return records


def merge_records(historical: list, new_records: list) -> list:
    """
    Merge inteligente por clave (fecha + ulp/agente + mercado).
    - Histórico del HTML se conserva para todo lo que no esté en los Excel nuevos
    - Registros nuevos reemplazan los históricos si coinciden en fecha+agente+mercado
    - Nunca mezcla mercados entre sí
    """
    def rec_key(r):
        # Clave única: ulp si existe, sino nombre del agente
        id_part = r.get('ulp') or r.get('a', '')
        return (r['d'], id_part, r.get('mk', 'MX'))

    # Construir índice desde histórico
    index = {rec_key(r): r for r in historical}

    new_days_by_market = {}
    for r in new_records:
        mk = r.get('mk', 'MX')
        new_days_by_market.setdefault(mk, set()).add(r['d'])
        index[rec_key(r)] = r  # nuevo reemplaza histórico si misma clave

    merged = sorted(index.values(), key=lambda r: (r['d'], r.get('mk', 'MX'), r.get('a', '')))

    # Resumen
    print(f'\n  📊 Resultado del merge:')
    print(f'     Registros totales : {len(merged):,}')
    all_dates = sorted(set(r['d'] for r in merged))
    print(f'     Rango final       : {all_dates[0]} → {all_dates[-1]}')
    mk_summary = {}
    for r in merged:
        mk = r.get('mk', 'MX')
        mk_summary[mk] = mk_summary.get(mk, 0) + 1
    for mk, cnt in sorted(mk_summary.items()):
        mk_dates = sorted(set(r['d'] for r in merged if r.get('mk') == mk))
        print(f'     {mk}: {cnt:,} registros ({mk_dates[0]} → {mk_dates[-1]})')

    return merged


def inject_data(html: str, records: list) -> str:
    new_data = json.dumps(records, ensure_ascii=False)
    updated  = re.sub(r'const RAW_DATA=\[.*?\];', f'const RAW_DATA={new_data};',
                      html, flags=re.DOTALL)
    if updated == html:
        raise ValueError('No se encontró RAW_DATA en index.html')
    return updated


def main():
    # 1. Leer HTML y extraer histórico
    if not os.path.exists(DASHBOARD):
        raise FileNotFoundError(f'No se encontró {DASHBOARD}')

    print(f'\n📖 Leyendo {DASHBOARD}...')
    with open(DASHBOARD, 'r', encoding='utf-8') as f:
        html = f.read()

    historical = extract_historical(html)

    # 2. Buscar Excel en /data
    xlsx_files = sorted(
        glob.glob(os.path.join(DATA_DIR, '*.xlsx')) +
        glob.glob(os.path.join(DATA_DIR, '*.xls'))
    )

    if not xlsx_files:
        print('\n⚠️  No hay archivos Excel en /data — histórico sin cambios.')
        return

    print(f'\n🔍 Archivos en /{DATA_DIR}:')
    for f in xlsx_files:
        print(f'   • {os.path.basename(f)}')

    # 3. Cargar y unir Excel
    frames    = [load_excel(f) for f in xlsx_files]
    merged_df = pd.concat(frames, ignore_index=True)
    # Dedup interno por (fecha + ulp + mercado)
    merged_df = merged_df.drop_duplicates(
        subset=['date_str', 'ulp', 'mk'], keep='last'
    )
    new_records = df_to_records(merged_df)

    # 4. Merge con histórico
    final = merge_records(historical, new_records)

    agents = sorted(set(r['a'] for r in final))
    print(f'     Agentes activos   : {len(agents)}')

    # 5. Inyectar y guardar
    html_updated = inject_data(html, final)
    with open(DASHBOARD, 'w', encoding='utf-8') as f:
        f.write(html_updated)

    size_kb = os.path.getsize(DASHBOARD) // 1024
    print(f'\n🚀 {DASHBOARD} actualizado ({size_kb} KB)')
    print(f'   Visible en GitHub Pages en ~1-2 minutos.\n')


if __name__ == '__main__':
    main()
