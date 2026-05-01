"""
update_dashboard.py
───────────────────────────────────────────────────
1. Extrae RAW_DATA del index.html actual (histórico)
2. Lee todos los .xlsx de /data (datos nuevos)
3. Merge inteligente: nuevos reemplazan días duplicados,
   histórico se conserva aunque no esté en /data
4. Inyecta el resultado en index.html
"""

import os, re, json, glob
import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────
DATA_DIR   = "data"
DASHBOARD  = "index.html"
PARTICLES  = {"DE","DEL","LA","LAS","LOS","EL","Y"}

# ── NOMBRES FIJOS POR CÓDIGO ──────────────────────────────────────
# Primer nombre + primer apellido.
# Cuando ingresen asesores nuevos, agrega la línea aquí:
# 'ATLCOLBAQxxx': 'Nombre Apellido',
NAME_FIXES = {
    'ATLCOLBAQ314': 'Andrés Ruíz',
    'ATLCOLBAQ315': 'Luisa Castillo',
    'ATLCOLBAQ316': 'Pedro Piñeres',
    'ATLCOLBAQ317': 'Sharly Salcedo',
    'ATLCOLBAQ318': 'Cristina Vargas',
    'ATLCOLBAQ319': 'Juan Calderón',
    # → Nuevos ingresos van aquí:
    # 'ATLCOLBAQ320': 'Nombre Apellido',
}
# ─────────────────────────────────────────────────────────────────


def parse_name(raw: str) -> str:
    raw = str(raw).strip()
    if raw in NAME_FIXES:
        return NAME_FIXES[raw]
    m = re.match(
        r"^MX-[A-Z0-9]+-(.+)-(?:Colombia|Mexico|Peru|Ecuador|Venezuela)$",
        raw, re.IGNORECASE
    )
    if not m:
        parts = raw.split("-")
        code = parts[0] if parts else raw
        if code in NAME_FIXES:
            return NAME_FIXES[code]
        return parts[1] if len(parts) > 1 else raw

    name = re.sub(r"[\s\u00A0]+", " ", m.group(1)).strip()
    words = name.split()
    if not words:
        return raw
    cap = lambda w: w[0].upper() + w[1:].lower()
    first = cap(words[0])
    if len(words) == 1:
        return first
    if len(words) == 2:
        return first + " " + cap(words[1])
    idx = 2
    while idx < len(words) and words[idx].upper() in PARTICLES:
        idx += 1
    if idx >= len(words):
        idx = len(words) - 1
    return first + " " + cap(words[idx])


def apply_name_fixes(records: list) -> list:
    for r in records:
        if r['a'] in NAME_FIXES:
            r['a'] = NAME_FIXES[r['a']]
    return records


def iso_week(d: pd.Timestamp) -> int:
    return int(d.isocalendar().week)


def extract_historical(html: str) -> list:
    """Extrae el RAW_DATA embebido en el HTML actual como base histórica."""
    m = re.search(r"const RAW_DATA=(\[.*?\]);", html, flags=re.DOTALL)
    if not m:
        print("  ⚠️  No se encontró RAW_DATA histórico en el HTML — se inicia desde cero.")
        return []
    try:
        records = json.loads(m.group(1))
        dates = sorted(set(r["d"] for r in records))
        print(f"  📚 Histórico extraído del HTML: {len(records):,} registros ({dates[0]} → {dates[-1]})")
        return records
    except Exception as e:
        print(f"  ⚠️  Error al parsear histórico: {e} — se inicia desde cero.")
        return []


def load_excel(path: str) -> pd.DataFrame:
    print(f"  📂 Leyendo: {os.path.basename(path)}")
    df = pd.read_excel(path, parse_dates=["Date"])
    df = df[df["Served chats"] > 0].copy()
    df["short_name"] = df["Agent Name"].apply(parse_name)
    df["date_str"]   = df["Date"].dt.strftime("%Y-%m-%d")
    df["week"]       = df["Date"].apply(iso_week)
    df["month"]      = df["Date"].dt.month
    print(f"     → {len(df)} registros | {df['date_str'].min()} → {df['date_str'].max()}")
    return df


def df_to_records(df: pd.DataFrame) -> list:
    records = []
    for _, row in df.iterrows():
        records.append({
            "d":   row["date_str"],
            "w":   int(row["week"]),
            "m":   int(row["month"]),
            "a":   row["short_name"],
            "sc":  int(row["Served chats"]),
            "arc": int(row.get("Assigned&Replied Chat", 0) or 0),
            "bc":  int(row["Bad chats"]),
            "ed":  round(float(row["Engagement duration（min）"]), 2),
            "ot":  round(float(row["Online Time（H）"]), 3),
            "st":  round(float(row["Serving Time（H）"]), 3),
            "s1":  int(row["is30s1ServedID"]),
            "n1":  int(row["not30s1ServedID"]),
            "s2":  int(row["30s2 Served Engagements"]),
            "n2":  int(row["Non-30s2 Served Engagements"]),
            "me":  int(row["Missed Engagements"]),
        })
    return records


def merge_records(historical: list, new_records: list) -> list:
    """
    Merge inteligente:
    - Base: histórico del HTML (Ene–Mar y lo que ya estaba)
    - Los registros nuevos (del Excel) reemplazan días duplicados
    - El histórico se conserva íntegro para días no presentes en los Excel
    """
    # Construir índice del histórico: (fecha, agente) → registro
    index = {(r["d"], r["a"]): r for r in historical}

    new_days = set()
    for r in new_records:
        key = (r["d"], r["a"])
        index[key] = r  # nuevo reemplaza histórico si mismo día+agente
        new_days.add(r["d"])

    merged = sorted(index.values(), key=lambda r: (r["d"], r["a"]))

    hist_days  = sorted(set(r["d"] for r in historical))
    final_days = sorted(set(r["d"] for r in merged))

    print(f"\n  📊 Resultado del merge:")
    print(f"     Días en histórico  : {len(hist_days)}")
    print(f"     Días en Excel nuevo: {len(new_days)}")
    print(f"     Días en resultado  : {len(final_days)}")
    print(f"     Registros totales  : {len(merged):,}")
    print(f"     Rango final        : {final_days[0]} → {final_days[-1]}")

    return merged


def inject_data(html: str, records: list) -> str:
    new_data = json.dumps(records, ensure_ascii=False)
    updated  = re.sub(r"const RAW_DATA=\[.*?\];", f"const RAW_DATA={new_data};",
                      html, flags=re.DOTALL)
    if updated == html:
        raise ValueError("No se encontró RAW_DATA en el HTML.")
    return updated


def main():
    # 1. Leer HTML actual y extraer histórico
    if not os.path.exists(DASHBOARD):
        raise FileNotFoundError(f"No se encontró {DASHBOARD} en la raíz del repo.")

    print(f"\n📖 Leyendo {DASHBOARD}...")
    with open(DASHBOARD, "r", encoding="utf-8") as f:
        html = f.read()

    historical = extract_historical(html)

    # 2. Buscar Excel nuevos en /data
    xlsx_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.xlsx")) +
                        glob.glob(os.path.join(DATA_DIR, "*.xls")))

    if not xlsx_files:
        print("\n⚠️  No se encontraron archivos Excel en /data.")
        print("   El dashboard conserva los datos históricos sin cambios.")
        return

    print(f"\n🔍 Archivos Excel en /{DATA_DIR}:")
    for f in xlsx_files:
        print(f"   • {os.path.basename(f)}")

    # 3. Cargar Excel y convertir a registros
    frames      = [load_excel(f) for f in xlsx_files]
    merged_df   = pd.concat(frames, ignore_index=True)
    merged_df   = merged_df.drop_duplicates(subset=["date_str", "short_name"], keep="last")
    new_records = df_to_records(merged_df)

    # 4. Merge histórico + nuevos
    final = merge_records(historical, new_records)

    # 5. Aplicar correcciones de nombres (pasada final)
    final = apply_name_fixes(final)

    agents = sorted(set(r["a"] for r in final))
    print(f"     Agentes activos   : {len(agents)}")

    # 6. Inyectar y guardar
    html_updated = inject_data(html, final)

    with open(DASHBOARD, "w", encoding="utf-8") as f:
        f.write(html_updated)

    size_kb = os.path.getsize(DASHBOARD) // 1024
    print(f"\n🚀 {DASHBOARD} actualizado ({size_kb} KB)")
    print(f"   Los cambios serán visibles en GitHub Pages en ~1-2 minutos.\n")


if __name__ == "__main__":
    main()
