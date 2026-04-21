"""
update_dashboard.py
───────────────────
Lee los archivos .xlsx de /data, los combina con la data histórica
que ya está embebida en index.html, y publica el resultado.
La data histórica NUNCA se pierde aunque no estén todos los Excel.
"""

import os, re, json, glob
import pandas as pd

# ── CONFIG ────────────────────────────────────────────────
DATA_DIR   = "data"
DASHBOARD  = "index.html"
PARTICLES  = {"DE","DEL","LA","LAS","LOS","EL","Y"}
# ─────────────────────────────────────────────────────────


def parse_name(raw: str) -> str:
    raw = str(raw).strip()
    m = re.match(
        r"^MX-[A-Z0-9]+-(.+)-(?:Colombia|Mexico|Peru|Ecuador|Venezuela)$",
        raw, re.IGNORECASE
    )
    if not m:
        parts = raw.split("-")
        return parts[1] if len(parts) > 1 else raw
    name = re.sub(r"[\s\u00A0]+", " ", m.group(1)).strip()
    words = name.split()
    if not words:
        return raw
    cap = lambda w: w[0].upper() + w[1:].lower()
    first = cap(words[0])
    if len(words) == 1: return first
    if len(words) == 2: return first + " " + cap(words[1])
    idx = 2
    while idx < len(words) and words[idx].upper() in PARTICLES:
        idx += 1
    if idx >= len(words):
        idx = len(words) - 1
    return first + " " + cap(words[idx])


def iso_week(d: pd.Timestamp) -> int:
    return int(d.isocalendar().week)


def load_existing_data(html_path: str) -> list:
    """Extrae RAW_DATA que ya está embebida en el index.html."""
    if not os.path.exists(html_path):
        return []
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()
    m = re.search(r"const RAW_DATA=(\[.*?\]);", html, re.DOTALL)
    if not m:
        return []
    try:
        records = json.loads(m.group(1))
        print(f"  📦 Data histórica en {html_path}: {len(records):,} registros")
        return records
    except Exception as e:
        print(f"  ⚠️  No se pudo leer RAW_DATA del HTML: {e}")
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


def merge_records(existing: list, new_records: list) -> list:
    """
    Combina registros existentes con los nuevos.
    Para días que aparecen en ambos, los nuevos tienen prioridad.
    La data histórica que no está en los Excel nuevos se conserva intacta.
    """
    # Fechas cubiertas por los nuevos Excel
    new_dates = set(r["d"] for r in new_records)

    # Conservar histórico que NO está en los nuevos archivos
    historical = [r for r in existing if r["d"] not in new_dates]

    # Combinar: histórico + nuevos
    combined = historical + new_records

    # Ordenar por fecha y agente
    combined.sort(key=lambda r: (r["d"], r["a"]))

    return combined


def inject_data(html: str, records: list) -> str:
    new_data = json.dumps(records, ensure_ascii=False)
    updated  = re.sub(
        r"const RAW_DATA=\[.*?\];",
        f"const RAW_DATA={new_data};",
        html, flags=re.DOTALL
    )
    if updated == html:
        raise ValueError("No se encontró RAW_DATA en el HTML.")
    return updated


def main():
    xlsx_files = sorted(
        glob.glob(os.path.join(DATA_DIR, "*.xlsx")) +
        glob.glob(os.path.join(DATA_DIR, "*.xls"))
    )

    if not xlsx_files:
        print("⚠️  No hay archivos Excel en /data. No se modifica el dashboard.")
        return

    print(f"\n🔍 Archivos nuevos en /{DATA_DIR}:")
    for f in xlsx_files:
        print(f"   • {os.path.basename(f)}")

    # 1. Extraer data histórica del HTML actual
    print(f"\n📖 Leyendo data histórica de {DASHBOARD}...")
    existing_records = load_existing_data(DASHBOARD)

    # 2. Leer todos los Excel nuevos
    print(f"\n📊 Procesando archivos Excel...")
    frames = [load_excel(f) for f in xlsx_files]
    combined_df = pd.concat(frames, ignore_index=True)
    # Si mismo día aparece en varios Excel, queda el último
    combined_df = combined_df.drop_duplicates(
        subset=["date_str", "short_name"], keep="last"
    )
    new_records = df_to_records(combined_df)

    # 3. Merge inteligente
    print(f"\n🔀 Combinando con data histórica...")
    new_dates = sorted(set(r["d"] for r in new_records))
    print(f"   Días en Excel nuevos  : {len(new_dates)} ({new_dates[0]} → {new_dates[-1]})")
    print(f"   Registros históricos  : {len(existing_records):,}")
    print(f"   Registros nuevos      : {len(new_records):,}")

    final_records = merge_records(existing_records, new_records)

    # Stats finales
    all_dates  = sorted(set(r["d"] for r in final_records))
    all_agents = sorted(set(r["a"] for r in final_records))

    print(f"\n✅ Dataset final consolidado:")
    print(f"   Registros totales : {len(final_records):,}")
    print(f"   Rango de fechas   : {all_dates[0]} → {all_dates[-1]}")
    print(f"   Días únicos       : {len(all_dates)}")
    print(f"   Agentes activos   : {len(all_agents)}")

    # 4. Inyectar en el HTML
    if not os.path.exists(DASHBOARD):
        raise FileNotFoundError(f"No se encontró {DASHBOARD}")

    with open(DASHBOARD, "r", encoding="utf-8") as f:
        html = f.read()

    html_updated = inject_data(html, final_records)

    with open(DASHBOARD, "w", encoding="utf-8") as f:
        f.write(html_updated)

    size_kb = os.path.getsize(DASHBOARD) // 1024
    print(f"\n🚀 {DASHBOARD} actualizado ({size_kb} KB)")
    print(f"   Visible en GitHub Pages en ~1-2 minutos.\n")


if __name__ == "__main__":
    main()
