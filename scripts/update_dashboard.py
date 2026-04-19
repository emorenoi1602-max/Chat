"""
update_dashboard.py
───────────────────
Lee todos los archivos .xlsx de la carpeta /data,
los consolida en un único dataset sin duplicados de días,
e inyecta los datos actualizados en Shein_BPO_Dashboard.html
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


def iso_week(d: pd.Timestamp) -> int:
    return int(d.isocalendar().week)


def load_excel(path: str) -> pd.DataFrame:
    print(f"  📂 Leyendo: {os.path.basename(path)}")
    df = pd.read_excel(path, parse_dates=["Date"])
    df = df[df["Served chats"] > 0].copy()
    df["short_name"] = df["Agent Name"].apply(parse_name)
    df["date_str"]   = df["Date"].dt.strftime("%Y-%m-%d")
    df["week"]       = df["Date"].apply(iso_week)
    df["month"]      = df["Date"].dt.month
    print(f"     → {len(df)} registros activos | {df['date_str'].min()} → {df['date_str'].max()}")
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


def merge_dataframes(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Une todos los DataFrames. Si un mismo día aparece en varios archivos,
    prevalece el del archivo más reciente (mayor fecha de modificación = último en la lista).
    """
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Para días duplicados: el último archivo cargado gana (drop_duplicates mantiene 'last')
    combined = combined.sort_values(["date_str", "Agent Name"])
    # Deduplicar por día + agente manteniendo la última aparición
    combined = combined.drop_duplicates(subset=["date_str", "short_name"], keep="last")

    return combined.sort_values("date_str")


def inject_data(html: str, records: list) -> str:
    new_data = json.dumps(records, ensure_ascii=False)
    updated  = re.sub(r"const RAW_DATA=\[.*?\];", f"const RAW_DATA={new_data};",
                      html, flags=re.DOTALL)
    if updated == html:
        raise ValueError("No se encontró RAW_DATA en el HTML. Verifica que el dashboard es correcto.")
    return updated


def main():
    # 1. Buscar todos los xlsx en /data ordenados por nombre (cronológico si los nombras bien)
    xlsx_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.xlsx")) +
                        glob.glob(os.path.join(DATA_DIR, "*.xls")))

    if not xlsx_files:
        print("⚠️  No se encontraron archivos Excel en /data. Nada que actualizar.")
        return

    print(f"\n🔍 Archivos encontrados en /{DATA_DIR}:")
    for f in xlsx_files:
        print(f"   • {os.path.basename(f)}")

    # 2. Cargar y consolidar
    frames = [load_excel(f) for f in xlsx_files]
    merged = merge_dataframes(frames)

    records = df_to_records(merged)

    dates  = sorted(set(r["d"] for r in records))
    agents = sorted(set(r["a"] for r in records))

    print(f"\n✅ Dataset consolidado:")
    print(f"   Registros activos : {len(records):,}")
    print(f"   Rango de fechas   : {dates[0]} → {dates[-1]}")
    print(f"   Días únicos       : {len(dates)}")
    print(f"   Agentes activos   : {len(agents)}")

    # 3. Inyectar en el HTML
    if not os.path.exists(DASHBOARD):
        raise FileNotFoundError(f"No se encontró {DASHBOARD} en la raíz del repo.")

    with open(DASHBOARD, "r", encoding="utf-8") as f:
        html = f.read()

    html_updated = inject_data(html, records)

    with open(DASHBOARD, "w", encoding="utf-8") as f:
        f.write(html_updated)

    size_kb = os.path.getsize(DASHBOARD) // 1024
    print(f"\n🚀 {DASHBOARD} actualizado ({size_kb} KB)")
    print(f"   Los cambios serán visibles en GitHub Pages en ~1-2 minutos.\n")


if __name__ == "__main__":
    main()
