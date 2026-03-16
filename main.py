import asyncio
import httpx
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# --- Configuración ---
ATTIO_API_KEY = os.getenv("ATTIO_API_KEY")
DEALS_ID = "dbcd94bf-ec33-4f00-a7c8-74f57a559869"
DEAL_FLOW_ID = "54265eb6-d53d-465d-ad35-4e823e135629"

TYPEBOT_API_TOKEN = os.getenv("TYPEBOT_API_TOKEN")
TYPEBOT_ID = os.getenv("TYPEBOT_ID")
TYPEBOT_BASE_URL = os.getenv("TYPEBOT_BASE_URL", "https://app.typebot.io/api")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
OPEN_CALL_START_DATE = os.getenv("OPEN_CALL_START_DATE", "2026-02-13")
TZ = os.getenv("TZ", "Europe/Madrid")

# ==============================================================================
# HELPERS (IDÉNTICOS AL ORIGINAL)
# ==============================================================================

def get_day_number():
    start = datetime.strptime(OPEN_CALL_START_DATE, "%Y-%m-%d")
    now = datetime.now()
    return (now - start).days + 1

def extract_value(attr_list):
    """Extrae el valor simple (como hacía extractValue en JS)."""
    if not attr_list or not isinstance(attr_list, list): return None
    v = attr_list[0]
    if "status" in v: return v["status"].get("title")
    if "option" in v: return v["option"].get("title")
    if "value" in v: return v["value"]
    return None

def extract_multi_values(attr_list):
    """Extrae lista de valores (como hacía extractMultiValues en JS)."""
    if not attr_list: return []
    results = []
    for item in attr_list:
        val = None
        if "option" in item: val = item["option"].get("title")
        elif "status" in item: val = item["status"].get("title")
        elif "value" in item: val = str(item["value"])
        if val: results.append(val)
    return results

# ==============================================================================
# LLAMADAS API ASÍNCRONAS
# ==============================================================================

async def fetch_attio(client, url, payload=None):
    all_data = []
    cursor = None
    while True:
        body = payload.copy() if payload else {}
        if cursor: body["cursor"] = cursor
        body["limit"] = 100
        
        headers = {"Authorization": f"Bearer {ATTIO_API_KEY}", "Content-Type": "application/json"}
        res = await client.post(url, headers=headers, json=body)
        res.raise_for_status()
        data = res.json()
        all_data.extend(data.get("data", []))
        cursor = data.get("pagination", {}).get("next_cursor")
        if not cursor: break
    return all_data

async def fetch_typebot(client):
    if not TYPEBOT_API_TOKEN: return 0
    url = f"{TYPEBOT_BASE_URL}/v1/typebots/{TYPEBOT_ID}/results"
    headers = {"Authorization": f"Bearer {TYPEBOT_API_TOKEN}"}
    count = 0
    cursor = None
    while True:
        params = {"limit": 100}
        if cursor: params["cursor"] = cursor
        res = await client.get(url, headers=headers, params=params)
        if res.status_code != 200: break
        data = res.json()
        results = data.get("results", [])
        count += sum(1 for r in results if r.get("hasStarted") and not r.get("isCompleted"))
        cursor = data.get("nextCursor")
        if not cursor or not results: break
    return count

# ==============================================================================
# PROCESAMIENTO Y REPORTE
# ==============================================================================

async def send_daily_metrics():
    async with httpx.AsyncClient(timeout=60.0) as client:
        # 1. Recolectar datos
        tasks = [
            fetch_attio(client, f"https://api.attio.com/v2/objects/{DEALS_ID}/records/query", 
                       payload={"filter": {"$or": [{"stage": {"$eq": "Menorca 2026"}}, {"stage": {"$eq": "Leads Menorca 2026"}}]}}),
            fetch_attio(client, f"https://api.attio.com/v2/lists/{DEAL_FLOW_ID}/entries/query"),
            fetch_typebot(client)
        ]
        raw_records, raw_entries, in_progress = await asyncio.gather(*tasks)

    # 2. Procesamiento de DataFrames con LIMPIEZA
    df_rec = pd.DataFrame([{
        "record_id": r["id"]["record_id"],
        "name": extract_value(r["values"].get("name")),
        "reference_3": str(extract_value(r["values"].get("reference_3")) or 'Other').strip(),
        "stage": extract_value(r["values"].get("stage"))
    } for r in raw_records])

    df_ent = pd.DataFrame([{
        "record_id": e["parent_record_id"],
        "status": str(extract_value(e["entry_values"].get("status")) or 'Unknown').strip(),
        "status_date": e["entry_values"].get("status", [{}])[0].get("active_from"),
        "red_flags": [str(f).strip() for f in extract_multi_values(e["entry_values"].get("red_flags_form_7"))]
    } for e in raw_entries])

    df = pd.merge(df_rec, df_ent, on="record_id")
    
    # 3. Cálculos de tiempos y métricas
    submitted = len(df)
    day_num = get_day_number()
    one_day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    
    # Filtrar novedades de las últimas 24h
    new_qualified = df[
        (df['status'].isin(['Initial screening', 'First interaction', 'Deep dive'])) & 
        (df['status_date'] >= one_day_ago)
    ]

    # --- 1. PREPARACIÓN DE DATOS (Limpieza total) ---
    df['status'] = df['status'].astype(str).str.strip()
    st_counts = df['status'].value_counts()
    
    # --- 2. INICIO DEL MENSAJE ---
    W = 50
    msg = []
    msg.append("\n" + "═" * W)
    msg.append(f"Open Call – Day {get_day_number()} Update")
    msg.append("═" * W + "\n")

    # Bloque: Resumen General
    msg.append(f"Applications submitted: {len(df)}")
    msg.append(f"Applications in progress: {in_progress}")
    msg.append(f"Total interest (sum): {len(df) + in_progress}\n")

    # Bloque: Source Breakdown
    msg.append("─" * W)
    msg.append("Submitted Applications – Source Breakdown")
    msg.append("─" * W)
    sources = df['reference_3'].fillna('Other').value_counts()
    for src, count in sources.items():
        pct = (count / len(df) * 100) if len(df) > 0 else 0
        msg.append(f"  {src}: {count} ({pct:.1f}%)")

    # Bloque: Pipeline Status (ELIMINAMOS EL DESORDEN AQUÍ)
    msg.append("\n" + "─" * W)
    msg.append("Current Pipeline Status (Total)")
    msg.append("─" * W)
    estados_ordenados = [
        'Not qualified', 'Contacted', 'Stand by', 'Initial screening', 
        'First interaction', 'Deep dive', 'Pre-committee'
    ]
    for st in estados_ordenados:
        msg.append(f"  {st}: {st_counts.get(st, 0)}")

    # Bloque: Novedades últimas 24h
    msg.append("\n" + "─" * W)
    msg.append("New Qualified / In Play (last 24 h)")
    msg.append("─" * W)
    if not new_qualified.empty:
        for _, row in new_qualified.iterrows():
            msg.append(f"  {row['name']} → {row['status']}")
    else:
        msg.append("  (none)")

    # Bloque: Red Flags (LIMPIEZA DE DUPLICADOS)
    msg.append("\n" + "─" * W)
    msg.append("Main Red Flags (Total)")
    msg.append("─" * W)
    
    df_nq = df[df['status'] == 'Not qualified']
    if not df_nq.empty:
        # Explode y limpieza de espacios en blanco
        all_flags = df_nq['red_flags'].explode().dropna().astype(str).str.strip()
        all_flags = all_flags[all_flags != ""] # Quitar strings vacíos
        
        if not all_flags.empty:
            top_flags = all_flags.value_counts()
            for flag, count in top_flags.items():
                pct = (count / len(df_nq) * 100)
                msg.append(f"  {flag} ({pct:.1f}%)")
        else:
            msg.append("  (none)")
    else:
        msg.append("  (No applications disqualified yet)")

    msg.append("\n" + "═" * W)
    
    # 5. Envío
    final_text = "\n".join(msg)
    print(final_text)
    
    if SLACK_WEBHOOK_URL:
        httpx.post(SLACK_WEBHOOK_URL, json={"text": f"```\n{final_text}\n```"})

if __name__ == "__main__":
    asyncio.run(send_daily_metrics())
