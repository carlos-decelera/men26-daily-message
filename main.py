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

    # 2. Transformar a DataFrames
    df_rec = pd.DataFrame([{
        "record_id": r["id"]["record_id"],
        "name": extract_value(r["values"].get("name")),
        "reference_3": extract_value(r["values"].get("reference_3")),
        "stage": extract_value(r["values"].get("stage"))
    } for r in raw_records])

    df_ent = pd.DataFrame([{
        "record_id": e["parent_record_id"],
        "status": extract_value(e["entry_values"].get("status")),
        "status_date": e["entry_values"].get("status", [{}])[0].get("active_from"),
        "red_flags": extract_multi_values(e["entry_values"].get("red_flags_form_7"))
    } for e in raw_entries])

    df = pd.merge(df_rec, df_ent, on="record_id")
    
    # 3. Cálculos
    submitted = len(df)
    total = submitted + in_progress
    day_num = get_day_number()
    
    # Novedades últimas 24h
    one_day_ago = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    new_qualified = df[
        (df['status'].isin(['Initial screening', 'First interaction', 'Deep dive'])) & 
        (df['status_date'] >= one_day_ago)
    ]

    # 4. Construir Mensaje Visual (Idéntico al original)
    W = 50
    msg = []
    msg.append("\n" + "═" * W)
    msg.append(f"Open Call – Day {day_num} Update")
    msg.append("═" * W + "\n")

    msg.append(f"Applications submitted: {submitted}")
    msg.append(f"Applications in progress: {in_progress}")
    msg.append(f"Total applications (submitted + in progress): {total}\n")

    msg.append("─" * W)
    msg.append("Submitted Applications – Source Breakdown")
    msg.append("─" * W)
    sources = df['reference_3'].fillna('Other').value_counts()
    for src, count in sources.items():
        pct = (count / submitted * 100) if submitted > 0 else 0
        msg.append(f"  {src}: {count} ({pct:.1f}%)")

    msg.append("\n" + "─" * W)
    st_counts = df['status'].value_counts()
    for st in ['Not qualified', 'Contacted', 'Stand by', 'Initial screening', 'First interaction', 'Deep dive', 'Pre-committee']:
        msg.append(f"{st}: {st_counts.get(st, 0)}")

    msg.append("\n" + "─" * W)
    msg.append("New Initial screening / First interaction / Deep dive (last 24 h)")
    msg.append("─" * W)
    if not new_qualified.empty:
        for _, row in new_qualified.iterrows():
            msg.append(f"  {row['name']} → {row['status']}")
    else:
        msg.append("  (none)")

    msg.append("\n" + "─" * W)
    msg.append("Main Red Flags")
    msg.append("─" * W)
    all_flags = df[df['status'] == 'Not qualified']['red_flags'].explode()
    if not all_flags.empty:
        top_flags = all_flags.value_counts()
        nq_total = len(df[df['status'] == 'Not qualified'])
        for flag, count in top_flags.items():
            pct = (count / nq_total * 100)
            msg.append(f"  {flag} ({pct:.1f}%)")

    msg.append("\n" + "═" * W)
    
    final_text = "\n".join(msg)
    print(final_text)
    
    if SLACK_WEBHOOK_URL:
        httpx.post(SLACK_WEBHOOK_URL, json={"text": f"```\n{final_text}\n```"})

if __name__ == "__main__":
    asyncio.run(send_daily_metrics())
