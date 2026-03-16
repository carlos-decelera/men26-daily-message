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

    # 2. Procesamiento de DataFrames
    df_rec = pd.DataFrame([{
        "record_id": r["id"]["record_id"],
        "reference": str(extract_value(r["values"].get("reference_3")) or 'Other').strip()
    } for r in raw_records])

    df_ent = pd.DataFrame([{
        "record_id": e["parent_record_id"],
        "status": str(extract_value(e["entry_values"].get("status")) or 'Unknown').strip(),
        "reasons": [str(f).strip() for f in extract_multi_values(e["entry_values"].get("red_flags_form_7"))]
    } for e in raw_entries])

    df = pd.merge(df_rec, df_ent, on="record_id")
    total_submitted = len(df)
    total_combined = total_submitted + in_progress

    # --- CONSTRUCCIÓN DEL MENSAJE ---
    W = 45
    msg = []
    msg.append(f"*📊 Reporte Open Call - {datetime.now().strftime('%d/%m/%Y')}*")
    msg.append("=" * W)

    # Bloque 1: Totales
    msg.append(f"✅ *Applications submitted:* {total_submitted}")
    msg.append(f"⏳ *Applications in progress:* {in_progress}")
    msg.append(f"🔥 *Total interest:* {total_combined}")
    msg.append("-" * W)

    # Bloque 2: Source Breakdown (Reference 3)
    msg.append("*📍 Sources (Reference 3)*")
    if total_submitted > 0:
        sources = df['reference'].value_counts()
        for src, count in sources.items():
            pct = (count / total_submitted) * 100
            msg.append(f"• {src}: {count} ({pct:.1f}%)")
    else:
        msg.append("  (No data)")
    msg.append("-" * W)

    # Bloque 3: Status Breakdown
    msg.append("*📈 Pipeline Status*")
    if total_submitted > 0:
        status_counts = df['status'].value_counts()
        for st, count in status_counts.items():
            pct = (count / total_submitted) * 100
            msg.append(f"• {st}: {count} ({pct:.1f}%)")
    else:
        msg.append("  (No data)")
    msg.append("-" * W)

    # Bloque 4: Reasons / Red Flags Breakdown (basado en 'Not qualified')
    msg.append("*🚩 Disqualification Reasons*")
    df_nq = df[df['status'] == 'Not qualified']
    if not df_nq.empty:
        all_reasons = df_nq['reasons'].explode().dropna()
        all_reasons = all_reasons[all_reasons != ""]
        
        if not all_reasons.empty:
            reason_counts = all_reasons.value_counts()
            total_reasons = len(df_nq) # Porcentaje sobre el total de descalificados
            for reason, count in reason_counts.items():
                pct = (count / total_reasons) * 100
                msg.append(f"• {reason}: {count} ({pct:.1f}%)")
        else:
            msg.append("  (No reasons specified)")
    else:
        msg.append("  (No applications disqualified yet)")

    msg.append("=" * W)

    # 5. Envío a Slack
    final_text = "\n".join(msg)
    print(final_text) # Para debug en consola
    
    if SLACK_WEBHOOK_URL:
        # Enviamos como bloque de texto Markdown de Slack
        async with httpx.AsyncClient() as client:
            await client.post(SLACK_WEBHOOK_URL, json={"text": final_text})

if __name__ == "__main__":
    asyncio.run(send_daily_metrics())
