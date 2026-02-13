import pandas as pd, re
from pathlib import Path
from html import escape

# ====== BASE = script folder ======
BASE = Path(__file__).resolve().parent
IN_DIR, OUT_DIR = BASE / "in", BASE / "out"

# ====== ORDER LOG ======
ORDER_LOG = Path(r"G:\Automation Google Drive\Order Exports\Completed Orders\AMS\Completed Orders With Profit.csv")

# ====== COLUMN NAMES ======
OID, ODATE, OUPC = "Order ID", "Date Time Ordered", "Combined ISBN X Quantity + Add Ons"
OTHANK = "Thank You Confirmation"
UPC, QTY = "Barcode", "Quantity"

# ====== OUTPUTS ======
latest_csv   = OUT_DIR / "overstock_allocations.csv"
history_csv  = OUT_DIR / "overstock_allocations_history.csv"
email_txt    = OUT_DIR / "overstock_return_email.txt"
credit_txt   = OUT_DIR / "overstock_credit_format.txt"
credit_html  = OUT_DIR / "overstock_credit_format.html"
unfilled_csv = OUT_DIR / "overstock_unfilled.csv"

# ====== EXCLUSIONS ======
EXCLUDE_ORDERS = {"[108934000000000]", "[108935000000000]"}  # bracketed order IDs

# ====== HELPERS ======
D = lambda s: re.sub(r"\D", "", str(s or ""))  # digits only

def R(p: Path) -> pd.DataFrame:
    return (pd.read_excel(p, dtype=str) if p.suffix.lower() in (".xlsx", ".xls")
            else pd.read_csv(p, dtype=str, encoding="utf-8-sig")).fillna("")

def BR(x) -> str:
    """Bracket Order ID, idempotent (won't turn [X] into [[X]])."""
    s = str(x or "").strip()
    if not s:
        return ""
    s = s.strip("[]")                 # critical fix
    s = s.split(".")[0].strip()       # remove trailing .0
    return f"[{s}]"

# ====== DISCOVER INPUT (newest Overstock file) ======
IN_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)

ov = sorted(
    [*IN_DIR.glob("*Overstock*.csv"), *IN_DIR.glob("*Overstock*.xlsx"), *IN_DIR.glob("*Overstock*.xls")],
    key=lambda p: p.stat().st_mtime,
    reverse=True,
)
if not ov:
    raise FileNotFoundError(f"No 'Overstock' file found in: {IN_DIR}")

OV_FILE = ov[0]

# ====== LOAD OVERSTOCK REQUESTS (UPC + total qty) ======
odf = R(OV_FILE)
req = (
    odf.assign(
        upc=odf.get(UPC, "").map(D),
        qty=pd.to_numeric(odf.get(QTY, ""), errors="coerce").fillna(0).astype(int),
    )
    .query("upc!='' and qty>0")[["upc", "qty"]]
    .groupby("upc", as_index=False)["qty"]
    .sum()
)

# ====== LOAD ORDER LOG -> POOL (UPC, ORDER, DATE, AVAILABLE QTY) ======
df = R(ORDER_LOG)
order_id_series = df[OID] if OID in df.columns else pd.Series("", index=df.index)
thank_you_series = df[OTHANK] if OTHANK in df.columns else pd.Series("", index=df.index)
df[OID] = thank_you_series.where(thank_you_series.astype(str).str.strip().ne(""), order_id_series).map(BR)

dtp = pd.to_datetime(df.get(ODATE, ""), errors="coerce")  # keep local-like, don't force UTC
df["_order_dt"] = dtp.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

rows = []
for oid, odt, cell in df[[OID, "_order_dt", OUPC]].itertuples(index=False):
    if not oid or not cell:
        continue
    for ln in str(cell).replace("\r", "\n").split("\n"):
        p = [x.strip().strip('"') for x in ln.split(",")]
        if len(p) < 2:
            continue
        up = D(p[0])
        try:
            q = int(float(p[1]))
        except Exception:
            continue
        if up and q > 0:
            rows.append((up, oid, odt, q))

pool = pd.DataFrame(rows, columns=["upc", "order_number", "datetime_ordered", "avail_qty"])
pool = pool[pool["upc"].isin(req["upc"])].copy()
pool = pool[~pool["order_number"].isin(EXCLUDE_ORDERS)].copy()
pool["avail_qty"] = pd.to_numeric(pool["avail_qty"], errors="coerce").fillna(0).astype(int)

# ====== APPLY HISTORY CONSUMPTION (prevents reusing exhausted order inventory) ======
if history_csv.exists():
    h = pd.read_csv(history_csv, dtype=str, encoding="utf-8-sig").fillna("")
    if set(["Order Number", "UPC", "Qty"]).issubset(h.columns):
        # Normalize to match pool keys
        h["Order Number"] = h["Order Number"].map(BR)  # now idempotent
        h = h[~h["Order Number"].isin(EXCLUDE_ORDERS)].copy()
        h["UPC"] = h["UPC"].map(D)
        h["Qty"] = pd.to_numeric(h["Qty"], errors="coerce").fillna(0).astype(int)

        used = (
            h.groupby(["Order Number", "UPC"], as_index=False)["Qty"]
            .sum()
            .rename(columns={"Order Number": "order_number", "UPC": "upc", "Qty": "used_qty"})
        )

        pool = pool.merge(used, on=["order_number", "upc"], how="left")
        pool["used_qty"] = pool["used_qty"].fillna(0).astype(int)
        pool["avail_qty"] = (pool["avail_qty"] - pool["used_qty"]).clip(lower=0)
        pool = pool[pool["avail_qty"] > 0].drop(columns=["used_qty"])

# FIFO by Date Time Ordered
pool = pool.sort_values(["upc", "datetime_ordered", "order_number"]).reset_index(drop=True)

# ====== ALLOCATE (FIFO, history-aware) ======
alloc = []
unfilled = []

for upc, need in req.itertuples(index=False):
    sub = pool[pool["upc"] == upc]
    for i in sub.index:
        if need <= 0:
            break
        take = min(need, int(pool.at[i, "avail_qty"]))
        if take > 0:
            alloc.append((pool.at[i, "order_number"], pool.at[i, "datetime_ordered"], upc, take))
            pool.at[i, "avail_qty"] -= take
            need -= take
    if need > 0:
        unfilled.append((upc, need))

# ====== BUILD OUTPUT DF ======
out = pd.DataFrame(alloc, columns=["Order Number", "Date Time Ordered", "UPC", "Qty"])
out["log_added_at"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

# ====== WRITE LATEST (overwrite) ======
out.drop(columns=["log_added_at"]).to_csv(latest_csv, index=False, encoding="utf-8-sig")
pd.DataFrame(unfilled, columns=["UPC", "Qty Unfilled"]).to_csv(unfilled_csv, index=False, encoding="utf-8-sig")

# ====== APPEND HISTORY (keeps everything) ======
if history_csv.exists():
    old = pd.read_csv(history_csv, dtype=str, encoding="utf-8-sig").fillna("")
    new = pd.concat([old, out.astype(str)], ignore_index=True)
else:
    new = out.astype(str)
new.to_csv(history_csv, index=False, encoding="utf-8-sig")

# ====== WHOLESALER EXPORT TEXT (no brackets, only 3 columns) ======
wholesaler_df = out[["Order Number", "UPC", "Qty"]].copy()
wholesaler_df["Order Number"] = wholesaler_df["Order Number"].astype(str).str.replace(r"^\[|\]$", "", regex=True)
wholesaler_df["UPC"] = wholesaler_df["UPC"].map(D)
wholesaler_df["Qty"] = pd.to_numeric(wholesaler_df["Qty"], errors="coerce").fillna(0).astype(int)

rows = (
    wholesaler_df.groupby(["Order Number", "UPC"], as_index=False)["Qty"]
    .sum()
    .sort_values(["Order Number", "UPC"])
)

credit_lines = ["**Orders #**\t**UPC**\t**QTY**"]
for r in rows.itertuples(index=False):
    credit_lines.append(f"{r[0]}\t{r[1]}\t{int(r[2]):02d}")
credit_txt.write_text("\n".join(credit_lines).strip() + "\n", encoding="utf-8")

# Rich-text table for clean email paste
html_rows = []
for r in rows.itertuples(index=False):
    html_rows.append(
        f"<tr><td>{escape(str(r[0]))}</td><td>{escape(str(r[1]))}</td><td>{int(r[2]):02d}</td></tr>"
    )
credit_html.write_text(
    (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<style>"
        "body{font-family:Calibri,Arial,sans-serif;font-size:11pt;color:#111;}"
        "table{border-collapse:collapse;margin:0;}"
        "th,td{border:1px solid #d9d9d9;padding:6px 10px;text-align:left;}"
        "th{font-weight:700;background:#f7f7f7;}"
        "</style></head><body>"
        "<table><thead><tr><th>Orders #</th><th>UPC</th><th>QTY</th></tr></thead>"
        f"<tbody>{''.join(html_rows)}</tbody></table>"
        "</body></html>"
    ),
    encoding="utf-8",
)

lines = [
    "Hi AMS Team,",
    "",
    "I would like to request an overstock return for the following items.",
    "Please advise next steps and confirm the return authorization / instructions.",
    "",
    "Orders #\tUPC\tQTY",
]
for r in rows.itertuples(index=False):
    lines.append(f"{r[0]}\t{r[1]}\t{int(r[2]):02d}")
lines += ["", "Thank you,", "Daniel"]

email_txt.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")

print("Overstock input:", OV_FILE)
print("Excluded orders:", ", ".join(sorted(EXCLUDE_ORDERS)) if EXCLUDE_ORDERS else "(none)")
print("Saved (overwrite):", latest_csv)
print("Saved (overwrite):", email_txt)
print("Saved (overwrite):", credit_txt)
print("Saved (overwrite):", credit_html)
print("Saved (overwrite):", unfilled_csv)
print("Saved (append history):", history_csv)
