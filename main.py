# -*- coding: utf-8 -*-
# 符合你今日设计的“正式版”主程序：多币种独立、QFL 触发、DCA、全局移动止盈、日志、日报、UI 端口。
import os, sys, time, json, threading, math, traceback, pytz, datetime as dt
from typing import Dict, Any, List
try:
    import sitecustomize  # LOT_SIZE 对齐补丁
except Exception:
    pass

import yaml
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from binance.client import Client
from binance.exceptions import BinanceAPIException
import requests

import qfl_signal

APP_DIR = os.path.abspath(os.path.dirname(__file__))
LOG_DIR = os.path.join(APP_DIR, "logs")
DATA_DIR = os.path.join(APP_DIR, "data")
REPORT_DIR = os.path.join(APP_DIR, "reports")
for _d in (LOG_DIR, DATA_DIR, REPORT_DIR):
    os.makedirs(_d, exist_ok=True)

LOG_PATH = os.path.join(LOG_DIR, "qfl_bot.log")

def log(msg: str):
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line+"\n")
    except Exception:
        pass

# 载入配置
CONFIG_PATH = os.path.join(APP_DIR, "config.yaml")
conf = yaml.safe_load(open(CONFIG_PATH, "r", encoding="utf-8"))

TZ = pytz.timezone(conf["general"]["timezone"])
REAL_TRADING = int(os.environ.get(conf["general"]["real_trading_env_var"], os.environ.get("REAL_TRADING", "0")))
KEY_FILE = os.environ.get(conf["exchange"]["key_file_env"], os.environ.get("API_KEY_FILE"))
if not KEY_FILE:
    KEY_FILE = os.path.join(APP_DIR, "secrets", "binance_main.key")

API_KEY = ""
API_SECRET = ""

if os.path.exists(KEY_FILE):
    try:
        # 简单解析 KEY 文件：每行 KEY=VAL
        kv = {}
        for line in open(KEY_FILE, "r", encoding="utf-8").read().splitlines():
            line = line.strip()
            if not line or line.startswith("#"): continue
            if "=" in line:
                k, v = line.split("=", 1)
                kv[k.strip()] = v.strip()
        API_KEY = kv.get("API_KEY", "")
        API_SECRET = kv.get("API_SECRET", "")
    except Exception as e:
        log(f"[WARN] parse key file error: {e}")
else:
    log(f"[WARN] key file not found: {KEY_FILE}")

client = Client(API_KEY, API_SECRET) if REAL_TRADING else Client(None, None)
log(f"[INFO] Binance client initialized (REAL_TRADING={REAL_TRADING})")

SYMBOLS: List[str] = list(conf["symbols"])
BASE_USDT = float(conf["entry"]["first_order_usdt"])
DCA_LEVELS = conf["dca"]["levels"]
COOLDOWN = int(conf["general"]["cooldown_sec"])

TTP_MIN_GAIN = float(conf["take_profit"]["min_gain_pct"])
TTP_OFFSET = float(conf["take_profit"]["offset_pct"])

# 状态：每个 symbol 独立
state: Dict[str, Dict[str, Any]] = {}
for s in SYMBOLS:
    state[s] = dict(
        has_position=False,
        qty=0.0,
        avg=0.0,
        entry_price=None,
        peak=None,                    # 激活后最高价
        ttp_active=False,
        dca_done=set(),               # 已完成的 DCA 阶梯（记录 drop_pct）
        last_action_ts=0.0,
    )

def get_symbol_price(sym: str) -> float:
    # 用 binance REST ticker 价格
    data = client.get_symbol_ticker(symbol=sym)
    return float(data["price"])

def get_klines_close(sym: str, interval="1m", limit=200) -> List[float]:
    kl = client.get_klines(symbol=sym, interval=interval, limit=limit)
    return [float(x[4]) for x in kl]  # 收盘价

def usdt_to_qty(usdt: float, price: float) -> float:
    if price <= 0: return 0.0
    return usdt / price

def market_buy(sym: str, qty: float) -> float:
    if qty <= 0: return 0.0
    if REAL_TRADING:
        try:
            o = client.order_market_buy(symbol=sym, quantity=qty)
            fills = o.get("fills", [])
            if fills:
                cost = sum(float(f["price"]) * float(f["qty"]) for f in fills)
                qty_filled = sum(float(f["qty"]) for f in fills)
                avg = cost / qty_filled if qty_filled > 0 else get_symbol_price(sym)
                return avg
            else:
                return get_symbol_price(sym)
        except BinanceAPIException as e:
            log(f"[ERROR] {sym} BUY failed: {e.message}")
            return 0.0
    else:
        # 仿真：用当前价格当均价
        return get_symbol_price(sym)

def market_sell(sym: str, qty: float) -> float:
    if qty <= 0: return 0.0
    if REAL_TRADING:
        try:
            o = client.order_market_sell(symbol=sym, quantity=qty)
            # 简化：直接用当前价作为成交均价
            return get_symbol_price(sym)
        except BinanceAPIException as e:
            log(f"[ERROR] {sym} SELL failed: {e.message}")
            return 0.0
    else:
        return get_symbol_price(sym)

def recalc_avg(old_avg: float, old_qty: float, add_avg: float, add_qty: float) -> float:
    if old_qty + add_qty <= 0: return 0.0
    return (old_avg*old_qty + add_avg*add_qty) / (old_qty + add_qty)

def worker_symbol(sym: str):
    log(f"[INFO] [{sym}] worker started. base={BASE_USDT} DCA={DCA_LEVELS} TTP(offset={TTP_OFFSET}%, min_gain={TTP_MIN_GAIN}%)")
    while True:
        try:
            st = state[sym]
            now = time.time()
            if now - st["last_action_ts"] < COOLDOWN:
                time.sleep(1)
                continue

            price = get_symbol_price(sym)

            # 拉取 1m K 线收盘价进行 QFL 判定
            closes = get_klines_close(sym, "1m", limit=max(conf["qfl"]["lookback_bars"], 60))
            qfl_ok, qfl_detail = qfl_signal.qfl_entry_signal(
                closes,
                lookback_bars=conf["qfl"]["lookback_bars"],
                drop_trigger_pct=conf["qfl"]["drop_trigger_pct"],
                confirm_recovery_pct=conf["qfl"]["confirm_recovery_pct"],
                logger=None  # 日志靠外层打印
            )

            if not st["has_position"]:
                if qfl_ok:
                    qty = usdt_to_qty(BASE_USDT, price)
                    avg = market_buy(sym, qty)
                    if avg > 0:
                        st["has_position"] = True
                        st["qty"] = qty
                        st["avg"] = avg
                        st["entry_price"] = avg
                        st["peak"] = None
                        st["ttp_active"] = False
                        st["dca_done"] = set()
                        log(f"[{sym}] ENTRY -> avg={avg:.6f} qty={qty:.6f}")
                        st["last_action_ts"] = time.time()
                else:
                    # 不满足 QFL，不开单
                    pass
            else:
                # 已持仓：检查 DCA / TTP
                drop_from_entry_pct = (st["avg"] - price) / st["avg"] * 100.0
                # DCA：仅当未启用 TTP 或尚未触发回撤清仓
                for lv in DCA_LEVELS:
                    dp = float(lv["drop_pct"])
                    amt = float(lv["amount"])
                    if dp in st["dca_done"]:
                        continue
                    if drop_from_entry_pct >= dp:
                        # 执行一次 DCA 市价单
                        add_qty = usdt_to_qty(amt, price)
                        add_avg = market_buy(sym, add_qty)
                        if add_avg > 0:
                            new_avg = recalc_avg(st["avg"], st["qty"], add_avg, add_qty)
                            st["qty"] += add_qty
                            st["avg"] = new_avg
                            st["dca_done"].add(dp)
                            log(f"[{sym}] DCA {dp}% -> avg={new_avg:.6f} qty={st['qty']:.6f}")
                            st["last_action_ts"] = time.time()

                # Trailing Take Profit：全局开启
                gain_pct = (price - st["avg"]) / st["avg"] * 100.0
                if gain_pct >= TTP_MIN_GAIN:
                    # 激活追踪并记录峰值
                    if st["peak"] is None or price > st["peak"]:
                        st["peak"] = price
                    # 从峰值回撤
                    pullback_pct = (st["peak"] - price) / st["peak"] * 100.0 if st["peak"] else 0.0
                    if pullback_pct >= TTP_OFFSET:
                        # 市价清仓
                        sell_avg = market_sell(sym, st["qty"])
                        if sell_avg > 0:
                            log(f"[{sym}] TTP SELL ALL at ~{sell_avg:.6f} (gain≈{gain_pct:.3f}% , pullback≈{pullback_pct:.3f}%)")
                            st["has_position"] = False
                            st["qty"] = 0.0
                            st["avg"] = 0.0
                            st["entry_price"] = None
                            st["peak"] = None
                            st["ttp_active"] = False
                            st["dca_done"] = set()
                            st["last_action_ts"] = time.time()

            time.sleep(1)

        except Exception as e:
            log(f"[ERROR] worker {sym}: {e}\n{traceback.format_exc()}")
            time.sleep(2)

# -------- HTTP 控制端 --------
app = FastAPI()

@app.get("/health")
def http_health():
    return {"ok": True, "real_trading": REAL_TRADING, "ts": dt.datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")}

@app.get("/positions")
def http_positions():
    items = []
    for sym, st in state.items():
        items.append({
            "symbol": sym,
            "qty": round(st["qty"], 8),
            "avg_price": round(st["avg"], 8),
            "entry_price": st["entry_price"],
            "trailing": {"min_gain_pct": TTP_MIN_GAIN, "offset_pct": TTP_OFFSET},
            "base_fills": [],
            "dca_fills": [],
        })
    return {"ok": True, "items": items}

@app.get("/balances")
def http_balances():
    try:
        if REAL_TRADING:
            acc = client.get_account()
            bals = {b["asset"]: float(b["free"])+float(b["locked"]) for b in acc["balances"]}
            return {"ok": True, "balances": bals}
        else:
            return {"ok": True, "balances": {}}
    except Exception as e:
        return {"ok": False, "reason": str(e)}

# 简单计算器：返回当前 QFL 触发的理论价格区间
@app.get("/calc")
def http_calc(symbol: str, lookback_bars: int=50, drop_trigger_pct: float=2.5, confirm_recovery_pct: float=1.0):
    try:
        closes = get_klines_close(symbol, "1m", limit=max(lookback_bars, 60))
        ok, detail = qfl_signal.qfl_entry_signal(closes, lookback_bars, drop_trigger_pct, confirm_recovery_pct, logger=None)
        return {"ok": True, "signal": ok, "detail": detail}
    except Exception as e:
        return {"ok": False, "reason": str(e)}

def daily_report_job():
    try:
        ts = dt.datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
        path = os.path.join(REPORT_DIR, f"daily_{ts}.txt")
        lines = []
        lines.append(f"Daily report @ {dt.datetime.now(TZ):%Y-%m-%d %H:%M:%S} {conf['report']['base_currency']}")
        if REAL_TRADING:
            acc = client.get_account()
            total_usdt = 0.0
            for b in acc["balances"]:
                asset = b["asset"]
                free = float(b["free"]); locked = float(b["locked"])
                amt = free + locked
                if amt == 0: continue
                if asset == "USDT":
                    total_usdt += amt
                else:
                    sym = asset + "USDT"
                    try:
                        px = get_symbol_price(sym)
                        total_usdt += amt * px
                    except Exception:
                        pass
                lines.append(f"{asset}: {amt}")
            lines.append(f"Total ≈ {total_usdt:.2f} USDT")
        else:
            lines.append("Paper trading mode, no balances.")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        log(f"[REPORT] generated -> {path}")
    except Exception as e:
        log(f"[ERROR] daily report: {e}")

def schedule_report():
    if not conf["report"]["enabled"]:
        return
    hh, mm = map(int, conf["report"]["time_of_day"].split(":"))
    scheduler = BackgroundScheduler(timezone=TZ)
    scheduler.add_job(daily_report_job, CronTrigger(hour=hh, minute=mm))
    scheduler.start()
    log(f"[INFO] daily report scheduled at {conf['report']['time_of_day']}")

def start_workers():
    for sym in SYMBOLS:
        th = threading.Thread(target=worker_symbol, args=(sym,), daemon=True)
        th.start()

if __name__ == "__main__":
    schedule_report()
    start_workers()
    uvicorn.run(app, host="0.0.0.0", port=8765)
