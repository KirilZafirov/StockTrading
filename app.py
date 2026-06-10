import json
import math
import os
import random
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


APP_DIR = Path(__file__).parent
HOST = "127.0.0.1"
PORT = 5003
STATE_FILE = APP_DIR / "paper_state.json"

SYMBOL = "NAS100USD"
CANDLE_SECONDS = 15 * 60
DECISION_SECONDS = 180
MAX_COUNTER_WICK_POINTS = 20.0
FIRST_TRAILING_POINTS = 30.0
SECOND_TRAILING_POINTS = 50.0
STOP_LOSS_POINTS = 20.0
PAPER_START_BALANCE = 20000.0
PAPER_LOTS = 5
PAPER_DOLLARS_PER_POINT_PER_LOT = 1.0
DATA_PROVIDER = os.getenv("DATA_PROVIDER", "yahoo").strip().lower()
YAHOO_SYMBOL = os.getenv("YAHOO_SYMBOL", "NQ=F").strip()
YAHOO_POLL_SECONDS = 5

OANDA_ENV = os.getenv("OANDA_ENV", "practice").strip().lower()
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "").strip()
OANDA_API_TOKEN = os.getenv("OANDA_API_TOKEN", "").strip()
OANDA_REST_URLS = {
    "practice": "https://api-fxpractice.oanda.com",
    "live": "https://api-fxtrade.oanda.com",
}


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat(timespec="seconds")


def candle_start_epoch(ts=None):
    value = int(ts or time.time())
    return value - (value % CANDLE_SECONDS)


def format_clock(epoch):
    return datetime.fromtimestamp(epoch).strftime("%H:%M:%S")


def oanda_base_url():
    return OANDA_REST_URLS.get(OANDA_ENV, OANDA_REST_URLS["practice"])


def mask_account_id(account_id):
    if not account_id:
        return ""
    if len(account_id) <= 8:
        return "****"
    return f"{account_id[:4]}...{account_id[-4:]}"


def oanda_config_status():
    return {
        "environment": OANDA_ENV if OANDA_ENV in OANDA_REST_URLS else "practice",
        "base_url": oanda_base_url(),
        "has_token": bool(OANDA_API_TOKEN),
        "has_account_id": bool(OANDA_ACCOUNT_ID),
        "account_id": mask_account_id(OANDA_ACCOUNT_ID),
        "ready": bool(OANDA_API_TOKEN and OANDA_ACCOUNT_ID),
        "trading_enabled": False,
        "note": "Account-specific OANDA data requires your practice API token and account ID.",
    }


def oanda_request(path):
    if not OANDA_API_TOKEN:
        return {
            "ok": False,
            "status": 428,
            "error": "OANDA_API_TOKEN is not configured.",
        }

    request = Request(
        f"{oanda_base_url()}{path}",
        headers={
            "Authorization": f"Bearer {OANDA_API_TOKEN}",
            "Content-Type": "application/json",
            "Accept-Datetime-Format": "RFC3339",
        },
    )

    try:
        with urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
            return {
                "ok": True,
                "status": response.status,
                "data": json.loads(body) if body else {},
            }
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = {"message": body}
        return {"ok": False, "status": exc.code, "error": parsed}
    except URLError as exc:
        return {"ok": False, "status": 503, "error": str(exc.reason)}
    except TimeoutError:
        return {"ok": False, "status": 504, "error": "OANDA request timed out."}


class YahooPriceProvider:
    def __init__(self, symbol):
        self.symbol = symbol
        self.last_price = None
        self.last_fetch = 0.0
        self.last_quote_time = None
        self.last_error = ""
        self.status = "waiting"

    def fetch_price(self):
        encoded_symbol = self.symbol.replace("=", "%3D")
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded_symbol}?range=1d&interval=1m"
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
        )

        with urlopen(request, timeout=6) as response:
            payload = json.loads(response.read().decode("utf-8"))

        result = payload.get("chart", {}).get("result", [])
        if not result:
            raise ValueError("Yahoo returned no chart result")

        chart = result[0]
        closes = chart.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        timestamps = chart.get("timestamp", [])
        valid_points = [(ts, close) for ts, close in zip(timestamps, closes) if close is not None]
        if not valid_points:
            raise ValueError("Yahoo returned no valid close price")

        quote_ts, price = valid_points[-1]
        self.last_quote_time = datetime.fromtimestamp(quote_ts).strftime("%H:%M:%S")
        self.last_price = float(price)
        self.last_fetch = time.time()
        self.last_error = ""
        self.status = "live"
        return round(self.last_price, 2)

    def get_price(self):
        if time.time() - self.last_fetch < YAHOO_POLL_SECONDS and self.last_price is not None:
            return round(self.last_price, 2)

        try:
            return self.fetch_price()
        except Exception as exc:
            self.last_error = str(exc)[:160]
            self.status = "fallback" if self.last_price is None else "stale"
            return None

    def snapshot(self):
        return {
            "name": "Yahoo Finance chart",
            "symbol": self.symbol,
            "status": self.status,
            "last_price": round(self.last_price, 2) if self.last_price is not None else None,
            "last_fetch": datetime.fromtimestamp(self.last_fetch).strftime("%H:%M:%S") if self.last_fetch else None,
            "last_quote_time": self.last_quote_time,
            "last_error": self.last_error,
            "poll_seconds": YAHOO_POLL_SECONDS,
            "note": "Read-only market data for paper testing; not broker execution pricing.",
        }


class Nas100PaperEngine:
    def __init__(self):
        self.lock = threading.Lock()
        self.provider = YahooPriceProvider(YAHOO_SYMBOL) if DATA_PROVIDER == "yahoo" else None
        self.pending_feed_reset = False
        self.price = 19150.0 + random.uniform(-70, 70)
        self.candle = None
        self.trades = []
        self.transactions = []
        self.open_trade = None
        self.decision_log = []
        self.feed_mode = "waiting-real" if self.provider else "simulated"
        self.start_balance = PAPER_START_BALANCE
        self.balance = PAPER_START_BALANCE
        self.realized_pnl = 0.0
        self.reset_candle(candle_start_epoch(), self.price)
        self.load_state()

    def export_state(self):
        return {
            "version": 1,
            "saved_at": iso_now(),
            "data_provider": DATA_PROVIDER,
            "provider_symbol": YAHOO_SYMBOL if self.provider else "simulated",
            "start_balance": self.start_balance,
            "balance": self.balance,
            "realized_pnl": self.realized_pnl,
            "trades": self.trades,
            "transactions": self.transactions,
            "decisions": self.decision_log,
            "open_trade": self.open_trade,
        }

    def load_state(self):
        if not STATE_FILE.exists():
            return

        try:
            with STATE_FILE.open("r", encoding="utf-8") as state_file:
                state = json.load(state_file)
        except Exception as exc:
            print(f"[STATE] Could not load {STATE_FILE.name}: {exc}")
            return

        self.start_balance = float(state.get("start_balance", PAPER_START_BALANCE))
        if state.get("data_provider") != DATA_PROVIDER:
            print(f"[STATE] Ignoring saved state from a different data provider")
            return
        if self.provider and state.get("provider_symbol") != YAHOO_SYMBOL:
            print(f"[STATE] Ignoring saved state without matching provider symbol")
            return

        self.balance = float(state.get("balance", PAPER_START_BALANCE))
        self.realized_pnl = float(state.get("realized_pnl", 0.0))
        self.trades = list(state.get("trades", []))[:50]
        self.transactions = list(state.get("transactions", []))[:200]
        self.decision_log = list(state.get("decisions", []))[:30]
        self.open_trade = state.get("open_trade")
        print(f"[STATE] Loaded paper state from {STATE_FILE.name}")

    def save_state(self):
        temp_file = STATE_FILE.with_suffix(".tmp")
        try:
            with temp_file.open("w", encoding="utf-8") as state_file:
                json.dump(self.export_state(), state_file, indent=2)
            temp_file.replace(STATE_FILE)
        except Exception as exc:
            print(f"[STATE] Could not save {STATE_FILE.name}: {exc}")

    def reset_candle(self, start_epoch, open_price):
        self.candle = {
            "symbol": SYMBOL,
            "start_epoch": start_epoch,
            "start": format_clock(start_epoch),
            "decision_epoch": start_epoch + DECISION_SECONDS,
            "decision_time": format_clock(start_epoch + DECISION_SECONDS),
            "end_epoch": start_epoch + CANDLE_SECONDS,
            "open": round(open_price, 2),
            "high": round(open_price, 2),
            "low": round(open_price, 2),
            "close": round(open_price, 2),
            "decision_checked": False,
            "decision": "WAIT",
            "reason": "Waiting for the 180-second decision point.",
            "counter_wick": 0.0,
            "elapsed_seconds": max(0, int(time.time() - start_epoch)),
        }

    def next_price(self):
        if self.provider:
            provider_price = self.provider.get_price()
            if provider_price is not None:
                if self.feed_mode != "real-yahoo" and abs(provider_price - self.price) > 1000:
                    self.pending_feed_reset = True
                self.price = provider_price
                self.feed_mode = "real-yahoo"
                return round(self.price, 2)

        drift = random.uniform(-7.5, 7.5)
        pulse = math.sin(time.time() / 14.0) * random.uniform(0.0, 3.2)
        self.price = max(1000.0, self.price + drift + pulse)
        self.feed_mode = "simulated-fallback" if self.provider else "simulated"
        return round(self.price, 2)

    def mark_to_market(self):
        if not self.open_trade:
            return
        direction = self.open_trade["side"]
        entry = self.open_trade["entry"]
        pnl_points = self.price - entry if direction == "BUY" else entry - self.price
        pnl_dollars = pnl_points * self.open_trade["lots"] * PAPER_DOLLARS_PER_POINT_PER_LOT
        self.open_trade["unrealized_points"] = round(pnl_points, 2)
        self.open_trade["unrealized_pnl"] = round(pnl_dollars, 2)
        self.open_trade["last_price"] = round(self.price, 2)
        self.open_trade["state"] = "positive" if pnl_dollars > 0 else "negative" if pnl_dollars < 0 else "flat"
        self.open_trade["best_points"] = round(max(self.open_trade.get("best_points", 0.0), pnl_points), 2)
        if self.open_trade["best_points"] >= SECOND_TRAILING_POINTS:
            self.open_trade["trailing_stage"] = "second"
            self.open_trade["trailing_stop_points"] = SECOND_TRAILING_POINTS
        elif self.open_trade["best_points"] >= FIRST_TRAILING_POINTS:
            self.open_trade["trailing_stage"] = "first"
            self.open_trade["trailing_stop_points"] = FIRST_TRAILING_POINTS
        self.open_trade["trailing_active"] = self.open_trade["trailing_stage"] != "none"

    def record_transaction(self, event_type, trade, price, reason, points=0.0, pnl=0.0):
        self.transactions.insert(0, {
            "id": len(self.transactions) + 1,
            "time": iso_now(),
            "type": event_type,
            "trade_id": trade["id"],
            "symbol": trade["symbol"],
            "side": trade["side"],
            "price": round(price, 2),
            "lots": trade["lots"],
            "points": round(points, 2),
            "pnl": round(pnl, 2),
            "balance": round(self.balance, 2),
            "reason": reason,
        })
        self.transactions = self.transactions[:200]
        self.save_state()

    def enforce_trade_exits(self):
        if not self.open_trade:
            return
        if self.open_trade["unrealized_points"] <= -STOP_LOSS_POINTS:
            self.close_open_trade(self.price, "20 point stop loss reached")
        elif self.open_trade.get("trailing_active") and self.open_trade["unrealized_points"] < self.open_trade["trailing_stop_points"]:
            stage = self.open_trade["trailing_stage"]
            reason = "first 30 point trailing protection reached" if stage == "first" else "second 50 point trailing protection reached"
            self.close_open_trade(self.price, reason)

    def close_open_trade(self, exit_price, reason):
        if not self.open_trade:
            return

        trade = self.open_trade
        direction = trade["side"]
        pnl_points = exit_price - trade["entry"] if direction == "BUY" else trade["entry"] - exit_price
        pnl_dollars = pnl_points * trade["lots"] * PAPER_DOLLARS_PER_POINT_PER_LOT
        self.balance += pnl_dollars
        self.realized_pnl += pnl_dollars

        trade["status"] = "CLOSED"
        trade["closed_at"] = iso_now()
        trade["exit"] = round(exit_price, 2)
        trade["close_reason"] = reason
        trade["realized_points"] = round(pnl_points, 2)
        trade["realized_pnl"] = round(pnl_dollars, 2)
        trade["unrealized_points"] = 0.0
        trade["unrealized_pnl"] = 0.0
        trade["last_price"] = round(exit_price, 2)
        trade["outcome"] = "WIN" if pnl_dollars > 0 else "LOSS" if pnl_dollars < 0 else "FLAT"
        trade["state"] = "positive" if pnl_dollars > 0 else "negative" if pnl_dollars < 0 else "flat"
        self.record_transaction("CLOSE", trade, exit_price, reason, pnl_points, pnl_dollars)
        self.open_trade = None

    def evaluate_decision(self, now_epoch):
        if self.candle["decision_checked"] or now_epoch < self.candle["decision_epoch"]:
            return

        open_price = self.candle["open"]
        close_price = self.candle["close"]
        high = self.candle["high"]
        low = self.candle["low"]

        if close_price > open_price:
            side = "BUY"
            counter_wick = max(0.0, open_price - low)
            structure = "bullish"
        elif close_price < open_price:
            side = "SELL"
            counter_wick = max(0.0, high - open_price)
            structure = "bearish"
        else:
            side = "SKIP"
            counter_wick = 0.0
            structure = "flat"

        allowed = side != "SKIP" and counter_wick <= MAX_COUNTER_WICK_POINTS
        reason = (
            f"{structure} candle; counter wick {counter_wick:.2f} points."
            if allowed
            else f"Skipped: {structure} candle; counter wick {counter_wick:.2f} points."
        )

        self.candle["decision_checked"] = True
        self.candle["decision"] = side if allowed else "SKIP"
        self.candle["reason"] = reason
        self.candle["counter_wick"] = round(counter_wick, 2)

        event = {
            "time": iso_now(),
            "candle": self.candle["start"],
            "side": self.candle["decision"],
            "open": open_price,
            "price_at_decision": close_price,
            "high": high,
            "low": low,
            "counter_wick": round(counter_wick, 2),
            "reason": reason,
            "strict_mode_passed": bool(allowed),
        }
        self.decision_log.insert(0, event)
        self.decision_log = self.decision_log[:30]
        self.save_state()

        if allowed:
            self.open_paper_trade(side, close_price, event)

    def open_paper_trade(self, side, entry, source_event):
        if self.open_trade:
            source_event["execution"] = "blocked_existing_position"
            return

        trade = {
            "id": len(self.trades) + 1,
            "symbol": SYMBOL,
            "side": side,
            "entry": round(entry, 2),
            "lots": PAPER_LOTS,
            "dollars_per_point": round(PAPER_LOTS * PAPER_DOLLARS_PER_POINT_PER_LOT, 2),
            "opened_at": iso_now(),
            "source_candle": source_event["candle"],
            "status": "OPEN",
            "last_price": round(entry, 2),
            "unrealized_points": 0.0,
            "unrealized_pnl": 0.0,
            "best_points": 0.0,
            "trailing_active": False,
            "trailing_stage": "none",
            "trailing_stop_points": None,
            "state": "flat",
            "mode": "paper",
        }
        self.open_trade = trade
        self.trades.insert(0, trade)
        self.trades = self.trades[:50]
        self.record_transaction("OPEN", trade, entry, "strict 180s wick rule passed")
        source_event["execution"] = "paper_trade_opened"

    def tick(self):
        with self.lock:
            now_epoch = int(time.time())
            current_start = candle_start_epoch(now_epoch)
            price = self.next_price()

            if self.pending_feed_reset:
                self.close_open_trade(price, "real data feed reset")
                self.reset_candle(current_start, price)
                if now_epoch >= self.candle["decision_epoch"]:
                    self.candle["decision_checked"] = True
                    self.candle["decision"] = "SKIP"
                    self.candle["reason"] = "Skipped: real data feed started after this candle's 180-second checkpoint."
                self.pending_feed_reset = False
            elif current_start != self.candle["start_epoch"]:
                self.close_open_trade(price, "15m candle closed")
                self.reset_candle(current_start, price)

            self.candle["high"] = round(max(self.candle["high"], price), 2)
            self.candle["low"] = round(min(self.candle["low"], price), 2)
            self.candle["close"] = price
            self.candle["elapsed_seconds"] = max(0, now_epoch - self.candle["start_epoch"])

            self.evaluate_decision(now_epoch)
            self.mark_to_market()
            self.enforce_trade_exits()

    def snapshot(self):
        with self.lock:
            now_epoch = int(time.time())
            seconds_to_decision = max(0, self.candle["decision_epoch"] - now_epoch)
            seconds_to_close = max(0, self.candle["end_epoch"] - now_epoch)
            open_unrealized = self.open_trade["unrealized_pnl"] if self.open_trade else 0.0
            equity = self.balance + open_unrealized
            closed_trades = [trade for trade in self.trades if trade["status"] == "CLOSED"]
            wins = len([trade for trade in closed_trades if trade.get("outcome") == "WIN"])
            losses = len([trade for trade in closed_trades if trade.get("outcome") == "LOSS"])
            win_rate = round((wins / len(closed_trades)) * 100, 1) if closed_trades else 0.0
            return {
                "mode": "paper",
                "feed_mode": self.feed_mode,
                "symbol": SYMBOL,
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "account": {
                    "start_balance": round(self.start_balance, 2),
                    "balance": round(self.balance, 2),
                    "equity": round(equity, 2),
                    "realized_pnl": round(self.realized_pnl, 2),
                    "unrealized_pnl": round(open_unrealized, 2),
                    "total_pnl": round(equity - self.start_balance, 2),
                    "lots_per_trade": PAPER_LOTS,
                    "dollars_per_point": round(PAPER_LOTS * PAPER_DOLLARS_PER_POINT_PER_LOT, 2),
                    "closed_trades": len(closed_trades),
                    "wins": wins,
                    "losses": losses,
                    "win_rate": win_rate,
                },
                "rules": {
                    "timeframe": "M15",
                    "decision_after_seconds": DECISION_SECONDS,
                    "max_counter_wick_points": MAX_COUNTER_WICK_POINTS,
                    "first_trailing_points": FIRST_TRAILING_POINTS,
                    "second_trailing_points": SECOND_TRAILING_POINTS,
                    "stop_loss_points": STOP_LOSS_POINTS,
                    "strict_mode": True,
                    "strict_mode_description": "At 180 seconds, enter only if the candle is directional and counter wick is <= 20 points.",
                    "paper_lots": PAPER_LOTS,
                    "paper_start_balance": PAPER_START_BALANCE,
                    "paper_dollars_per_point_per_lot": PAPER_DOLLARS_PER_POINT_PER_LOT,
                    "real_orders_enabled": False,
                },
                "price": round(self.price, 2),
                "candle": dict(self.candle),
                "seconds_to_decision": seconds_to_decision,
                "seconds_to_close": seconds_to_close,
                "open_trade": dict(self.open_trade) if self.open_trade else None,
                "trades": [dict(trade) for trade in self.trades],
                "transactions": [dict(transaction) for transaction in self.transactions],
                "decisions": list(self.decision_log),
                "persistence": {
                    "type": "local-json",
                    "file": str(STATE_FILE),
                    "exists": STATE_FILE.exists(),
                    "note": "Lightweight paper-state persistence; not a database.",
                },
                "provider_status": {
                    "current": self.feed_mode,
                    "configured_provider": DATA_PROVIDER,
                    "provider": self.provider.snapshot() if self.provider else None,
                    "recommended": [
                        "Use Yahoo/NQ=F only for read-only paper testing against real market movement.",
                        "Use broker demo pricing before trusting NAS100 CFD execution behavior.",
                        "Add spread/slippage modeling before interpreting paper P/L.",
                    ],
                },
                "oanda": oanda_config_status(),
            }


engine = Nas100PaperEngine()


def run_engine():
    while True:
        engine.tick()
        time.sleep(1)


class AppHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        return

    def send_json(self, payload):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/api/paper", "/api/data"):
            self.send_json(engine.snapshot())
            return
        if path == "/api/oanda/status":
            self.send_json(oanda_config_status())
            return
        if path == "/api/oanda/accounts":
            self.send_json(oanda_request("/v3/accounts"))
            return
        if path == "/api/oanda/pricing":
            if not OANDA_ACCOUNT_ID:
                self.send_json({"ok": False, "status": 428, "error": "OANDA_ACCOUNT_ID is not configured."})
                return
            instruments = "NAS100_USD"
            self.send_json(oanda_request(f"/v3/accounts/{OANDA_ACCOUNT_ID}/pricing?instruments={instruments}"))
            return

        file_name = "trading.html" if path in ("/", "/paper-trading", "/paper-trading/") else "index.html"
        file_path = APP_DIR / "static" / file_name
        html = file_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.end_headers()
        self.wfile.write(html)


def main():
    threading.Thread(target=run_engine, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), AppHandler)
    print(f"NAS100 paper trading app running at http://{HOST}:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
