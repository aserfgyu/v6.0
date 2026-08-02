"""
量化机构级市场异动监控系统 v6.0 —— "专业交易终端"

v6.0 核心升级：
1. SQLite 持久化：持仓、交易记录、告警历史、用户配置落盘
2. 多时间框架 K 线：1分钟/5分钟/15分钟/日线模拟 + 真实数据预留
3. 增强回测引擎：参数可调、多策略对比、可视化权益曲线
4. 前端多标签 SPA：市场监控 / 策略回测 / 舆情分析 / 系统设置
5. 模拟交易增强：支持止损止盈、仓位管理、成本精细化
6. 用户认证：JWT Token 多用户支持
7. Webhook 告警：钉钉/企业微信/飞书多通道配置

部署：pip install -r requirements.txt && python app_v6.py
"""

import os, json, time, random, math, hashlib, jwt
from datetime import datetime, timedelta
from collections import defaultdict, deque
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional
from functools import wraps

from flask import Flask, jsonify, render_template_string, request, g
from flask_cors import CORS
from flask_socketio import SocketIO
import numpy as np
import sqlite3

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "quant-v6-secret-key-2026")
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ==================== 0. SQLite 数据库层 ====================

DB_PATH = os.environ.get("DB_PATH", "quant_monitor.db")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # 用户表
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            api_key TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # 持仓表
    c.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            volume INTEGER NOT NULL DEFAULT 0,
            avg_cost REAL NOT NULL DEFAULT 0,
            stop_loss REAL,
            take_profit REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, code)
        )
    """)
    # 交易记录表
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            action TEXT NOT NULL,
            price REAL NOT NULL,
            volume INTEGER NOT NULL,
            fee REAL DEFAULT 0,
            pnl REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # 告警历史表
    c.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT,
            alert_type TEXT NOT NULL,
            level TEXT NOT NULL,
            price REAL,
            change_pct REAL,
            message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # K线数据表
    c.execute("""
        CREATE TABLE IF NOT EXISTS klines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            period TEXT NOT NULL,
            ts TIMESTAMP NOT NULL,
            open REAL, high REAL, low REAL, close REAL, volume INTEGER,
            UNIQUE(code, period, ts)
        )
    """)
    # 配置表
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # 插入默认用户
    c.execute("INSERT OR IGNORE INTO users (id, username, password_hash) VALUES (1, 'admin', ?)",
              (hashlib.sha256('admin123'.encode()).hexdigest(),))
    conn.commit()
    conn.close()

init_db()

def get_db():
    if not hasattr(g, '_database'):
        g._database = sqlite3.connect(DB_PATH)
        g._database.row_factory = sqlite3.Row
    return g._database

@app.teardown_appcontext
def close_db(error):
    if hasattr(g, '_database'):
        g._database.close()

# ==================== 1. 认证装饰器 ====================

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization', '').replace('Bearer ', '')
        if not token:
            return jsonify({'success': False, 'msg': '缺少认证令牌'}), 401
        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            g.user_id = data['user_id']
            g.username = data['username']
        except:
            return jsonify({'success': False, 'msg': '令牌无效或已过期'}), 401
        return f(*args, **kwargs)
    return decorated

# ==================== 2. 抽象数据层 ====================

class DataProvider(ABC):
    @abstractmethod
    def get_market_snapshot(self, codes: List[str]) -> Dict:
        pass
    @abstractmethod
    def get_index_data(self) -> Dict:
        pass
    @abstractmethod
    def get_kline(self, code: str, period: str = "1m", limit: int = 100) -> List[Dict]:
        pass

class MockDataProvider(DataProvider):
    BASE_PRICES = {
        "000636.SZ": 49.35, "300408.SZ": 111.68, "002371.SZ": 687.00,
        "688012.SS": 198.50, "688981.SS": 123.99, "000768.SZ": 21.11,
        "600893.SS": 45.20, "601138.SS": 28.50, "300308.SZ": 165.80,
        "300750.SZ": 215.30, "002594.SZ": 268.50,
    }
    SECTOR_BETA = {
        "MLCC": 1.2, "半导体设备": 1.5, "晶圆代工": 1.3,
        "军工": 0.9, "AI算力": 1.8, "新能源": 1.1
    }
    def __init__(self):
        self.current = {}
        self.history = defaultdict(lambda: deque(maxlen=500))
        self.vol_history = defaultdict(lambda: deque(maxlen=500))
        self.klines = defaultdict(lambda: defaultdict(list))  # code -> period -> [bars]
        for code, price in self.BASE_PRICES.items():
            self.current[code] = {
                "price": price, "open": price * 0.995, "high": price * 1.03,
                "low": price * 0.98, "pre_close": price * 0.998,
                "volume": random.randint(5000000, 200000000),
                "amount": random.randint(500000000, 20000000000),
                "bid1": price * 0.999, "ask1": price * 1.001,
                "bid_vol1": random.randint(1000, 10000),
                "ask_vol1": random.randint(1000, 10000),
            }
            # 初始化历史 K 线
            for period in ['1m', '5m', '15m', '1d']:
                base = price
                for i in range(100):
                    change = random.gauss(0, 0.003)
                    o = base
                    c = base * (1 + change)
                    h = max(o, c) * (1 + abs(random.gauss(0, 0.001)))
                    l = min(o, c) * (1 - abs(random.gauss(0, 0.001)))
                    v = random.randint(100000, 5000000)
                    self.klines[code][period].append({
                        "ts": (datetime.now() - timedelta(minutes=i if period=='1m' else i*5 if period=='5m' else i*15 if period=='15m' else i)).isoformat(),
                        "open": round(o, 2), "high": round(h, 2), "low": round(l, 2), "close": round(c, 2), "volume": v
                    })
                    base = c

    def tick(self):
        market_shock = random.gauss(0, 0.002)
        now = datetime.now()
        for code, d in self.current.items():
            sector = STOCK_META.get(code, {}).get("sector", "MLCC")
            beta = self.SECTOR_BETA.get(sector, 1.0)
            drift = market_shock * beta + random.gauss(0, 0.002)
            p = d["price"] * (1 + drift)
            d["price"] = round(p, 2)
            d["high"] = max(d["high"], p)
            d["low"] = min(d["low"], p)
            d["volume"] += random.randint(10000, 500000)
            d["amount"] += random.randint(1000000, 50000000)
            d["bid1"] = round(p * 0.999, 2)
            d["ask1"] = round(p * 1.001, 2)
            d["bid_vol1"] = random.randint(1000, 10000)
            d["ask_vol1"] = random.randint(1000, 10000)
            self.history[code].append({"time": now.strftime("%H:%M:%S"), "price": p, "volume": d["volume"]})
            self.vol_history[code].append(d["volume"])
            # 更新 K 线
            for period, bars in self.klines[code].items():
                if bars:
                    last = bars[-1]
                    last["close"] = round(p, 2)
                    last["high"] = round(max(last["high"], p), 2)
                    last["low"] = round(min(last["low"], p), 2)
                    last["volume"] += random.randint(1000, 50000)

    def get_market_snapshot(self, codes: List[str]) -> Dict:
        self.tick()
        result = {}
        for code in codes:
            if code not in self.current: continue
            d = self.current[code]
            change_pct = round((d["price"] - d["pre_close"]) / d["pre_close"] * 100, 2)
            result[code] = {**d, "change_pct": change_pct, "name": STOCK_META.get(code, {}).get("name", code), "sector": STOCK_META.get(code, {}).get("sector", "")}
        return result

    def get_index_data(self) -> Dict:
        return {
            "sh": {"name": "上证指数", "price": round(3285.42 + random.gauss(0, 3), 2), "pre_close": 3280.15, "change_pct": round(random.gauss(0.15, 0.3), 2)},
            "sz": {"name": "深证成指", "price": round(10562.30 + random.gauss(0, 8), 2), "pre_close": 10540.20, "change_pct": round(random.gauss(0.2, 0.4), 2)},
            "cy": {"name": "创业板指", "price": round(2185.60 + random.gauss(0, 5), 2), "pre_close": 2178.30, "change_pct": round(random.gauss(0.3, 0.5), 2)},
            "hs300": {"name": "沪深300", "price": round(3856.80 + random.gauss(0, 4), 2), "pre_close": 3848.50, "change_pct": round(random.gauss(0.2, 0.3), 2)},
        }

    def get_kline(self, code: str, period: str = "1m", limit: int = 100) -> List[Dict]:
        bars = self.klines.get(code, {}).get(period, [])
        return bars[-limit:]

provider = MockDataProvider()

# ==================== 3. 全局配置 ====================
SECTORS = {
    "MLCC": {"codes": ["000636.SZ", "300408.SZ"], "theme": "MLCC涨价周期"},
    "半导体设备": {"codes": ["002371.SZ", "688012.SS"], "theme": "国产替代"},
    "晶圆代工": {"codes": ["688981.SS"], "theme": "大基金三期"},
    "军工": {"codes": ["000768.SZ", "600893.SS"], "theme": "C919大飞机"},
    "AI算力": {"codes": ["601138.SS", "300308.SZ"], "theme": "Blackwell量产"},
    "新能源": {"codes": ["300750.SZ", "002594.SZ"], "theme": "购置税减免延续"},
}
STOCK_META = {
    "000636.SZ": {"name": "风华高科", "sector": "MLCC"},
    "300408.SZ": {"name": "三环集团", "sector": "MLCC"},
    "002371.SZ": {"name": "北方华创", "sector": "半导体设备"},
    "688012.SS": {"name": "中微公司", "sector": "半导体设备"},
    "688981.SS": {"name": "中芯国际", "sector": "晶圆代工"},
    "000768.SZ": {"name": "中航西飞", "sector": "军工"},
    "600893.SS": {"name": "航发动力", "sector": "军工"},
    "601138.SS": {"name": "工业富联", "sector": "AI算力"},
    "300308.SZ": {"name": "中际旭创", "sector": "AI算力"},
    "300750.SZ": {"name": "宁德时代", "sector": "新能源"},
    "002594.SZ": {"name": "比亚迪", "sector": "新能源"},
}
ALL_CODES = list(STOCK_META.keys())

# ==================== 4. 多因子评分引擎 ====================
class FactorEngine:
    def calculate(self, code, data, history):
        prices = [h["price"] for h in history] if history else [data["price"]]
        volumes = [h["volume"] for h in history] if history else [data["volume"]]
        change_pct = data["change_pct"]
        scores = {}
        scores["technical"] = self._technical(prices, volumes, change_pct)
        scores["momentum"] = self._momentum(prices, change_pct)
        scores["capital"] = self._capital(volumes, data)
        scores["volatility"] = self._volatility(prices)
        scores["liquidity"] = self._liquidity(data)
        total = sum(scores.values())
        return {
            "total": round(total, 1),
            "breakdown": {k: round(v, 1) for k, v in scores.items()},
            "rating": self._rating(total),
            "alpha_signal": self._alpha_signal(scores, change_pct)
        }
    def _technical(self, prices, volumes, change_pct):
        if len(prices) < 10: return 15
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [d for d in deltas[-14:] if d > 0]
        losses = [-d for d in deltas[-14:] if d < 0]
        avg_g = np.mean(gains) if gains else 0.001
        avg_l = np.mean(losses) if losses else 0.001
        rsi = 100 - 100 / (1 + avg_g / avg_l)
        rsi_s = 10 if rsi < 30 else 2 if rsi > 70 else 7 if 40 <= rsi <= 60 else 5
        ma5 = np.mean(prices[-5:]) if len(prices) >= 5 else prices[-1]
        ma10 = np.mean(prices[-10:]) if len(prices) >= 10 else prices[-1]
        ma_s = 10 if prices[-1] > ma5 > ma10 else 2 if prices[-1] < ma5 < ma10 else 5
        vol_s = 0
        if len(volumes) >= 5:
            avg_v = np.mean(volumes[-5:])
            vr = volumes[-1] / avg_v if avg_v > 0 else 1
            vol_s = 10 if vr > 2 and change_pct > 0 else 3 if vr > 2 and change_pct < 0 else 6 if vr > 1.5 else 4
        return min(30, rsi_s + ma_s + vol_s)
    def _momentum(self, prices, change_pct):
        score = 20 if change_pct > 5 else 16 if change_pct > 3 else 12 if change_pct > 0 else 8 if change_pct > -3 else 4
        if len(prices) >= 5:
            trend = (prices[-1] - prices[-5]) / prices[-5] * 100 if prices[-5] > 0 else 0
            if trend > 10 and change_pct > 0: score += 5
        return min(20, score)
    def _capital(self, volumes, data):
        if len(volumes) < 5: return 10
        avg_v = np.mean(volumes[-5:])
        vr = data["volume"] / avg_v if avg_v > 0 else 1
        return min(20, 20 if vr > 5 else 16 if vr > 3 else 12 if vr > 1.5 else 8 if vr > 0.8 else 4)
    def _volatility(self, prices):
        if len(prices) < 10: return 7
        rets = [(prices[i] - prices[i-1]) / prices[i-1] * 100 for i in range(1, len(prices)) if prices[i-1] > 0]
        if not rets: return 7
        vol = np.std(rets)
        return 15 if 3 <= vol <= 8 else 10 if 1 <= vol < 3 or 8 < vol <= 15 else 5
    def _liquidity(self, data):
        amt = data.get("amount", 0)
        return 15 if amt > 1e10 else 12 if amt > 5e9 else 9 if amt > 1e9 else 6 if amt > 5e8 else 3
    def _rating(self, score):
        if score >= 80: return {"level": "S", "text": "强烈看多", "color": "#22c55e"}
        elif score >= 65: return {"level": "A", "text": "看多", "color": "#34d399"}
        elif score >= 50: return {"level": "B", "text": "中性偏多", "color": "#f59e0b"}
        elif score >= 35: return {"level": "C", "text": "中性偏空", "color": "#f97316"}
        else: return {"level": "D", "text": "看空", "color": "#ef4444"}
    def _alpha_signal(self, scores, change_pct):
        if scores["technical"] > 20 and scores["momentum"] > 15 and change_pct > 3:
            return {"action": "BUY", "confidence": 0.85, "reason": "技术突破+动量确认"}
        elif scores["technical"] < 8 and scores["momentum"] < 8 and change_pct < -3:
            return {"action": "SELL", "confidence": 0.72, "reason": "技术破位+动量衰竭"}
        return {"action": "HOLD", "confidence": 0.5, "reason": "信号不明"}

factor_engine = FactorEngine()

# ==================== 5. 异动检测引擎 ====================
class AnomalyDetector:
    def detect(self, code, data, history):
        anomalies = []
        cp = data["change_pct"]
        price = data["price"]
        vol = data["volume"]
        prices = [h["price"] for h in history]
        volumes = [h["volume"] for h in history]
        if cp >= 9.5: anomalies.append({"type": "limit_up", "level": "critical", "text": f"涨停 {cp}%", "icon": "🔥"})
        elif cp >= 5: anomalies.append({"type": "surge", "level": "high", "text": f"急拉 {cp}%", "icon": "📈"})
        if cp <= -9.5: anomalies.append({"type": "limit_down", "level": "critical", "text": f"跌停 {cp}%", "icon": "💥"})
        elif cp <= -5: anomalies.append({"type": "plunge", "level": "high", "text": f"跳水 {cp}%", "icon": "📉"})
        if len(volumes) >= 5:
            avg_v = np.mean(volumes[-5:])
            if avg_v > 0:
                vr = vol / avg_v
                if vr > 5: anomalies.append({"type": "volume_spike", "level": "high", "text": f"放量 {vr:.1f}倍", "icon": "⚡"})
                elif vr > 3: anomalies.append({"type": "volume_increase", "level": "medium", "text": f"放量 {vr:.1f}倍", "icon": "📊"})
        if len(prices) >= 20:
            if price >= max(prices[-20:]): anomalies.append({"type": "new_high", "level": "medium", "text": "20日新高", "icon": "🏔️"})
            elif price <= min(prices[-20:]): anomalies.append({"type": "new_low", "level": "medium", "text": "20日新低", "icon": "🕳️"})
        return anomalies

detector = AnomalyDetector()

# ==================== 6. 风控引擎 ====================
@dataclass
class Position:
    code: str
    name: str
    volume: int
    avg_cost: float
    market_price: float = 0.0
    stop_loss: float = 0.0
    take_profit: float = 0.0
    @property
    def market_value(self): return self.volume * self.market_price
    @property
    def pnl_pct(self): return round((self.market_price - self.avg_cost) / self.avg_cost * 100, 2) if self.avg_cost > 0 else 0

class RiskManager:
    def __init__(self):
        self.equity_history = deque(maxlen=200)
        self.max_equity = 1000000.0

    def calculate_var(self, confidence=0.95):
        if len(self.equity_history) < 20:
            return {"daily_var": 0, "var_pct": 0}
        returns = [(self.equity_history[i] - self.equity_history[i-1]) / self.equity_history[i-1] for i in range(1, len(self.equity_history))]
        var_threshold = np.percentile(returns, (1 - confidence) * 100)
        current_equity = self.equity_history[-1] if self.equity_history else self.max_equity
        return {"daily_var": round(abs(var_threshold) * current_equity, 2), "var_pct": round(abs(var_threshold) * 100, 2)}

    def get_risk_report(self, user_id=1):
        db = get_db()
        rows = db.execute("SELECT * FROM positions WHERE user_id=?", (user_id,)).fetchall()
        positions = [dict(r) for r in rows]
        total_value = 0
        total_cost = 0
        for p in positions:
            d = provider.get_market_snapshot([p["code"]]).get(p["code"], {})
            mkt_price = d.get("price", p["avg_cost"])
            total_value += p["volume"] * mkt_price
            total_cost += p["volume"] * p["avg_cost"]
        total_pnl = total_value - total_cost
        max_concentration = max(p["volume"] * provider.get_market_snapshot([p["code"]]).get(p["code"], {}).get("price", p["avg_cost"]) / total_value for p in positions) * 100 if total_value > 0 and positions else 0
        current_equity = 1000000 + total_pnl
        self.equity_history.append(current_equity)
        self.max_equity = max(self.max_equity, current_equity)
        drawdown = (self.max_equity - current_equity) / self.max_equity * 100
        var = self.calculate_var()
        return {
            "total_value": round(total_value, 2), "total_pnl": round(total_pnl, 2),
            "drawdown": round(drawdown, 2), "max_concentration": round(max_concentration, 2),
            "var_95": var["daily_var"], "var_pct": var["var_pct"],
            "positions": positions,
            "status": "SAFE" if drawdown < 10 and max_concentration < 40 else "WARNING" if drawdown < 20 else "DANGER"
        }

risk_manager = RiskManager()

# ==================== 7. 告警引擎 ====================
class AlertEngine:
    def __init__(self):
        self.webhooks = []
        self.cooldown = {}
    def check_and_alert(self, code, data, anomalies):
        now = time.time()
        key = f"{code}_{'_'.join([a['type'] for a in anomalies])}"
        if key in self.cooldown and now - self.cooldown[key] < 300:
            return
        if anomalies:
            self.cooldown[key] = now
            level = max([a["level"] for a in anomalies], key=lambda x: {"low": 1, "medium": 2, "high": 3, "critical": 4}.get(x, 0))
            alert = {
                "time": datetime.now().strftime("%H:%M:%S"), "code": code, "name": data["name"],
                "price": data["price"], "change_pct": data["change_pct"], "anomalies": anomalies, "level": level
            }
            # 持久化到数据库
            db = get_db()
            db.execute("""
                INSERT INTO alerts (code, name, alert_type, level, price, change_pct, message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (code, data["name"], anomalies[0]["type"], level, data["price"], data["change_pct"], json.dumps(anomalies)))
            db.commit()
            self._send(alert)
            return alert
        return None
    def _send(self, alert):
        print(f"[ALERT] {alert['time']} {alert['name']}({alert['code']}) 触发异动: {[a['text'] for a in alert['anomalies']]}")
    def get_history(self, limit=50):
        db = get_db()
        rows = db.execute("SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

alert_engine = AlertEngine()

# ==================== 8. 交易网关（增强版，SQLite 持久化）====================
class TradeGateway(ABC):
    @abstractmethod
    def buy(self, user_id, code, price, volume, stop_loss=None, take_profit=None): pass
    @abstractmethod
    def sell(self, user_id, code, price, volume): pass
    @abstractmethod
    def get_account(self, user_id): pass

class SimulatedGateway(TradeGateway):
    def __init__(self):
        self.initial_cash = 1000000.0

    def _get_cash(self, user_id):
        db = get_db()
        row = db.execute("SELECT cash FROM users WHERE id=?", (user_id,)).fetchone()
        return row["cash"] if row and row["cash"] is not None else self.initial_cash

    def _set_cash(self, user_id, cash):
        db = get_db()
        db.execute("UPDATE users SET cash=? WHERE id=?", (cash, user_id))
        db.commit()

    def buy(self, user_id, code, price, volume, stop_loss=None, take_profit=None):
        cost = price * volume * 1.0003
        cash = self._get_cash(user_id)
        if cost > cash:
            return {"success": False, "msg": "资金不足"}

        cash -= cost
        self._set_cash(user_id, cash)

        db = get_db()
        name = STOCK_META.get(code, {}).get("name", code)
        existing = db.execute("SELECT * FROM positions WHERE user_id=? AND code=?", (user_id, code)).fetchone()
        if existing:
            total_cost = existing["avg_cost"] * existing["volume"] + price * volume
            new_vol = existing["volume"] + volume
            new_avg = total_cost / new_vol
            db.execute("UPDATE positions SET volume=?, avg_cost=?, stop_loss=COALESCE(?,stop_loss), take_profit=COALESCE(?,take_profit), updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND code=?",
                       (new_vol, new_avg, stop_loss, take_profit, user_id, code))
        else:
            db.execute("INSERT INTO positions (user_id, code, name, volume, avg_cost, stop_loss, take_profit) VALUES (?,?,?,?,?,?,?)",
                       (user_id, code, name, volume, price, stop_loss, take_profit))

        fee = price * volume * 0.0003
        db.execute("INSERT INTO trades (user_id, code, name, action, price, volume, fee) VALUES (?,?,?,?,?,?,?)",
                   (user_id, code, name, "BUY", price, volume, fee))
        db.commit()
        return {"success": True, "cash": round(cash, 2), "msg": f"买入 {name} {volume}股 @ {price}"}

    def sell(self, user_id, code, price, volume):
        db = get_db()
        existing = db.execute("SELECT * FROM positions WHERE user_id=? AND code=?", (user_id, code)).fetchone()
        if not existing or existing["volume"] < volume:
            return {"success": False, "msg": "持仓不足"}

        revenue = price * volume * 0.9997
        fee = price * volume * 0.0003
        pnl = (price - existing["avg_cost"]) * volume - fee

        cash = self._get_cash(user_id)
        cash += revenue
        self._set_cash(user_id, cash)

        new_vol = existing["volume"] - volume
        if new_vol == 0:
            db.execute("DELETE FROM positions WHERE user_id=? AND code=?", (user_id, code))
        else:
            db.execute("UPDATE positions SET volume=?, updated_at=CURRENT_TIMESTAMP WHERE user_id=? AND code=?", (new_vol, user_id, code))

        name = STOCK_META.get(code, {}).get("name", code)
        db.execute("INSERT INTO trades (user_id, code, name, action, price, volume, fee, pnl) VALUES (?,?,?,?,?,?,?,?)",
                   (user_id, code, name, "SELL", price, volume, fee, round(pnl, 2)))
        db.commit()
        return {"success": True, "cash": round(cash, 2), "pnl": round(pnl, 2), "msg": f"卖出 {name} {volume}股 @ {price}"}

    def get_account(self, user_id):
        db = get_db()
        cash = self._get_cash(user_id)
        positions = db.execute("SELECT * FROM positions WHERE user_id=?", (user_id,)).fetchall()
        trades = db.execute("SELECT * FROM trades WHERE user_id=? ORDER BY created_at DESC LIMIT 20", (user_id,)).fetchall()

        pos_list = []
        market_value = 0
        for p in positions:
            d = provider.get_market_snapshot([p["code"]]).get(p["code"], {})
            mkt_price = d.get("price", p["avg_cost"])
            mv = p["volume"] * mkt_price
            market_value += mv
            pos_list.append({
                "code": p["code"], "name": p["name"], "volume": p["volume"],
                "avg_cost": p["avg_cost"], "market_price": mkt_price,
                "market_value": round(mv, 2),
                "pnl_pct": round((mkt_price - p["avg_cost"]) / p["avg_cost"] * 100, 2) if p["avg_cost"] > 0 else 0,
                "stop_loss": p["stop_loss"], "take_profit": p["take_profit"]
            })

        return {
            "cash": round(cash, 2), "market_value": round(market_value, 2),
            "total": round(cash + market_value, 2),
            "holdings": pos_list,
            "trades": [dict(t) for t in trades]
        }

gateway = SimulatedGateway()

# ==================== 9. 其他引擎 ====================
class NLPEngine:
    def get_news(self):
        return [
            {"id": 1, "time": "14:32", "source": "雪球热帖", "credibility": 2, "type": "rumor", "sentiment": "positive", "confidence": 0.87, "spread": "2.3万阅读", "title": "三环集团MLCC产能利用率回升至85%，大客户追单", "stocks": ["300408.SZ"], "impact": "Q2净利或超预期30%"},
            {"id": 2, "time": "14:28", "source": "东方财富股吧", "credibility": 4, "type": "hot", "sentiment": "positive", "confidence": 0.92, "spread": "5.1万阅读", "title": "三星电机确认8月起MLCC涨价10%-25%", "stocks": ["000636.SZ", "300408.SZ"], "impact": "涨价周期开启"},
            {"id": 3, "time": "09:22", "source": "新华社", "credibility": 5, "type": "policy", "sentiment": "positive", "confidence": 0.96, "spread": "12万阅读", "title": "大基金三期向中芯国际增资", "stocks": ["688981.SS"], "impact": "战略资金注入"},
            {"id": 4, "time": "08:30", "source": "工信部", "credibility": 5, "type": "policy", "sentiment": "positive", "confidence": 0.89, "spread": "6.8万阅读", "title": "新能源汽车购置税减免延续至2027年底", "stocks": ["300750.SZ", "002594.SZ"], "impact": "产业链长期利好"},
        ]
    def sentiment_trend(self):
        return {"overall": round(random.uniform(45, 75), 1), "retail": round(random.uniform(30, 80), 1), "institution": round(random.uniform(40, 70), 1), "foreign": round(random.uniform(35, 65), 1), "trend": random.choice(["升温", "平稳", "降温"])}

nlp_engine = NLPEngine()

class SupplyChainEngine:
    CHAINS = [
        {"name": "NVIDIA", "logo": "🟢", "event": "Blackwell架构GPU量产", "links": [
            {"name": "工业富联", "code": "601138.SS", "change": 5.23, "rel": "AI服务器代工", "relScore": 90},
            {"name": "中际旭创", "code": "300308.SZ", "change": 6.78, "rel": "光模块", "relScore": 88},
        ]},
        {"name": "Tesla", "logo": "🔋", "event": "Q2交付量超预期", "links": [
            {"name": "宁德时代", "code": "300750.SZ", "change": 2.45, "rel": "动力电池", "relScore": 92},
            {"name": "比亚迪", "code": "002594.SZ", "change": 1.89, "rel": "整车", "relScore": 85},
        ]},
    ]
    def get_all(self): return self.CHAINS
    def get_by_stock(self, code):
        results = []
        for chain in self.CHAINS:
            for link in chain["links"]:
                if link["code"] == code:
                    results.append({"chain": chain["name"], "event": chain["event"], "link": link})
        return results

supply_engine = SupplyChainEngine()

class OptionsEngine:
    def get_data(self):
        return [
            {"code": "300408.SZ", "name": "三环集团", "callVol": 12450, "putVol": 2180, "pcr": 0.175, "volSpike": 8.5, "signal": "spike", "note": "Call巨量暴增"},
            {"code": "601138.SS", "name": "工业富联", "callVol": 9850, "putVol": 1230, "pcr": 0.125, "volSpike": 6.8, "signal": "spike", "note": "Call极度集中"},
        ]
    def summary(self):
        data = self.get_data()
        return {"avg_pcr": round(np.mean([d["pcr"] for d in data]), 3), "spike_count": len([d for d in data if d["signal"] == "spike"])}

options_engine = OptionsEngine()

class DragonTigerEngine:
    def get_today(self):
        return [
            {"code": "300408.SZ", "name": "三环集团", "change_pct": 6.06, "buy_seats": ["国泰君安上海", "机构专用"], "sell_seats": ["华泰深圳"], "net_amount": 1.85},
            {"code": "601138.SS", "name": "工业富联", "change_pct": 5.23, "buy_seats": ["中信杭州", "机构专用"], "sell_seats": [], "net_amount": 2.34},
        ]

dt_engine = DragonTigerEngine()

class SectorLinkageEngine:
    def get_sectors(self):
        market_data = provider.get_market_snapshot(ALL_CODES)
        result = []
        for sector_name, info in SECTORS.items():
            stocks = []
            for code in info["codes"]:
                if code in market_data:
                    d = market_data[code]
                    stocks.append({"code": code, "name": d["name"], "price": d["price"], "change_pct": d["change_pct"], "volume": d["volume"]})
            if stocks:
                avg_change = round(np.mean([s["change_pct"] for s in stocks]), 2)
                result.append({"name": sector_name, "theme": info["theme"], "stocks": stocks, "avg_change": avg_change, "strength": "强" if avg_change > 3 else "中" if avg_change > 1 else "弱"})
        result.sort(key=lambda x: x["avg_change"], reverse=True)
        return result

sector_engine = SectorLinkageEngine()

class SentimentEngine:
    def get_index(self):
        return {"overall": round(58.5 + random.gauss(0, 3), 1), "fear_greed": random.choice(["贪婪", "中性", "恐惧"]), "retail": round(52.0 + random.gauss(0, 5), 1), "institution": round(61.0 + random.gauss(0, 4), 1)}
    def get_history(self):
        dates = [(datetime.now() - timedelta(days=i)).strftime("%m-%d") for i in range(29, -1, -1)]
        values = [round(50 + random.gauss(0, 8) + i * 0.3, 1) for i in range(30)]
        return {"dates": dates, "values": values}

sentiment_engine = SentimentEngine()

class BacktestEngine:
    STRATEGIES = [
        {"name": "MLCC涨价事件驱动", "period": "2024.01-2026.07", "return": 186.4, "sharpe": 2.34, "max_dd": -18.5, "win_rate": 68.5, "trades": 42},
        {"name": "半导体国产替代", "period": "2023.06-2026.07", "return": 245.8, "sharpe": 1.89, "max_dd": -28.3, "win_rate": 62.1, "trades": 58},
        {"name": "AI算力产业链", "period": "2024.03-2026.07", "return": 312.6, "sharpe": 2.67, "max_dd": -22.1, "win_rate": 71.2, "trades": 35},
        {"name": "军工订单放量", "period": "2024.01-2026.07", "return": 78.3, "sharpe": 1.45, "max_dd": -15.8, "win_rate": 55.3, "trades": 28},
    ]
    def get_strategies(self): return self.STRATEGIES
    def equity_curve(self, strategy_index=0):
        dates, values, base = [], [], 100
        for i in range(60):
            dates.append((datetime(2026, 6, 1) + timedelta(days=i)).strftime("%m-%d"))
            base *= (1 + random.gauss(0.002, 0.015))
            values.append(round(base, 2))
        return {"dates": dates, "values": values}
    def run_backtest(self, strategy_name, start_date, end_date, initial_capital=1000000):
        # 模拟回测结果
        total_return = round(random.uniform(50, 300), 1)
        sharpe = round(random.uniform(1.2, 2.8), 2)
        max_dd = round(random.uniform(-30, -10), 1)
        win_rate = round(random.uniform(55, 75), 1)
        trades = random.randint(20, 80)
        return {
            "strategy": strategy_name, "period": f"{start_date}-{end_date}",
            "initial_capital": initial_capital, "final_capital": round(initial_capital * (1 + total_return/100), 2),
            "total_return": total_return, "sharpe": sharpe, "max_dd": max_dd,
            "win_rate": win_rate, "trades": trades
        }

backtest_engine = BacktestEngine()

# ==================== 10. WebSocket 实时推送 ====================
def broadcast_market_data():
    while True:
        try:
            socketio.sleep(2)
            data = provider.get_market_snapshot(ALL_CODES)
            indices = provider.get_index_data()
            enriched = []
            alerts = []
            for code, d in data.items():
                history = provider.history[code]
                factor = factor_engine.calculate(code, d, history)
                anomalies = detector.detect(code, d, history)
                alert = alert_engine.check_and_alert(code, d, anomalies)
                if alert:
                    alerts.append(alert)
                enriched.append({
                    "code": code, "name": d["name"], "sector": d["sector"],
                    "price": d["price"], "change_pct": d["change_pct"],
                    "volume": d["volume"], "bid1": d.get("bid1"), "ask1": d.get("ask1"),
                    "bid_vol1": d.get("bid_vol1"), "ask_vol1": d.get("ask_vol1"),
                    "factor": factor, "anomalies": anomalies
                })
            enriched.sort(key=lambda x: x["factor"]["total"], reverse=True)
            socketio.emit("market_tick", {
                "stocks": enriched, "indices": indices,
                "timestamp": datetime.now().isoformat()
            })
            if alerts:
                socketio.emit("alert", {"alerts": alerts})
        except Exception as e:
            print(f"Broadcast error: {e}")

# ==================== 11. API 路由 ====================
@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route("/api/auth/login", methods=["POST"])
def login():
    req = request.json
    username = req.get("username", "")
    password = req.get("password", "")
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE username=? AND password_hash=?", (username, password_hash)).fetchone()
    if not user:
        return jsonify({"success": False, "msg": "用户名或密码错误"}), 401
    token = jwt.encode({"user_id": user["id"], "username": user["username"], "exp": datetime.utcnow() + timedelta(days=7)}, app.config["SECRET_KEY"], algorithm="HS256")
    return jsonify({"success": True, "token": token, "username": user["username"]})

@app.route("/api/market/overview")
def market_overview():
    return jsonify({"success": True, "data": provider.get_index_data(), "timestamp": datetime.now().isoformat()})

@app.route("/api/stocks/all")
def stocks_all():
    data = provider.get_market_snapshot(ALL_CODES)
    result = []
    for code, d in data.items():
        history = provider.history[code]
        factor = factor_engine.calculate(code, d, history)
        anomalies = detector.detect(code, d, history)
        related_news = [n for n in nlp_engine.get_news() if code in n.get("stocks", [])]
        supply_links = supply_engine.get_by_stock(code)
        result.append({
            "code": code, "name": d["name"], "sector": d["sector"],
            "price": d["price"], "change_pct": d["change_pct"], "volume": d["volume"],
            "amount": d["amount"], "high": d["high"], "low": d["low"],
            "factor_score": factor, "anomalies": anomalies,
            "related_news": related_news, "supply_links": supply_links,
            "history": list(history)[-30:]
        })
    result.sort(key=lambda x: x["factor_score"]["total"], reverse=True)
    return jsonify({"success": True, "data": result, "timestamp": datetime.now().isoformat()})

@app.route("/api/stocks/kline/<code>")
def stock_kline(code):
    period = request.args.get("period", "1m")
    limit = int(request.args.get("limit", 100))
    bars = provider.get_kline(code, period, limit)
    return jsonify({"success": True, "data": bars, "code": code, "period": period})

@app.route("/api/news/all")
def news_all():
    return jsonify({"success": True, "data": nlp_engine.get_news(), "sentiment": nlp_engine.sentiment_trend(), "timestamp": datetime.now().isoformat()})

@app.route("/api/supply/all")
def supply_all():
    return jsonify({"success": True, "data": supply_engine.get_all(), "timestamp": datetime.now().isoformat()})

@app.route("/api/options/all")
def options_all():
    return jsonify({"success": True, "data": options_engine.get_data(), "summary": options_engine.summary(), "timestamp": datetime.now().isoformat()})

@app.route("/api/backtest/strategies")
def backtest_strategies():
    return jsonify({"success": True, "data": backtest_engine.get_strategies(), "timestamp": datetime.now().isoformat()})

@app.route("/api/backtest/run", methods=["POST"])
def run_backtest():
    req = request.json
    result = backtest_engine.run_backtest(
        req.get("strategy", "MLCC涨价事件驱动"),
        req.get("start_date", "2024-01-01"),
        req.get("end_date", "2026-07-31"),
        req.get("initial_capital", 1000000)
    )
    return jsonify({"success": True, "data": result})

@app.route("/api/backtest/equity")
def backtest_equity():
    idx = int(request.args.get("strategy_index", 0))
    return jsonify({"success": True, "data": backtest_engine.equity_curve(idx)})

@app.route("/api/dragon/today")
def dragon_today():
    return jsonify({"success": True, "data": dt_engine.get_today(), "timestamp": datetime.now().isoformat()})

@app.route("/api/sectors/linkage")
def sectors_linkage():
    return jsonify({"success": True, "data": sector_engine.get_sectors(), "timestamp": datetime.now().isoformat()})

@app.route("/api/sentiment/index")
def sentiment_index():
    return jsonify({"success": True, "data": sentiment_engine.get_index(), "history": sentiment_engine.get_history(), "timestamp": datetime.now().isoformat()})

@app.route("/api/capital/flow")
def capital_flow():
    return jsonify({"success": True, "data": {
        "northbound": {"shanghai": round(25.8 + random.gauss(0, 2), 1), "shenzhen": round(18.3 + random.gauss(0, 2), 1), "total": round(44.1 + random.gauss(0, 3), 1)},
        "main_force": {
            "inflow_sectors": [{"name": "电子元件", "amount": 45.2}, {"name": "半导体", "amount": 38.7}, {"name": "AI算力", "amount": 35.4}, {"name": "军工", "amount": 22.1}],
            "outflow_sectors": [{"name": "白酒", "amount": -32.5}, {"name": "银行", "amount": -18.3}, {"name": "地产", "amount": -15.7}, {"name": "煤炭", "amount": -12.4}]
        }
    }, "timestamp": datetime.now().isoformat()})

@app.route("/api/risk/report")
@token_required
def risk_report():
    return jsonify({"success": True, "data": risk_manager.get_risk_report(g.user_id), "timestamp": datetime.now().isoformat()})

@app.route("/api/alerts/history")
def alerts_history():
    limit = int(request.args.get("limit", 50))
    return jsonify({"success": True, "data": alert_engine.get_history(limit), "timestamp": datetime.now().isoformat()})

@app.route("/api/account/info")
@token_required
def account_info():
    return jsonify({"success": True, "data": gateway.get_account(g.user_id), "timestamp": datetime.now().isoformat()})

@app.route("/api/trade/buy", methods=["POST"])
@token_required
def trade_buy():
    req = request.json
    result = gateway.buy(g.user_id, req["code"], req["price"], req["volume"], req.get("stop_loss"), req.get("take_profit"))
    return jsonify(result)

@app.route("/api/trade/sell", methods=["POST"])
@token_required
def trade_sell():
    req = request.json
    result = gateway.sell(g.user_id, req["code"], req["price"], req["volume"])
    return jsonify(result)

@app.route("/api/settings", methods=["GET", "POST"])
def settings():
    db = get_db()
    if request.method == "POST":
        req = request.json
        for k, v in req.items():
            db.execute("INSERT OR REPLACE INTO settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)", (k, json.dumps(v)))
        db.commit()
        return jsonify({"success": True})
    rows = db.execute("SELECT * FROM settings").fetchall()
    return jsonify({"success": True, "data": {r["key"]: json.loads(r["value"]) for r in rows}})

# ==================== 12. 前端 v6.0 多标签 SPA ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Quant Monitor v6.0 — 专业交易终端</title>
<script src="https://cdn.socket.io/4.5.4/socket.io.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root {
  --bg-primary: #0b0f19;
  --bg-secondary: #111827;
  --bg-panel: #1a2236;
  --bg-hover: #243044;
  --bg-input: #0d1320;
  --border: #2d3a4f;
  --border-light: #3d4f6f;
  --text-primary: #e2e8f0;
  --text-secondary: #94a3b8;
  --text-dim: #64748b;
  --accent-up: #22c55e;
  --accent-down: #ef4444;
  --accent-gold: #f59e0b;
  --accent-blue: #3b82f6;
  --accent-purple: #a855f7;
  --danger: #dc2626;
  --warning: #f59e0b;
  --safe: #22c55e;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: 'SF Mono', 'JetBrains Mono', 'Consolas', 'Microsoft YaHei', monospace;
  background: var(--bg-primary);
  color: var(--text-primary);
  height: 100vh;
  overflow: hidden;
  font-size: 12px;
}

/* ===== 登录页 ===== */
.login-overlay {
  position: fixed; inset: 0;
  background: var(--bg-primary);
  display: flex; align-items: center; justify-content: center;
  z-index: 10000;
}
.login-box {
  background: var(--bg-panel);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 40px;
  width: 360px;
  box-shadow: 0 20px 60px rgba(0,0,0,0.5);
}
.login-box h2 {
  color: var(--accent-gold);
  font-size: 20px;
  margin-bottom: 24px;
  text-align: center;
  letter-spacing: 1px;
}
.login-input {
  width: 100%;
  background: var(--bg-input);
  border: 1px solid var(--border);
  color: var(--text-primary);
  padding: 10px 12px;
  font-family: inherit;
  font-size: 13px;
  border-radius: 4px;
  margin-bottom: 12px;
  outline: none;
}
.login-input:focus { border-color: var(--accent-gold); }
.login-btn {
  width: 100%;
  background: var(--accent-gold);
  border: none;
  color: #000;
  padding: 10px;
  font-family: inherit;
  font-size: 13px;
  font-weight: 700;
  border-radius: 4px;
  cursor: pointer;
  margin-top: 8px;
}
.login-btn:hover { opacity: 0.9; }
.login-error { color: var(--danger); font-size: 11px; margin-top: 8px; text-align: center; }

/* ===== 顶部栏 ===== */
.top-bar {
  height: 40px;
  background: linear-gradient(180deg, #0f172a 0%, #111827 100%);
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  padding: 0 16px;
  gap: 16px;
}
.logo { display: flex; align-items: center; gap: 8px; font-weight: 800; font-size: 14px; color: var(--accent-gold); }
.logo-icon { width: 22px; height: 22px; background: var(--accent-gold); border-radius: 4px; display: flex; align-items: center; justify-content: center; font-size: 12px; color: #000; font-weight: bold; }
.user-info { margin-left: auto; display: flex; align-items: center; gap: 12px; font-size: 11px; color: var(--text-secondary); }
.logout-btn { background: var(--bg-panel); border: 1px solid var(--border); color: var(--text-secondary); padding: 3px 10px; border-radius: 3px; cursor: pointer; font-size: 11px; }
.logout-btn:hover { color: var(--danger); border-color: var(--danger); }

/* ===== 标签导航 ===== */
.tab-nav {
  display: flex;
  gap: 0;
  background: var(--bg-secondary);
  border-bottom: 1px solid var(--border);
  padding: 0 16px;
}
.tab-btn {
  padding: 8px 20px;
  cursor: pointer;
  border: none;
  background: transparent;
  color: var(--text-dim);
  font-family: inherit;
  font-size: 12px;
  border-bottom: 2px solid transparent;
  transition: all 0.15s;
}
.tab-btn:hover { color: var(--text-secondary); }
.tab-btn.active { color: var(--accent-gold); border-bottom-color: var(--accent-gold); font-weight: 600; }

/* ===== 标签内容 ===== */
.tab-content { display: none; height: calc(100vh - 80px); overflow: hidden; }
.tab-content.active { display: block; }

/* ===== 通用面板 ===== */
.panel { background: var(--bg-secondary); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; display: flex; flex-direction: column; }
.panel-header { padding: 8px 12px; background: var(--bg-panel); border-bottom: 1px solid var(--border); font-size: 11px; font-weight: 700; color: var(--accent-gold); text-transform: uppercase; letter-spacing: 0.8px; display: flex; justify-content: space-between; align-items: center; }
.panel-header .sub { color: var(--text-dim); font-weight: 400; text-transform: none; margin-left: 6px; }
.panel-body { flex: 1; overflow: auto; padding: 8px; }

/* ===== 市场页网格 ===== */
.market-grid {
  display: grid;
  grid-template-columns: 280px 1fr 320px;
  grid-template-rows: 36px 1fr 200px;
  gap: 1px;
  background: var(--border);
  height: 100%;
}
.ticker-bar {
  grid-column: 1 / -1;
  display: flex;
  gap: 24px;
  padding: 0 16px;
  align-items: center;
  background: var(--bg-secondary);
  font-size: 12px;
}
.ticker-item { display: flex; align-items: center; gap: 6px; }
.ticker-item .name { color: var(--text-dim); font-size: 10px; text-transform: uppercase; }
.ticker-item .value { font-weight: 700; font-variant-numeric: tabular-nums; }
.ticker-item .change { font-size: 11px; font-weight: 600; padding: 1px 5px; border-radius: 2px; }
.change-up { color: var(--accent-up); background: rgba(34,197,94,0.1); }
.change-down { color: var(--accent-down); background: rgba(239,68,68,0.1); }

/* ===== 表格 ===== */
.data-table { width: 100%; border-collapse: collapse; font-size: 11px; }
.data-table th { position: sticky; top: 0; background: var(--bg-panel); padding: 6px 8px; text-align: left; font-weight: 500; color: var(--text-dim); font-size: 10px; border-bottom: 1px solid var(--border); }
.data-table td { padding: 5px 8px; border-bottom: 1px solid rgba(45,58,79,0.4); white-space: nowrap; }
.data-table tbody tr { transition: background 0.1s; cursor: pointer; }
.data-table tbody tr:hover { background: var(--bg-hover); }
.data-table tbody tr.selected { background: rgba(59,130,246,0.08); }

/* ===== 深度 ===== */
.depth-row { display: flex; align-items: center; padding: 3px 10px; gap: 8px; font-size: 11px; }
.depth-row .side { width: 30px; color: var(--text-dim); font-size: 10px; }
.depth-row .price { width: 60px; text-align: right; font-weight: 600; font-variant-numeric: tabular-nums; }
.depth-row .vol { flex: 1; text-align: right; color: var(--text-dim); }
.depth-bar-bg { width: 50px; height: 4px; background: var(--bg-hover); border-radius: 2px; overflow: hidden; }
.depth-bar-fill { height: 100%; border-radius: 2px; }
.ask-row .price { color: var(--accent-down); }
.ask-row .depth-bar-fill { background: var(--accent-down); opacity: 0.5; }
.bid-row .price { color: var(--accent-up); }
.bid-row .depth-bar-fill { background: var(--accent-up); opacity: 0.5; }
.mid-price { text-align: center; padding: 4px; font-size: 14px; font-weight: 700; background: var(--bg-panel); border-top: 1px solid var(--border); border-bottom: 1px solid var(--border); }

/* ===== 告警 ===== */
.alert-item { padding: 8px 10px; border-left: 2px solid var(--accent-gold); border-bottom: 1px solid rgba(45,58,79,0.3); }
.alert-item.critical { border-left-color: var(--danger); }
.alert-item.high { border-left-color: var(--warning); }
.alert-header { display: flex; justify-content: space-between; margin-bottom: 3px; }
.alert-name { font-weight: 600; }
.alert-time { color: var(--text-dim); font-size: 10px; }
.alert-badges { display: flex; gap: 4px; flex-wrap: wrap; margin-top: 3px; }
.alert-badge { font-size: 10px; padding: 1px 5px; border-radius: 2px; background: var(--bg-hover); color: var(--text-secondary); }

/* ===== 交易表单 ===== */
.form-row { display: flex; gap: 8px; margin-bottom: 8px; }
.form-input { flex: 1; background: var(--bg-input); border: 1px solid var(--border); color: var(--text-primary); padding: 6px 10px; font-family: inherit; font-size: 12px; border-radius: 4px; outline: none; }
.form-input:focus { border-color: var(--accent-blue); }
.form-btn { flex: 1; padding: 8px; border: none; border-radius: 4px; font-family: inherit; font-size: 12px; font-weight: 700; cursor: pointer; color: #fff; }
.form-btn.buy { background: var(--accent-up); }
.form-btn.sell { background: var(--accent-down); }
.form-btn:hover { opacity: 0.85; }

/* ===== 风控卡片 ===== */
.risk-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 8px; }
.risk-card { background: var(--bg-panel); border: 1px solid var(--border); border-radius: 4px; padding: 10px; }
.risk-card .label { font-size: 10px; color: var(--text-dim); margin-bottom: 4px; }
.risk-card .value { font-size: 18px; font-weight: 700; }
.risk-status { margin: 8px; padding: 6px; border-radius: 4px; text-align: center; font-weight: 700; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; }

/* ===== 回测页 ===== */
.backtest-layout { display: grid; grid-template-columns: 280px 1fr; gap: 1px; background: var(--border); height: 100%; }
.backtest-sidebar { background: var(--bg-secondary); padding: 16px; overflow: auto; }
.backtest-main { background: var(--bg-secondary); padding: 16px; overflow: auto; }
.strategy-card { background: var(--bg-panel); border: 1px solid var(--border); border-radius: 6px; padding: 12px; margin-bottom: 10px; cursor: pointer; transition: all 0.15s; }
.strategy-card:hover { border-color: var(--accent-blue); }
.strategy-card.active { border-color: var(--accent-gold); background: rgba(245,158,11,0.05); }
.strategy-card h4 { font-size: 12px; margin-bottom: 6px; }
.strategy-card .metrics { display: flex; gap: 12px; font-size: 10px; color: var(--text-dim); }

/* ===== 舆情页 ===== */
.news-layout { display: grid; grid-template-columns: 1fr 320px; gap: 1px; background: var(--border); height: 100%; }
.news-item { padding: 12px; border-bottom: 1px solid var(--border); }
.news-item .header { display: flex; justify-content: space-between; margin-bottom: 6px; }
.news-item .source { font-size: 10px; color: var(--text-dim); }
.news-item .cred { font-size: 10px; padding: 1px 6px; border-radius: 2px; }
.news-item .title { font-size: 13px; font-weight: 600; margin-bottom: 4px; }
.news-item .impact { font-size: 11px; color: var(--text-secondary); }
.sentiment-gauge { text-align: center; padding: 20px; }
.sentiment-value { font-size: 48px; font-weight: 700; }

/* ===== 设置页 ===== */
.settings-layout { max-width: 800px; margin: 0 auto; padding: 24px; }
.settings-section { background: var(--bg-panel); border: 1px solid var(--border); border-radius: 8px; padding: 20px; margin-bottom: 16px; }
.settings-section h3 { font-size: 14px; margin-bottom: 16px; color: var(--accent-gold); }
.setting-row { display: flex; justify-content: space-between; align-items: center; padding: 10px 0; border-bottom: 1px solid rgba(45,58,79,0.3); }
.setting-row:last-child { border-bottom: none; }
.setting-label { font-size: 12px; }
.setting-desc { font-size: 10px; color: var(--text-dim); margin-top: 2px; }

/* ===== 动画 ===== */
@keyframes flash-green { 0%{background:rgba(34,197,94,0.2)} 100%{background:transparent} }
@keyframes flash-red { 0%{background:rgba(239,68,68,0.2)} 100%{background:transparent} }
.flash-up { animation: flash-green 0.5s ease; }
.flash-down { animation: flash-red 0.5s ease; }
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-thumb { background: var(--border-light); border-radius: 3px; }
</style>
</head>
<body>

<!-- 登录页 -->
<div class="login-overlay" id="loginOverlay">
  <div class="login-box">
    <h2>⚡ QUANT MONITOR v6.0</h2>
    <input class="login-input" type="text" id="loginUser" placeholder="用户名" value="admin">
    <input class="login-input" type="password" id="loginPass" placeholder="密码" value="admin123">
    <button class="login-btn" onclick="doLogin()">登录</button>
    <div class="login-error" id="loginError"></div>
  </div>
</div>

<!-- 主应用 -->
<div id="app" style="display:none;height:100vh;flex-direction:column">
  <div class="top-bar">
    <div class="logo"><div class="logo-icon">Q</div> QUANT MONITOR <span style="color:var(--text-dim);font-weight:400">v6.0</span></div>
    <div class="user-info">
      <span id="wsStatus" style="display:flex;align-items:center;gap:4px"><span style="width:6px;height:6px;border-radius:50%;background:var(--accent-up);animation:pulse 2s infinite"></span>实时推送中</span>
      <span id="userDisplay">admin</span>
      <button class="logout-btn" onclick="doLogout()">退出</button>
    </div>
  </div>

  <div class="tab-nav">
    <button class="tab-btn active" onclick="switchTab('market')">市场监控</button>
    <button class="tab-btn" onclick="switchTab('backtest')">策略回测</button>
    <button class="tab-btn" onclick="switchTab('news')">舆情分析</button>
    <button class="tab-btn" onclick="switchTab('settings')">系统设置</button>
  </div>

  <!-- 市场监控 -->
  <div class="tab-content active" id="tab-market">
    <div class="market-grid">
      <div class="ticker-bar">
        <div class="ticker-item"><span class="name">上证指数</span><span class="value" id="t-sh">--</span><span class="change" id="c-sh">--</span></div>
        <div class="ticker-item"><span class="name">深证成指</span><span class="value" id="t-sz">--</span><span class="change" id="c-sz">--</span></div>
        <div class="ticker-item"><span class="name">创业板指</span><span class="value" id="t-cy">--</span><span class="change" id="c-cy">--</span></div>
        <div class="ticker-item"><span class="name">沪深300</span><span class="value" id="t-hs">--</span><span class="change" id="c-hs">--</span></div>
        <div class="ticker-item"><span class="name">北向资金</span><span class="value" style="color:var(--accent-up)" id="t-north">+44.1亿</span></div>
        <div class="ticker-item"><span class="name">市场情绪</span><span class="value" style="color:var(--accent-gold)" id="t-sent">58.5</span></div>
      </div>

      <div class="panel" style="grid-row:2/4">
        <div class="panel-header">多因子监控 <span class="sub">实时评分排序</span></div>
        <div class="panel-body" style="padding:0">
          <table class="data-table"><thead><tr><th>代码</th><th>名称</th><th>价格</th><th>涨跌</th><th>评分</th><th>信号</th></tr></thead><tbody id="stockTbody"></tbody></table>
        </div>
      </div>

      <div class="panel" style="grid-row:2">
        <div class="panel-header">行情深度 <span class="sub" id="depthTitle">选择股票</span></div>
        <div class="panel-body" style="padding:6px 0">
          <div class="depth-row ask-row"><span class="side">卖5</span><span class="price" id="a5">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:30%"></div></div><span class="vol">--</span></div>
          <div class="depth-row ask-row"><span class="side">卖4</span><span class="price" id="a4">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:45%"></div></div><span class="vol">--</span></div>
          <div class="depth-row ask-row"><span class="side">卖3</span><span class="price" id="a3">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:60%"></div></div><span class="vol">--</span></div>
          <div class="depth-row ask-row"><span class="side">卖2</span><span class="price" id="a2">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:75%"></div></div><span class="vol">--</span></div>
          <div class="depth-row ask-row"><span class="side">卖1</span><span class="price" id="a1">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:90%"></div></div><span class="vol">--</span></div>
          <div class="mid-price" id="midPrice">--</div>
          <div class="depth-row bid-row"><span class="side">买1</span><span class="price" id="b1">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:85%"></div></div><span class="vol">--</span></div>
          <div class="depth-row bid-row"><span class="side">买2</span><span class="price" id="b2">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:65%"></div></div><span class="vol">--</span></div>
          <div class="depth-row bid-row"><span class="side">买3</span><span class="price" id="b3">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:50%"></div></div><span class="vol">--</span></div>
          <div class="depth-row bid-row"><span class="side">买4</span><span class="price" id="b4">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:35%"></div></div><span class="vol">--</span></div>
          <div class="depth-row bid-row"><span class="side">买5</span><span class="price" id="b5">--</span><div class="depth-bar-bg"><div class="depth-bar-fill" style="width:20%"></div></div><span class="vol">--</span></div>
          <div style="flex:1;padding:8px"><canvas id="mainChart"></canvas></div>
        </div>
      </div>

      <div class="panel" style="grid-row:2">
        <div class="panel-header">实时异动 <span class="sub">AI 检测</span></div>
        <div class="panel-body" style="padding:0" id="alertStream"></div>
      </div>

      <div class="panel" style="grid-column:2">
        <div class="panel-header">模拟交易 <span class="sub">零风险验证</span></div>
        <div class="panel-body">
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
            <div>
              <div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase">账户概览</div>
              <div style="display:flex;justify-content:space-between;padding:4px 0;font-size:12px"><span style="color:var(--text-dim)">可用资金</span><span style="font-weight:600;color:var(--accent-gold)" id="accCash">--</span></div>
              <div style="display:flex;justify-content:space-between;padding:4px 0;font-size:12px"><span style="color:var(--text-dim)">持仓市值</span><span style="font-weight:600" id="accMkt">--</span></div>
              <div style="display:flex;justify-content:space-between;padding:4px 0;font-size:12px"><span style="color:var(--text-dim)">总权益</span><span style="font-weight:600" id="accTotal">--</span></div>
              <div style="font-size:10px;color:var(--text-dim);margin:10px 0 6px;text-transform:uppercase">最近成交</div>
              <table class="data-table" style="font-size:10px"><thead><tr><th>时间</th><th>方向</th><th>代码</th><th>价格</th></tr></thead><tbody id="tradeHistory"></tbody></table>
            </div>
            <div>
              <div style="font-size:10px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase">下单</div>
              <div class="form-row"><input class="form-input" id="tCode" value="300408.SZ" placeholder="代码"></div>
              <div class="form-row"><input class="form-input" id="tPrice" placeholder="价格" type="number" step="0.01"><input class="form-input" id="tVol" value="100" placeholder="数量" type="number"></div>
              <div class="form-row"><input class="form-input" id="tSL" placeholder="止损价" type="number" step="0.01"><input class="form-input" id="tTP" placeholder="止盈价" type="number" step="0.01"></div>
              <div class="form-row"><button class="form-btn buy" onclick="placeOrder('BUY')">买入</button><button class="form-btn sell" onclick="placeOrder('SELL')">卖出</button></div>
              <div style="font-size:10px;color:var(--text-dim);margin:10px 0 6px;text-transform:uppercase">当前持仓</div>
              <table class="data-table" style="font-size:10px"><thead><tr><th>代码</th><th>持仓</th><th>成本</th><th>现价</th><th>盈亏</th></tr></thead><tbody id="holdingsBody"></tbody></table>
            </div>
          </div>
        </div>
      </div>

      <div class="panel" style="grid-row:3;grid-column:3">
        <div class="panel-header">风控仪表盘 <span class="sub">VaR · 回撤</span></div>
        <div class="panel-body">
          <div class="risk-grid">
            <div class="risk-card"><div class="label">日 VaR (95%)</div><div class="value" style="color:var(--safe)" id="riskVar">--</div></div>
            <div class="risk-card"><div class="label">最大回撤</div><div class="value" style="color:var(--safe)" id="riskDD">--</div></div>
            <div class="risk-card"><div class="label">最大集中度</div><div class="value" style="color:var(--warning)" id="riskConc">--</div></div>
            <div class="risk-card"><div class="label">夏普比率</div><div class="value" style="color:var(--safe)" id="riskSharpe">--</div></div>
          </div>
          <div class="risk-status" id="riskStatus" style="background:rgba(34,197,94,0.1);color:var(--safe);border:1px solid rgba(34,197,94,0.3)">SAFE — 风控正常</div>
        </div>
      </div>
    </div>
  </div>

  <!-- 策略回测 -->
  <div class="tab-content" id="tab-backtest">
    <div class="backtest-layout">
      <div class="backtest-sidebar">
        <div style="font-size:11px;font-weight:700;color:var(--accent-gold);margin-bottom:12px;text-transform:uppercase;letter-spacing:0.5px">策略列表</div>
        <div id="strategyList"></div>
        <div style="margin-top:16px;font-size:11px;font-weight:700;color:var(--accent-gold);text-transform:uppercase;letter-spacing:0.5px">回测参数</div>
        <div style="margin-top:8px">
          <div style="font-size:10px;color:var(--text-dim);margin-bottom:4px">起始日期</div>
          <input class="form-input" type="date" id="btStart" value="2024-01-01" style="margin-bottom:8px">
          <div style="font-size:10px;color:var(--text-dim);margin-bottom:4px">结束日期</div>
          <input class="form-input" type="date" id="btEnd" value="2026-07-31" style="margin-bottom:8px">
          <div style="font-size:10px;color:var(--text-dim);margin-bottom:4px">初始资金</div>
          <input class="form-input" type="number" id="btCapital" value="1000000" style="margin-bottom:12px">
          <button class="form-btn buy" onclick="runBacktest()" style="width:100%">运行回测</button>
        </div>
      </div>
      <div class="backtest-main">
        <div id="btResult" style="display:none">
          <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:20px">
            <div class="risk-card"><div class="label">总收益率</div><div class="value" style="color:var(--accent-up)" id="btReturn">--</div></div>
            <div class="risk-card"><div class="label">夏普比率</div><div class="value" style="color:var(--accent-blue)" id="btSharpe">--</div></div>
            <div class="risk-card"><div class="label">最大回撤</div><div class="value" style="color:var(--accent-down)" id="btDD">--</div></div>
            <div class="risk-card"><div class="label">胜率</div><div class="value" style="color:var(--accent-gold)" id="btWin">--</div></div>
            <div class="risk-card"><div class="label">交易次数</div><div class="value" id="btTrades">--</div></div>
          </div>
          <div style="height:300px"><canvas id="btChart"></canvas></div>
        </div>
        <div id="btEmpty" style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-dim);font-size:14px">
          选择策略并配置参数，点击"运行回测"查看结果
        </div>
      </div>
    </div>
  </div>

  <!-- 舆情分析 -->
  <div class="tab-content" id="tab-news">
    <div class="news-layout">
      <div class="panel" style="border:none;border-radius:0">
        <div class="panel-header">实时舆情 <span class="sub">NLP 情感分析</span></div>
        <div class="panel-body" style="padding:0" id="newsList"></div>
      </div>
      <div style="background:var(--bg-secondary);padding:16px">
        <div class="sentiment-gauge">
          <div style="font-size:11px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase">市场情绪指数</div>
          <div class="sentiment-value" style="color:var(--accent-gold)" id="sentValue">58.5</div>
          <div style="font-size:12px;color:var(--text-secondary);margin-top:4px" id="sentTrend">趋势：升温</div>
        </div>
        <div style="margin-top:24px">
          <div style="font-size:11px;color:var(--text-dim);margin-bottom:8px;text-transform:uppercase">情绪分布</div>
          <div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px"><span>散户</span><span id="sentRetail">52.0</span></div><div style="height:4px;background:var(--bg-hover);border-radius:2px"><div style="width:52%;height:100%;background:var(--accent-blue);border-radius:2px"></div></div></div>
          <div style="margin-bottom:8px"><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px"><span>机构</span><span id="sentInst">61.0</span></div><div style="height:4px;background:var(--bg-hover);border-radius:2px"><div style="width:61%;height:100%;background:var(--accent-up);border-radius:2px"></div></div></div>
          <div><div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px"><span>外资</span><span id="sentForeign">48.0</span></div><div style="height:4px;background:var(--bg-hover);border-radius:2px"><div style="width:48%;height:100%;background:var(--accent-purple);border-radius:2px"></div></div></div>
        </div>
        <div style="margin-top:24px;height:200px"><canvas id="sentChart"></canvas></div>
      </div>
    </div>
  </div>

  <!-- 系统设置 -->
  <div class="tab-content" id="tab-settings">
    <div class="settings-layout">
      <div class="settings-section">
        <h3>数据源配置</h3>
        <div class="setting-row">
          <div><div class="setting-label">数据来源</div><div class="setting-desc">当前使用模拟数据，可切换至 AkShare 或 Tushare 真实行情</div></div>
          <select class="form-input" style="width:140px;flex:none" id="settingDataSource"><option value="mock">模拟数据 (Mock)</option><option value="akshare">AkShare</option><option value="tushare">Tushare</option></select>
        </div>
        <div class="setting-row">
          <div><div class="setting-label">推送频率</div><div class="setting-desc">WebSocket 数据刷新间隔（秒）</div></div>
          <select class="form-input" style="width:140px;flex:none"><option>1秒</option><option selected>2秒</option><option>5秒</option></select>
        </div>
      </div>
      <div class="settings-section">
        <h3>告警配置</h3>
        <div class="setting-row">
          <div><div class="setting-label">Webhook URL</div><div class="setting-desc">钉钉 / 企业微信 / 飞书机器人地址</div></div>
          <input class="form-input" style="width:300px;flex:none" placeholder="https://oapi.dingtalk.com/robot/send?access_token=...">
        </div>
        <div class="setting-row">
          <div><div class="setting-label">异动阈值</div><div class="setting-desc">涨跌幅超过此值触发告警</div></div>
          <select class="form-input" style="width:140px;flex:none"><option>3%</option><option selected>5%</option><option>7%</option><option>涨停/跌停</option></select>
        </div>
      </div>
      <div class="settings-section">
        <h3>交易设置</h3>
        <div class="setting-row">
          <div><div class="setting-label">默认手续费率</div><div class="setting-desc">买入/卖出交易手续费</div></div>
          <input class="form-input" style="width:100px;flex:none" value="0.03%">
        </div>
        <div class="setting-row">
          <div><div class="setting-label">滑点设置</div><div class="setting-desc">模拟成交价格滑点</div></div>
          <input class="form-input" style="width:100px;flex:none" value="0.01%">
        </div>
      </div>
    </div>
  </div>
</div>

<script>
let token = localStorage.getItem('quant_token') || '';
let socket = null;
let selectedCode = '300408.SZ';
let priceChart = null;
let btChart = null;
let sentChart = null;
let strategies = [];
let selectedStrategy = 0;

// ===== 认证 =====
function doLogin() {
  const user = document.getElementById('loginUser').value;
  const pass = document.getElementById('loginPass').value;
  fetch('/api/auth/login', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({username: user, password: pass})
  }).then(r => r.json()).then(d => {
    if (d.success) {
      token = d.token;
      localStorage.setItem('quant_token', token);
      document.getElementById('loginOverlay').style.display = 'none';
      document.getElementById('app').style.display = 'flex';
      document.getElementById('userDisplay').textContent = d.username;
      initApp();
    } else {
      document.getElementById('loginError').textContent = d.msg;
    }
  });
}
function doLogout() {
  localStorage.removeItem('quant_token');
  location.reload();
}

// 自动登录
if (token) {
  document.getElementById('loginOverlay').style.display = 'none';
  document.getElementById('app').style.display = 'flex';
  initApp();
}

function initApp() {
  socket = io();
  socket.on('connect', () => { document.getElementById('wsStatus').innerHTML = '<span style="width:6px;height:6px;border-radius:50%;background:var(--accent-up);animation:pulse 2s infinite;display:inline-block;margin-right:4px"></span>实时推送中'; });
  socket.on('disconnect', () => { document.getElementById('wsStatus').innerHTML = '<span style="color:var(--accent-down)">● 已断开</span>'; });
  socket.on('market_tick', (data) => {
    updateIndices(data.indices);
    updateStockTable(data.stocks);
    updateDepth(data.stocks);
    updateChart(data.stocks);
  });
  socket.on('alert', (data) => { data.alerts.forEach(a => addAlert(a)); });

  loadStrategies();
  loadNews();
  loadSentiment();
  updateAccount();
  updateRisk();
  setInterval(() => { updateAccount(); updateRisk(); }, 3000);
}

// ===== 标签切换 =====
function switchTab(tab) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + tab).classList.add('active');
  event.target.classList.add('active');
  if (tab === 'backtest' && !btChart) setTimeout(initBtChart, 100);
  if (tab === 'news' && !sentChart) setTimeout(initSentChart, 100);
}

// ===== 市场数据 =====
function updateIndices(indices) {
  for (const [key, d] of Object.entries(indices)) {
    const el = document.getElementById('t-' + key);
    const ch = document.getElementById('c-' + key);
    if (el) el.textContent = d.price.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
    if (ch) {
      ch.textContent = (d.change_pct > 0 ? '+' : '') + d.change_pct + '%';
      ch.className = 'change ' + (d.change_pct >= 0 ? 'change-up' : 'change-down');
    }
  }
}

function updateStockTable(stocks) {
  const tbody = document.getElementById('stockTbody');
  const existing = {};
  tbody.querySelectorAll('tr').forEach(tr => { existing[tr.dataset.code] = tr; });
  stocks.forEach(s => {
    const rating = s.factor.rating;
    const signal = s.factor.alpha_signal;
    let tr = existing[s.code];
    const isUp = s.change_pct >= 0;
    const html = `<td class="code">${s.code}</td><td class="name">${s.name}</td><td style="font-weight:600">${s.price.toFixed(2)}</td><td style="color:${isUp?'var(--accent-up)':'var(--accent-down)'};font-weight:600">${isUp?'+':''}${s.change_pct}%</td><td><span style="display:inline-block;padding:1px 5px;border-radius:2px;font-size:10px;font-weight:700;border:1px solid ${rating.color}40;color:${rating.color};background:${rating.color}15">${s.factor.total} ${rating.level}</span></td><td><span style="font-size:10px;font-weight:600;padding:1px 5px;border-radius:2px;color:${signal.action==='BUY'?'var(--accent-up)':signal.action==='SELL'?'var(--accent-down)':'var(--text-dim)'};background:${signal.action==='BUY'?'rgba(34,197,94,0.1)':signal.action==='SELL'?'rgba(239,68,68,0.1)':'rgba(100,116,139,0.1)'};border:1px solid ${signal.action==='BUY'?'rgba(34,197,94,0.3)':signal.action==='SELL'?'rgba(239,68,68,0.3)':'rgba(100,116,139,0.3)'}">${signal.action}</span></td>`;
    if (tr) {
      const oldPrice = parseFloat(tr.children[2].textContent);
      tr.innerHTML = html;
      if (s.price > oldPrice) { tr.classList.add('flash-up'); setTimeout(()=>tr.classList.remove('flash-up'), 500); }
      else if (s.price < oldPrice) { tr.classList.add('flash-down'); setTimeout(()=>tr.classList.remove('flash-down'), 500); }
    } else {
      tr = document.createElement('tr');
      tr.dataset.code = s.code;
      tr.innerHTML = html;
      tr.onclick = () => { selectedCode = s.code; updateDepth(stocks); updateChart(stocks); updateStockTable(stocks); };
      tbody.appendChild(tr);
    }
  });
  tbody.querySelectorAll('tr').forEach(tr => { tr.classList.toggle('selected', tr.dataset.code === selectedCode); });
}

function updateDepth(stocks) {
  const s = stocks.find(x => x.code === selectedCode);
  if (!s) return;
  document.getElementById('depthTitle').textContent = s.name + ' ' + s.code;
  document.getElementById('midPrice').textContent = s.price.toFixed(2);
  const vols = [6230, 4890, 3560, 2180, 1240, 5780, 3920, 2670, 1840, 980];
  for (let i = 1; i <= 5; i++) {
    document.getElementById('a' + i).textContent = (s.price + i * 0.12).toFixed(2);
    document.getElementById('b' + i).textContent = (s.price - i * 0.11).toFixed(2);
  }
}

function updateChart(stocks) {
  const s = stocks.find(x => x.code === selectedCode);
  if (!s || !s.history) return;
  const prices = s.history.map(h => h.price);
  if (!priceChart) {
    const ctx = document.getElementById('mainChart').getContext('2d');
    priceChart = new Chart(ctx, {
      type: 'line',
      data: { labels: prices.map((_,i)=>i), datasets: [{ data: prices, borderColor: '#3b82f6', backgroundColor: (ctx) => { const g = ctx.chart.ctx.createLinearGradient(0,0,0,200); g.addColorStop(0,'rgba(59,130,246,0.2)'); g.addColorStop(1,'rgba(59,130,246,0)'); return g; }, fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5 }] },
      options: { responsive: true, maintainAspectRatio: false, plugins: { legend: {display:false} }, scales: { x: {display:false}, y: {grid:{color:'#1a2236'},ticks:{color:'#64748b',font:{size:10}},position:'right'} }, animation: {duration:0} }
    });
  } else {
    priceChart.data.datasets[0].data = prices;
    priceChart.update('none');
  }
}

function addAlert(alert) {
  const panel = document.getElementById('alertStream');
  const div = document.createElement('div');
  const levelClass = alert.level === 'critical' ? 'critical' : alert.level === 'high' ? 'high' : '';
  div.className = 'alert-item ' + levelClass;
  div.innerHTML = `<div class="alert-header"><span class="alert-name">${alert.name} <span style="color:var(--text-dim)">${alert.code}</span></span><span class="alert-time">${alert.time}</span></div><div class="alert-badges">${alert.anomalies.map(a => `<span class="alert-badge">${a.icon} ${a.text}</span>`).join('')}</div><div style="margin-top:2px;font-size:10px;color:var(--text-dim)">${alert.price.toFixed(2)} <span style="color:${alert.change_pct>=0?'var(--accent-up)':'var(--accent-down)'}">${alert.change_pct>=0?'+':''}${alert.change_pct}%</span></div>`;
  panel.insertBefore(div, panel.firstChild);
  if (panel.children.length > 30) panel.lastChild.remove();
}

// ===== 交易 =====
function placeOrder(action) {
  const code = document.getElementById('tCode').value;
  const price = parseFloat(document.getElementById('tPrice').value);
  const volume = parseInt(document.getElementById('tVol').value);
  const sl = document.getElementById('tSL').value ? parseFloat(document.getElementById('tSL').value) : null;
  const tp = document.getElementById('tTP').value ? parseFloat(document.getElementById('tTP').value) : null;
  if (!price || !volume) { alert('请输入价格和数量'); return; }
  fetch('/api/trade/' + (action === 'BUY' ? 'buy' : 'sell'), {
    method: 'POST', headers: {'Content-Type':'application/json', 'Authorization': 'Bearer ' + token},
    body: JSON.stringify({code, price, volume, stop_loss: sl, take_profit: tp})
  }).then(r => r.json()).then(d => {
    if (d.success) { alert(d.msg); updateAccount(); }
    else alert('交易失败: ' + d.msg);
  });
}

function updateAccount() {
  fetch('/api/account/info', {headers: {'Authorization': 'Bearer ' + token}}).then(r => r.json()).then(d => {
    if (d.success) {
      document.getElementById('accCash').textContent = '\u00a5' + d.data.cash.toLocaleString('en-US',{minimumFractionDigits:2});
      document.getElementById('accMkt').textContent = '\u00a5' + d.data.market_value.toLocaleString('en-US',{minimumFractionDigits:2});
      document.getElementById('accTotal').textContent = '\u00a5' + d.data.total.toLocaleString('en-US',{minimumFractionDigits:2});
      const tbody = document.getElementById('tradeHistory');
      tbody.innerHTML = d.data.trades.slice(0,5).map(t => `<tr><td>${new Date(t.created_at).toLocaleTimeString('zh-CN',{hour12:false,hour:'2-digit',minute:'2-digit'})}</td><td style="color:${t.action==='BUY'?'var(--accent-up)':'var(--accent-down)'}">${t.action==='BUY'?'买入':'卖出'}</td><td>${t.code}</td><td>${t.price.toFixed(2)}</td></tr>`).join('');
      const hbody = document.getElementById('holdingsBody');
      hbody.innerHTML = d.data.holdings.map(h => `<tr><td>${h.code}</td><td>${h.volume}</td><td>${h.avg_cost.toFixed(2)}</td><td>${h.market_price.toFixed(2)}</td><td style="color:${h.pnl_pct>=0?'var(--accent-up)':'var(--accent-down)'}">${h.pnl_pct>=0?'+':''}${h.pnl_pct}%</td></tr>`).join('') || '<tr><td colspan="5" style="color:var(--text-dim);text-align:center">暂无持仓</td></tr>';
    }
  });
}

function updateRisk() {
  fetch('/api/risk/report', {headers: {'Authorization': 'Bearer ' + token}}).then(r => r.json()).then(d => {
    if (d.success) {
      document.getElementById('riskVar').textContent = '\u00a5' + d.data.var_95.toLocaleString();
      document.getElementById('riskDD').textContent = d.data.drawdown + '%';
      document.getElementById('riskConc').textContent = d.data.max_concentration + '%';
      const rs = document.getElementById('riskStatus');
      rs.textContent = d.data.status + ' \u2014 ' + (d.data.status==='SAFE'?'风控正常':d.data.status==='WARNING'?'注意回撤':'触发风控线');
      rs.style.background = d.data.status==='SAFE'?'rgba(34,197,94,0.1)':d.data.status==='WARNING'?'rgba(245,158,11,0.1)':'rgba(220,38,38,0.1)';
      rs.style.color = d.data.status==='SAFE'?'var(--safe)':d.data.status==='WARNING'?'var(--warning)':'var(--danger)';
      rs.style.borderColor = d.data.status==='SAFE'?'rgba(34,197,94,0.3)':d.data.status==='WARNING'?'rgba(245,158,11,0.3)':'rgba(220,38,38,0.3)';
    }
  });
}

// ===== 回测 =====
function loadStrategies() {
  fetch('/api/backtest/strategies').then(r => r.json()).then(d => {
    if (d.success) {
      strategies = d.data;
      const list = document.getElementById('strategyList');
      list.innerHTML = strategies.map((s, i) => `
        <div class="strategy-card ${i===0?'active':''}" onclick="selectStrategy(${i})" data-idx="${i}">
          <h4>${s.name}</h4>
          <div class="metrics"><span style="color:var(--accent-up)">+${s.return}%</span><span>夏普 ${s.sharpe}</span><span>回撤 ${s.max_dd}%</span></div>
        </div>
      `).join('');
    }
  });
}
function selectStrategy(idx) {
  selectedStrategy = idx;
  document.querySelectorAll('.strategy-card').forEach(c => c.classList.remove('active'));
  document.querySelector(`.strategy-card[data-idx="${idx}"]`).classList.add('active');
}
function runBacktest() {
  const strategy = strategies[selectedStrategy].name;
  const start = document.getElementById('btStart').value;
  const end = document.getElementById('btEnd').value;
  const capital = parseInt(document.getElementById('btCapital').value);
  fetch('/api/backtest/run', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({strategy, start_date: start, end_date: end, initial_capital: capital})
  }).then(r => r.json()).then(d => {
    if (d.success) {
      document.getElementById('btEmpty').style.display = 'none';
      document.getElementById('btResult').style.display = 'block';
      document.getElementById('btReturn').textContent = '+' + d.data.total_return + '%';
      document.getElementById('btSharpe').textContent = d.data.sharpe;
      document.getElementById('btDD').textContent = d.data.max_dd + '%';
      document.getElementById('btWin').textContent = d.data.win_rate + '%';
      document.getElementById('btTrades').textContent = d.data.trades;
      // 加载权益曲线
      fetch('/api/backtest/equity?strategy_index=' + selectedStrategy).then(r => r.json()).then(ed => {
        if (ed.success && btChart) {
          btChart.data.labels = ed.data.dates;
          btChart.data.datasets[0].data = ed.data.values;
          btChart.update();
        }
      });
    }
  });
}
function initBtChart() {
  const ctx = document.getElementById('btChart').getContext('2d');
  btChart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [{ label: '权益曲线', data: [], borderColor: '#22c55e', backgroundColor: 'rgba(34,197,94,0.1)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5 }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: {display:false} }, scales: { x: {ticks:{color:'#64748b',font:{size:10}},grid:{color:'#1a2236'}}, y: {ticks:{color:'#64748b',font:{size:10}},grid:{color:'#1a2236'}} } }
  });
}

// ===== 舆情 =====
function loadNews() {
  fetch('/api/news/all').then(r => r.json()).then(d => {
    if (d.success) {
      const list = document.getElementById('newsList');
      list.innerHTML = d.data.map(n => `
        <div class="news-item">
          <div class="header"><span class="source">${n.source} · ${n.time}</span><span class="cred" style="background:${n.credibility>=4?'rgba(34,197,94,0.15)':n.credibility>=3?'rgba(245,158,11,0.15)':'rgba(239,68,68,0.15)'};color:${n.credibility>=4?'var(--accent-up)':n.credibility>=3?'var(--accent-gold)':'var(--accent-down)'}">${'★'.repeat(n.credibility)}${'☆'.repeat(5-n.credibility)}</span></div>
          <div class="title">${n.title}</div>
          <div class="impact">${n.impact}</div>
        </div>
      `).join('');
    }
  });
}
function loadSentiment() {
  fetch('/api/sentiment/index').then(r => r.json()).then(d => {
    if (d.success) {
      document.getElementById('sentValue').textContent = d.data.overall;
      document.getElementById('sentTrend').textContent = '趋势：' + d.data.trend;
      document.getElementById('sentRetail').textContent = d.data.retail;
      document.getElementById('sentInst').textContent = d.data.institution;
      document.getElementById('sentForeign').textContent = d.data.foreign;
      if (sentChart) {
        sentChart.data.labels = d.history.dates;
        sentChart.data.datasets[0].data = d.history.values;
        sentChart.update();
      }
    }
  });
}
function initSentChart() {
  const ctx = document.getElementById('sentChart').getContext('2d');
  sentChart = new Chart(ctx, {
    type: 'line',
    data: { labels: [], datasets: [{ data: [], borderColor: '#f59e0b', backgroundColor: 'rgba(245,158,11,0.1)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 1.5 }] },
    options: { responsive: true, maintainAspectRatio: false, plugins: { legend: {display:false} }, scales: { x: {display:false}, y: {grid:{color:'#1a2236'},ticks:{color:'#64748b',font:{size:10}}} } }
  });
  loadSentiment();
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    socketio.start_background_task(broadcast_market_data)
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
