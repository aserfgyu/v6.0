"""
量化机构级市场异动监控系统
部署到Render: 替换你现有的app.py
"""

import os
import json
import time
import asyncio
import aiohttp
import requests
from datetime import datetime, timedelta
from functools import lru_cache
from flask import Flask, jsonify, render_template_string
from flask_cors import CORS
import numpy as np

app = Flask(__name__)
CORS(app)

# ============ 配置 ============
CACHE_TTL = 30  # 缓存30秒
DATA_SOURCES = {
    'sina': 'https://hq.sinajs.cn/list={}',
    'tencent': 'https://qt.gtimg.cn/q={}',
    'eastmoney': 'https://push2.eastmoney.com/api/qt/stock/get?secid={}&fields=f43,f44,f45,f46,f47,f48,f57,f58,f60,f170'
}

# 股票池：重点监控的标的（可扩展）
WATCHLIST = {
    # MLCC涨价链
    '000636.SZ': {'name': '风华高科', 'sector': 'MLCC', 'theme': 'MLCC涨价'},
    '300408.SZ': {'name': '三环集团', 'sector': 'MLCC', 'theme': 'MLCC涨价'},
    # 半导体设备
    '002371.SZ': {'name': '北方华创', 'sector': '半导体设备', 'theme': '国产替代'},
    '688981.SS': {'name': '中芯国际', 'sector': '晶圆代工', 'theme': '大基金三期'},
    # 军工
    '000768.SZ': {'name': '中航西飞', 'sector': '军工', 'theme': 'C919大飞机'},
    # 可扩展...
}

# ============ 数据采集引擎 ============

class DataEngine:
    """多源数据采集引擎，带故障转移"""
    
    def __init__(self):
        self.cache = {}
        self.cache_time = {}
    
    async def fetch_with_fallback(self, codes):
        """多源获取，失败自动切换"""
        for source_name, url_template in DATA_SOURCES.items():
            try:
                data = await self._fetch_from_source(source_name, url_template, codes)
                if data:
                    return data
            except Exception as e:
                print(f"[WARN] {source_name} failed: {e}")
                continue
        return {}
    
    async def _fetch_from_source(self, source, url_template, codes):
        async with aiohttp.ClientSession() as session:
            if source == 'sina':
                # 新浪接口
                url = url_template.format(','.join(codes))
                async with session.get(url, timeout=10) as resp:
                    text = await resp.text()
                    return self._parse_sina(text)
            elif source == 'tencent':
                # 腾讯接口
                url = url_template.format(','.join(codes))
                async with session.get(url, timeout=10) as resp:
                    text = await resp.text()
                    return self._parse_tencent(text)
        return {}
    
    def _parse_sina(self, text):
        """解析新浪行情数据"""
        result = {}
        for line in text.strip().split('\n'):
            if not line or '=' not in line:
                continue
            code = line.split('=')[0].split('_')[-1]
            data_str = line.split('=')[1].strip().strip('"')
            if not data_str:
                continue
            parts = data_str.split(',')
            if len(parts) < 33:
                continue
            result[code] = {
                'name': parts[0],
                'open': float(parts[1]),
                'close': float(parts[2]),
                'price': float(parts[3]),
                'high': float(parts[4]),
                'low': float(parts[5]),
                'volume': int(parts[8]),
                'amount': float(parts[9]),
                'pre_close': float(parts[2]),
                'change_pct': round((float(parts[3]) - float(parts[2])) / float(parts[2]) * 100, 2) if float(parts[2]) > 0 else 0,
                'bid1': float(parts[11]),
                'ask1': float(parts[21]),
                'timestamp': datetime.now().isoformat()
            }
        return result
    
    def _parse_tencent(self, text):
        """解析腾讯行情数据"""
        result = {}
        for line in text.strip().split(';'):
            if not line or '=' not in line:
                continue
            code = line.split('=')[0].split('_')[-1]
            data = line.split('=')[1].strip().strip('"').split('~')
            if len(data) < 45:
                continue
            result[code] = {
                'name': data[1],
                'price': float(data[3]),
                'pre_close': float(data[4]),
                'open': float(data[5]),
                'volume': int(data[6]),
                'amount': float(data[37]),
                'high': float(data[33]),
                'low': float(data[34]),
                'change_pct': float(data[32]),
                'bid1': float(data[9]),
                'ask1': float(data[19]),
                'timestamp': datetime.now().isoformat()
            }
        return result

engine = DataEngine()

# ============ 多因子评分引擎 ============

class FactorEngine:
    """量化多因子评分系统"""
    
    def __init__(self):
        self.price_history = {}  # 价格历史缓存
    
    def calculate_factors(self, code, data):
        """计算多因子评分"""
        price = data['price']
        pre_close = data['pre_close']
        change_pct = data['change_pct']
        volume = data['volume']
        
        # 更新历史数据
        if code not in self.price_history:
            self.price_history[code] = []
        self.price_history[code].append({
            'price': price,
            'volume': volume,
            'time': datetime.now()
        })
        # 保留最近100条
        self.price_history[code] = self.price_history[code][-100:]
        
        history = [h['price'] for h in self.price_history[code]]
        vol_history = [h['volume'] for h in self.price_history[code]]
        
        scores = {}
        
        # 1. 技术面因子 (30分)
        scores['technical'] = self._technical_score(history, vol_history, change_pct)
        
        # 2. 动量因子 (20分)
        scores['momentum'] = self._momentum_score(history, change_pct)
        
        # 3. 资金面因子 (20分)
        scores['capital'] = self._capital_score(volume, vol_history, data)
        
        # 4. 波动率因子 (15分)
        scores['volatility'] = self._volatility_score(history)
        
        # 5. 流动性因子 (15分)
        scores['liquidity'] = self._liquidity_score(volume, data)
        
        total = sum(scores.values())
        return {
            'total': round(total, 1),
            'breakdown': {k: round(v, 1) for k, v in scores.items()},
            'rating': self._get_rating(total)
        }
    
    def _technical_score(self, history, vol_history, change_pct):
        """技术面评分：RSI + MACD + 均线"""
        if len(history) < 20:
            return 15
        
        # RSI计算
        rsi = self._calc_rsi(history)
        rsi_score = 0
        if rsi < 30:
            rsi_score = 10  # 超卖，反弹概率高
        elif rsi > 70:
            rsi_score = 2   # 超买
        elif 40 <= rsi <= 60:
            rsi_score = 7   # 中性偏强
        else:
            rsi_score = 5
        
        # 均线趋势
        ma5 = np.mean(history[-5:]) if len(history) >= 5 else history[-1]
        ma10 = np.mean(history[-10:]) if len(history) >= 10 else history[-1]
        ma20 = np.mean(history[-20:]) if len(history) >= 20 else history[-1]
        
        ma_score = 0
        if history[-1] > ma5 > ma10 > ma20:
            ma_score = 10  # 多头排列
        elif history[-1] < ma5 < ma10 < ma20:
            ma_score = 2   # 空头排列
        else:
            ma_score = 5
        
        # 量价配合
        vol_score = 0
        if len(vol_history) >= 5:
            avg_vol = np.mean(vol_history[-5:])
            if vol_history[-1] > avg_vol * 2 and change_pct > 0:
                vol_score = 10  # 放量上涨
            elif vol_history[-1] > avg_vol * 2 and change_pct < 0:
                vol_score = 3   # 放量下跌
            elif vol_history[-1] > avg_vol * 1.5:
                vol_score = 6
            else:
                vol_score = 4
        
        # 技术面总分 = RSI(10) + 均线(10) + 量价(10)
        return min(30, rsi_score + ma_score + vol_score)
    
    def _momentum_score(self, history, change_pct):
        """动量评分"""
        if len(history) < 5:
            return 10
        
        # 短期趋势
        short_trend = (history[-1] - history[-5]) / history[-5] * 100 if history[-5] > 0 else 0
        
        score = 0
        if change_pct > 5:
            score = 20  # 强势涨停附近
        elif change_pct > 3:
            score = 16
        elif change_pct > 0:
            score = 12
        elif change_pct > -3:
            score = 8
        else:
            score = 4
        
        # 趋势加速加分
        if short_trend > 10 and change_pct > 0:
            score += 5
        
        return min(20, score)
    
    def _capital_score(self, volume, vol_history, data):
        """资金面评分"""
        if len(vol_history) < 5:
            return 10
        
        avg_vol = np.mean(vol_history[-5:])
        vol_ratio = volume / avg_vol if avg_vol > 0 else 1
        
        score = 0
        if vol_ratio > 5:
            score = 20  # 巨量异动
        elif vol_ratio > 3:
            score = 16
        elif vol_ratio > 1.5:
            score = 12
        elif vol_ratio > 0.8:
            score = 8
        else:
            score = 4
        
        # 盘口资金
        bid_ask_spread = (data.get('ask1', 0) - data.get('bid1', 0)) / data.get('price', 1) * 100
        if bid_ask_spread < 0.1:
            score += 3  # 盘口紧密，流动性好
        
        return min(20, score)
    
    def _volatility_score(self, history):
        """波动率评分：适中波动最好"""
        if len(history) < 10:
            return 7
        
        returns = [(history[i] - history[i-1]) / history[i-1] * 100 
                   for i in range(1, len(history)) if history[i-1] > 0]
        if not returns:
            return 7
        
        volatility = np.std(returns)
        
        # 波动率适中最好（3-8%），太高太低都不好
        if 3 <= volatility <= 8:
            return 15
        elif 1 <= volatility < 3:
            return 10
        elif 8 < volatility <= 15:
            return 10
        else:
            return 5
    
    def _liquidity_score(self, volume, data):
        """流动性评分"""
        amount = data.get('amount', 0)
        
        score = 0
        if amount > 10_0000_0000:  # 10亿
            score = 15
        elif amount > 5_0000_0000:  # 5亿
            score = 12
        elif amount > 1_0000_0000:  # 1亿
            score = 9
        elif amount > 5000_0000:  # 5000万
            score = 6
        else:
            score = 3
        
        return score
    
    def _calc_rsi(self, prices, period=14):
        """计算RSI"""
        if len(prices) < period + 1:
            return 50
        
        deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas[-period:]]
        losses = [-d if d < 0 else 0 for d in deltas[-period:]]
        
        avg_gain = np.mean(gains) if gains else 0
        avg_loss = np.mean(losses) if losses else 0
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))
    
    def _get_rating(self, score):
        """评级"""
        if score >= 80:
            return {'level': 'S', 'text': '强烈看多', 'color': '#10b981'}
        elif score >= 65:
            return {'level': 'A', 'text': '看多', 'color': '#34d399'}
        elif score >= 50:
            return {'level': 'B', 'text': '中性偏多', 'color': '#f59e0b'}
        elif score >= 35:
            return {'level': 'C', 'text': '中性偏空', 'color': '#f97316'}
        else:
            return {'level': 'D', 'text': '看空', 'color': '#ef4444'}

factor_engine = FactorEngine()

# ============ 异动检测引擎 ============

class AnomalyDetector:
    """异动检测：涨停、跌停、放量、急拉、跳水"""
    
    def detect(self, code, data, history):
        anomalies = []
        change_pct = data.get('change_pct', 0)
        price = data.get('price', 0)
        volume = data.get('volume', 0)
        
        # 涨停检测
        if change_pct >= 9.5:
            anomalies.append({
                'type': 'limit_up',
                'level': 'critical',
                'text': f'🔥 涨停 {change_pct}%',
                'icon': '🔥'
            })
        elif change_pct >= 5:
            anomalies.append({
                'type': 'surge',
                'level': 'high',
                'text': f'📈 急拉 {change_pct}%',
                'icon': '📈'
            })
        
        # 跌停检测
        if change_pct <= -9.5:
            anomalies.append({
                'type': 'limit_down',
                'level': 'critical',
                'text': f'💥 跌停 {change_pct}%',
                'icon': '💥'
            })
        elif change_pct <= -5:
            anomalies.append({
                'type': 'plunge',
                'level': 'high',
                'text': f'📉 跳水 {change_pct}%',
                'icon': '📉'
            })
        
        # 放量检测
        if history and len(history) >= 5:
            avg_vol = np.mean([h['volume'] for h in history[-5:]])
            if avg_vol > 0:
                vol_ratio = volume / avg_vol
                if vol_ratio > 5:
                    anomalies.append({
                        'type': 'volume_spike',
                        'level': 'high',
                        'text': f'⚡ 放量 {vol_ratio:.1f}倍',
                        'icon': '⚡'
                    })
                elif vol_ratio > 3:
                    anomalies.append({
                        'type': 'volume_increase',
                        'level': 'medium',
                        'text': f'📊 放量 {vol_ratio:.1f}倍',
                        'icon': '📊'
                    })
        
        # 新高/新低
        if history and len(history) >= 20:
            prices = [h['price'] for h in history]
            if price >= max(prices):
                anomalies.append({
                    'type': 'new_high',
                    'level': 'medium',
                    'text': '🏔️ 20日新高',
                    'icon': '🏔️'
                })
            elif price <= min(prices):
                anomalies.append({
                    'type': 'new_low',
                    'level': 'medium',
                    'text': '🕳️ 20日新低',
                    'icon': '🕳️'
                })
        
        return anomalies

detector = AnomalyDetector()

# ============ 龙头推荐引擎 ============

class LeaderEngine:
    """板块龙头推荐"""
    
    def recommend(self, stocks_data):
        """推荐龙头"""
        recommendations = []
        
        # 按板块分组
        sectors = {}
        for code, info in stocks_data.items():
            sector = WATCHLIST.get(code, {}).get('sector', '其他')
            if sector not in sectors:
                sectors[sector] = []
            sectors[sector].append({**info, 'code': code})
        
        for sector, stocks in sectors.items():
            if not stocks:
                continue
            
            # 排序：评分高 > 涨幅大 > 成交量大
            stocks_sorted = sorted(stocks, key=lambda x: (
                x.get('factor_score', {}).get('total', 0),
                x.get('change_pct', 0),
                x.get('amount', 0)
            ), reverse=True)
            
            leader = stocks_sorted[0]
            recommendations.append({
                'sector': sector,
                'theme': WATCHLIST.get(leader['code'], {}).get('theme', ''),
                'leader': {
                    'code': leader['code'],
                    'name': leader['name'],
                    'price': leader['price'],
                    'change_pct': leader['change_pct'],
                    'score': leader.get('factor_score', {}).get('total', 0),
                    'rating': leader.get('factor_score', {}).get('rating', {})
                },
                'followers': [
                    {'code': s['code'], 'name': s['name'], 'change_pct': s['change_pct']}
                    for s in stocks_sorted[1:3]
                ],
                'sector_avg_change': round(np.mean([s['change_pct'] for s in stocks]), 2)
            })
        
        # 按板块平均涨幅排序
        recommendations.sort(key=lambda x: x['sector_avg_change'], reverse=True)
        return recommendations

leader_engine = LeaderEngine()

# ============ 消息/舆情引擎 ============

class NewsEngine:
    """异动消息与小作文监控"""
    
    def get_news(self):
        """获取模拟的异动消息（实际可接入爬虫）"""
        # 实际部署时，这里应该接入：
        # 1. 东方财富股吧爬虫
        # 2. 雪球热帖监控
        # 3. 微信群消息监听（企业微信机器人）
        # 4. 财经新闻API
        
        return [
            {
                'id': 1,
                'time': '14:32',
                'source': '产业新闻',
                'credibility': 5,  # 1-5星
                'type': 'hot',
                'title': '三星电机计划8月起对MLCC涨价10%-25%',
                'stocks': ['000636.SZ', '300408.SZ'],
                'impact': 'MLCC涨价周期开启，国内厂商受益',
                'sentiment': 'positive'
            },
            {
                'id': 2,
                'time': '13:15',
                'source': '小作文/传闻',
                'credibility': 2,
                'type': 'rumor',
                'title': '北方华创Q3订单超预期，机构调仓明显',
                'stocks': ['002371.SZ'],
                'impact': '若属实，设备龙头业绩将上修',
                'sentiment': 'positive'
            },
            {
                'id': 3,
                'time': '10:48',
                'source': '公司公告',
                'credibility': 5,
                'type': 'news',
                'title': '中航西飞承接C919关键部件制造，新机型订单放量',
                'stocks': ['000768.SZ'],
                'impact': '军工龙头中长期业绩确定性提升',
                'sentiment': 'positive'
            },
            {
                'id': 4,
                'time': '09:22',
                'source': '政策',
                'credibility': 4,
                'type': 'policy',
                'title': '中芯国际获大基金三期增资支持',
                'stocks': ['688981.SS'],
                'impact': '先进制程扩产加速，但估值偏高',
                'sentiment': 'neutral'
            },
            {
                'id': 5,
                'time': '11:05',
                'source': '小作文/传闻',
                'credibility': 2,
                'type': 'rumor',
                'title': '三环集团MLCC产能利用率回升至85%，大客户追单',
                'stocks': ['300408.SZ'],
                'impact': '毛利率修复，Q2业绩有望超预期',
                'sentiment': 'positive'
            },
            {
                'id': 6,
                'time': '15:20',
                'source': '公司公告',
                'credibility': 5,
                'type': 'hot',
                'title': '风华高科电子级高纯红磷项目投产，切入半导体材料',
                'stocks': ['000636.SZ'],
                'impact': '估值重构，从周期股转向成长股',
                'sentiment': 'positive'
            }
        ]

news_engine = NewsEngine()

# ============ API路由 ============

@app.route('/')
def index():
    """主页面"""
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/market/overview')
async def market_overview():
    """大盘概览"""
    try:
        # 大盘指数代码
        indices = {
            'sh': 'sh000001',  # 上证指数
            'sz': 'sz399001',  # 深证成指
            'cy': 'sz399006',  # 创业板指
            'hs300': 'sh000300'  # 沪深300
        }
        
        data = await engine.fetch_with_fallback(list(indices.values()))
        
        result = {}
        for key, code in indices.items():
            if code in data:
                result[key] = data[code]
        
        return jsonify({
            'success': True,
            'data': result,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/stocks/watchlist')
async def watchlist_data():
    """自选股数据 + 多因子评分"""
    try:
        codes = list(WATCHLIST.keys())
        # 转换格式
        fetch_codes = []
        for c in codes:
            if c.endswith('.SS'):
                fetch_codes.append(c.replace('.SS', ''))
            elif c.endswith('.SZ'):
                fetch_codes.append('sz' + c.replace('.SZ', ''))
            else:
                fetch_codes.append(c)
        
        data = await engine.fetch_with_fallback(fetch_codes)
        
        result = []
        for code in codes:
            # 查找对应的数据
            stock_data = None
            for k, v in data.items():
                if code.replace('.SZ', '').replace('.SS', '') in k:
                    stock_data = v
                    break
            
            if not stock_data:
                continue
            
            # 计算多因子评分
            factor = factor_engine.calculate_factors(code, stock_data)
            
            # 检测异动
            history = factor_engine.price_history.get(code, [])
            anomalies = detector.detect(code, stock_data, history)
            
            # 关联消息
            related_news = [n for n in news_engine.get_news() if code in n.get('stocks', [])]
            
            result.append({
                'code': code,
                'name': WATCHLIST.get(code, {}).get('name', stock_data['name']),
                'sector': WATCHLIST.get(code, {}).get('sector', ''),
                'theme': WATCHLIST.get(code, {}).get('theme', ''),
                'price': stock_data['price'],
                'change_pct': stock_data['change_pct'],
                'volume': stock_data['volume'],
                'amount': stock_data.get('amount', 0),
                'high': stock_data['high'],
                'low': stock_data['low'],
                'factor_score': factor,
                'anomalies': anomalies,
                'related_news': related_news,
                'history': [
                    {'price': h['price'], 'volume': h['volume']}
                    for h in history[-30:]
                ]
            })
        
        return jsonify({
            'success': True,
            'data': result,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/stocks/leaders')
def leaders():
    """龙头推荐"""
    try:
        # 这里简化处理，实际应该从watchlist_data获取
        # 为了演示，返回模拟数据
        return jsonify({
            'success': True,
            'data': [
                {
                    'sector': 'MLCC',
                    'theme': 'MLCC涨价周期',
                    'leader': {'code': '300408.SZ', 'name': '三环集团', 'price': 111.68, 'change_pct': 6.06, 'score': 78, 'rating': {'level': 'A', 'text': '看多', 'color': '#34d399'}},
                    'followers': [{'code': '000636.SZ', 'name': '风华高科', 'change_pct': -2.28}],
                    'sector_avg_change': 1.89,
                    'logic': '涨价龙头+产能利用率回升',
                    'support': 'PE 74x，Q1净利+48.5%'
                },
                {
                    'sector': '半导体设备',
                    'theme': '国产替代',
                    'leader': {'code': '002371.SZ', 'name': '北方华创', 'price': 687.0, 'change_pct': 2.83, 'score': 52, 'rating': {'level': 'B', 'text': '中性偏多', 'color': '#f59e0b'}},
                    'followers': [],
                    'sector_avg_change': 1.82,
                    'logic': '订单传闻+国产替代',
                    'support': 'PE 89x，高位回调中'
                },
                {
                    'sector': '军工',
                    'theme': 'C919大飞机',
                    'leader': {'code': '000768.SZ', 'name': '中航西飞', 'price': 21.11, 'change_pct': 0.72, 'score': 58, 'rating': {'level': 'B', 'text': '中性偏多', 'color': '#f59e0b'}},
                    'followers': [],
                    'sector_avg_change': 0.72,
                    'logic': 'C919订单放量',
                    'support': 'PE 52x，订单回暖'
                }
            ],
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/news/anomalies')
def anomaly_news():
    """异动消息"""
    return jsonify({
        'success': True,
        'data': news_engine.get_news(),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/api/capital/flow')
def capital_flow():
    """资金流向（模拟数据，实际接入东方财富API）"""
    return jsonify({
        'success': True,
        'data': {
            'northbound': {
                'shanghai': 25.8,
                'shenzhen': 18.3,
                'total': 44.1
            },
            'main_force': {
                'inflow_sectors': [
                    {'name': '电子元件', 'amount': 45.2},
                    {'name': '半导体', 'amount': 38.7},
                    {'name': '军工', 'amount': 22.1}
                ],
                'outflow_sectors': [
                    {'name': '白酒', 'amount': -32.5},
                    {'name': '银行', 'amount': -18.3},
                    {'name': '地产', 'amount': -15.7}
                ]
            }
        },
        'timestamp': datetime.now().isoformat()
    })

# ============ HTML前端模板 ============

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>量化机构级 · 市场异动监控中心</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: #0a0e1a; color: #e2e8f0;
            min-height: 100vh;
        }
        .header {
            background: linear-gradient(135deg, #1e3a5f 0%, #0d2137 100%);
            padding: 20px 30px;
            border-bottom: 1px solid #1e3a5f;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .header h1 { font-size: 22px; font-weight: 700; }
        .header .status {
            display: flex; align-items: center; gap: 8px;
            font-size: 13px; color: #94a3b8;
        }
        .status-dot {
            width: 8px; height: 8px; border-radius: 50%;
            background: #10b981; animation: pulse 2s infinite;
        }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
        
        .container { padding: 20px 30px; max-width: 1600px; margin: 0 auto; }
        
        /* 大盘指数 */
        .indices-grid {
            display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px;
            margin-bottom: 20px;
        }
        .index-card {
            background: #111827; border: 1px solid #1e3a5f;
            border-radius: 10px; padding: 16px;
        }
        .index-name { font-size: 12px; color: #94a3b8; margin-bottom: 4px; }
        .index-value { font-size: 24px; font-weight: 700; font-variant-numeric: tabular-nums; }
        .index-change { font-size: 13px; margin-top: 4px; font-variant-numeric: tabular-nums; }
        .up { color: #f43f5e; }
        .down { color: #10b981; }
        
        /* 主内容区 */
        .main-grid {
            display: grid; grid-template-columns: 1fr 380px;
            gap: 16px; margin-bottom: 20px;
        }
        
        /* 异动消息面板 */
        .news-panel {
            background: #111827; border: 1px solid #1e3a5f;
            border-radius: 10px; padding: 16px;
        }
        .panel-title {
            font-size: 14px; font-weight: 700; margin-bottom: 12px;
            display: flex; align-items: center; gap: 6px;
        }
        .news-item {
            padding: 10px 0; border-bottom: 1px solid #1e293b;
            font-size: 12px; line-height: 1.6;
        }
        .news-item:last-child { border-bottom: none; }
        .news-header { display: flex; justify-content: space-between; margin-bottom: 4px; }
        .news-tag {
            font-size: 10px; padding: 2px 8px; border-radius: 4px;
            font-weight: 600;
        }
        .tag-hot { background: rgba(244,63,94,0.15); color: #f43f5e; }
        .tag-rumor { background: rgba(245,158,11,0.15); color: #f59e0b; }
        .tag-news { background: rgba(59,130,246,0.15); color: #60a5fa; }
        .tag-policy { background: rgba(16,185,129,0.15); color: #34d399; }
        .credibility { color: #fbbf24; font-size: 11px; }
        .news-stocks { margin-top: 4px; color: #60a5fa; }
        
        /* 股票卡片 */
        .stock-grid {
            display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 12px;
        }
        .stock-card {
            background: #111827; border: 1px solid #1e3a5f;
            border-radius: 10px; padding: 16px;
            cursor: pointer; transition: all 0.2s;
        }
        .stock-card:hover { border-color: #3b82f6; }
        .stock-card.active { border-color: #3b82f6; box-shadow: 0 0 0 2px rgba(59,130,246,0.2); }
        .stock-header { display: flex; justify-content: space-between; align-items: flex-start; }
        .stock-info h3 { font-size: 15px; font-weight: 700; }
        .stock-code { font-size: 11px; color: #64748b; }
        .stock-sector { font-size: 10px; color: #94a3b8; margin-top: 2px; }
        .stock-price-area { text-align: right; }
        .stock-price { font-size: 22px; font-weight: 700; font-variant-numeric: tabular-nums; }
        .stock-change { font-size: 12px; font-variant-numeric: tabular-nums; }
        
        .score-bar { margin: 10px 0; }
        .score-label { display: flex; justify-content: space-between; font-size: 11px; color: #94a3b8; margin-bottom: 4px; }
        .score-track { height: 6px; background: #1e293b; border-radius: 3px; overflow: hidden; }
        .score-fill { height: 100%; border-radius: 3px; transition: width 0.5s; }
        
        .anomalies { display: flex; flex-wrap: wrap; gap: 4px; margin-top: 8px; }
        .anomaly-tag {
            font-size: 10px; padding: 2px 8px; border-radius: 4px;
            background: rgba(245,158,11,0.1); color: #f59e0b;
        }
        
        .mini-chart { height: 40px; margin-top: 8px; }
        
        /* 龙头推荐 */
        .leader-panel {
            background: #111827; border: 1px solid #1e3a5f;
            border-radius: 10px; padding: 16px;
        }
        .leader-item {
            padding: 12px; background: #0f172a;
            border-radius: 8px; margin-bottom: 10px;
            border-left: 3px solid #3b82f6;
        }
        .leader-sector { font-size: 12px; font-weight: 700; color: #60a5fa; }
        .leader-name { font-size: 14px; font-weight: 700; margin: 4px 0; }
        .leader-detail { font-size: 11px; color: #94a3b8; line-height: 1.5; }
        .leader-score {
            display: inline-block; padding: 2px 10px; border-radius: 10px;
            font-size: 11px; font-weight: 700; margin-top: 6px;
        }
        
        /* 资金流向 */
        .flow-panel {
            background: #111827; border: 1px solid #1e3a5f;
            border-radius: 10px; padding: 16px;
            margin-top: 16px;
        }
        .flow-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
        .flow-section h4 { font-size: 12px; color: #94a3b8; margin-bottom: 8px; }
        .flow-item { display: flex; justify-content: space-between; font-size: 12px; padding: 4px 0; }
        
        .loading { text-align: center; padding: 40px; color: #64748b; }
        .error { color: #f43f5e; padding: 20px; text-align: center; }
        
        /* 详情弹窗 */
        .modal-overlay {
            display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0,0,0,0.7); z-index: 1000;
            justify-content: center; align-items: center;
        }
        .modal-overlay.show { display: flex; }
        .modal {
            background: #111827; border: 1px solid #1e3a5f;
            border-radius: 12px; padding: 24px;
            width: 90%; max-width: 600px; max-height: 80vh;
            overflow-y: auto;
        }
        .modal h2 { font-size: 18px; margin-bottom: 16px; }
        .detail-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 16px; }
        .detail-cell { text-align: center; padding: 10px; background: #0f172a; border-radius: 8px; }
        .detail-label { font-size: 11px; color: #64748b; margin-bottom: 4px; }
        .detail-value { font-size: 16px; font-weight: 700; }
        
        .factor-breakdown { margin-top: 16px; }
        .factor-row { display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }
        .factor-name { width: 80px; font-size: 12px; color: #94a3b8; }
        .factor-bar { flex: 1; height: 8px; background: #1e293b; border-radius: 4px; overflow: hidden; }
        .factor-fill { height: 100%; border-radius: 4px; }
        .factor-score { width: 40px; text-align: right; font-size: 12px; font-weight: 700; }
        
        @media (max-width: 1200px) {
            .main-grid { grid-template-columns: 1fr; }
            .indices-grid { grid-template-columns: repeat(2, 1fr); }
        }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1>🎯 量化机构级 · 市场异动监控中心</h1>
            <div style="font-size:12px;color:#64748b;margin-top:4px;">多因子评分 | 异动检测 | 龙头推荐 | 舆情监控</div>
        </div>
        <div class="status">
            <span class="status-dot"></span>
            <span id="statusText">系统运行中 · 实时刷新</span>
        </div>
    </div>
    
    <div class="container">
        <!-- 大盘指数 -->
        <div class="indices-grid" id="indicesGrid">
            <div class="index-card"><div class="loading">加载中...</div></div>
            <div class="index-card"><div class="loading">加载中...</div></div>
            <div class="index-card"><div class="loading">加载中...</div></div>
            <div class="index-card"><div class="loading">加载中...</div></div>
        </div>
        
        <div class="main-grid">
            <div>
                <!-- 自选股监控 -->
                <div class="stock-grid" id="stockGrid">
                    <div class="loading">正在加载股票数据...</div>
                </div>
            </div>
            
            <div>
                <!-- 异动消息 -->
                <div class="news-panel" style="margin-bottom:16px;">
                    <div class="panel-title">📡 异动消息 & 小作文监控</div>
                    <div id="newsList"><div class="loading">加载中...</div></div>
                </div>
                
                <!-- 龙头推荐 -->
                <div class="leader-panel" style="margin-bottom:16px;">
                    <div class="panel-title">🏆 板块龙头推荐</div>
                    <div id="leaderList"><div class="loading">加载中...</div></div>
                </div>
                
                <!-- 资金流向 -->
                <div class="flow-panel">
                    <div class="panel-title">💰 资金流向</div>
                    <div id="flowContent"><div class="loading">加载中...</div></div>
                </div>
            </div>
        </div>
    </div>
    
    <!-- 详情弹窗 -->
    <div class="modal-overlay" id="modalOverlay" onclick="closeModal(event)">
        <div class="modal" onclick="event.stopPropagation()">
            <h2 id="modalTitle">股票详情</h2>
            <div id="modalContent"></div>
        </div>
    </div>
    
    <script>
        const API_BASE = '';
        let stockData = {};
        let refreshInterval;
        
        // 初始化
        async function init() {
            await Promise.all([
                loadIndices(),
                loadWatchlist(),
                loadNews(),
                loadLeaders(),
                loadCapitalFlow()
            ]);
            startAutoRefresh();
        }
        
        function startAutoRefresh() {
            refreshInterval = setInterval(() => {
                loadIndices();
                loadWatchlist();
            }, 30000); // 30秒刷新
        }
        
        // 加载大盘指数
        async function loadIndices() {
            try {
                const res = await fetch(`${API_BASE}/api/market/overview`);
                const data = await res.json();
                if (data.success) {
                    renderIndices(data.data);
                }
            } catch (e) { console.error('Indices error:', e); }
        }
        
        function renderIndices(data) {
            const names = { sh: '上证指数', sz: '深证成指', cy: '创业板指', hs300: '沪深300' };
            const container = document.getElementById('indicesGrid');
            container.innerHTML = Object.entries(names).map(([key, name]) => {
                const d = data[key];
                if (!d) return `<div class="index-card"><div class="index-name">${name}</div><div class="loading">--</div></div>`;
                const cls = d.change_pct >= 0 ? 'up' : 'down';
                const sign = d.change_pct >= 0 ? '+' : '';
                return `
                    <div class="index-card">
                        <div class="index-name">${name}</div>
                        <div class="index-value ${cls}">${d.price.toFixed(2)}</div>
                        <div class="index-change ${cls}">${sign}${d.change_pct.toFixed(2)}%  ${sign}${(d.price - d.pre_close).toFixed(2)}</div>
                    </div>
                `;
            }).join('');
        }
        
        // 加载自选股
        async function loadWatchlist() {
            try {
                const res = await fetch(`${API_BASE}/api/stocks/watchlist`);
                const data = await res.json();
                if (data.success) {
                    stockData = {};
                    data.data.forEach(s => stockData[s.code] = s);
                    renderWatchlist(data.data);
                }
            } catch (e) { console.error('Watchlist error:', e); }
        }
        
        function renderWatchlist(stocks) {
            const container = document.getElementById('stockGrid');
            container.innerHTML = stocks.map(s => {
                const cls = s.change_pct >= 0 ? 'up' : 'down';
                const sign = s.change_pct >= 0 ? '+' : '';
                const score = s.factor_score?.total || 0;
                const rating = s.factor_score?.rating || {};
                const scoreColor = rating.color || '#64748b';
                const anomalies = s.anomalies || [];
                const news = s.related_news || [];
                
                // 迷你走势图
                const history = s.history || [];
                const chartSvg = drawMiniChart(history);
                
                return `
                    <div class="stock-card" onclick="showDetail('${s.code}')">
                        <div class="stock-header">
                            <div class="stock-info">
                                <h3>${s.name}</h3>
                                <div class="stock-code">${s.code}</div>
                                <div class="stock-sector">${s.sector} · ${s.theme}</div>
                            </div>
                            <div class="stock-price-area">
                                <div class="stock-price ${cls}">${s.price.toFixed(2)}</div>
                                <div class="stock-change ${cls}">${sign}${s.change_pct.toFixed(2)}%</div>
                            </div>
                        </div>
                        <div class="score-bar">
                            <div class="score-label">
                                <span>量化评分</span>
                                <span style="color:${scoreColor}">${score}分 · ${rating.text || '评估中'}</span>
                            </div>
                            <div class="score-track">
                                <div class="score-fill" style="width:${score}%;background:${scoreColor}"></div>
                            </div>
                        </div>
                        <div class="anomalies">
                            ${anomalies.map(a => `<span class="anomaly-tag">${a.icon} ${a.text}</span>`).join('')}
                            ${news.length > 0 ? `<span class="anomaly-tag" style="background:rgba(59,130,246,0.1);color:#60a5fa;">📰 关联${news.length}条消息</span>` : ''}
                        </div>
                        <div class="mini-chart">${chartSvg}</div>
                    </div>
                `;
            }).join('');
        }
        
        function drawMiniChart(history) {
            if (history.length < 2) return '';
            const prices = history.map(h => h.price);
            const min = Math.min(...prices), max = Math.max(...prices);
            const range = max - min || 1;
            const w = 260, h = 40;
            const pts = prices.map((p, i) => {
                const x = (i / (prices.length - 1)) * w;
                const y = h - ((p - min) / range) * h * 0.8 - h * 0.1;
                return `${x},${y}`;
            }).join(' ');
            const color = prices[prices.length - 1] >= prices[0] ? '#f43f5e' : '#10b981';
            return `<svg width="100%" height="100%" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
                <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
                <circle cx="${w}" cy="${h - ((prices[prices.length-1] - min) / range) * h * 0.8 - h * 0.1}" r="2.5" fill="${color}"/>
            </svg>`;
        }
        
        // 加载异动消息
        async function loadNews() {
            try {
                const res = await fetch(`${API_BASE}/api/news/anomalies`);
                const data = await res.json();
                if (data.success) renderNews(data.data);
            } catch (e) { console.error('News error:', e); }
        }
        
        function renderNews(news) {
            const container = document.getElementById('newsList');
            container.innerHTML = news.map(n => {
                const tagClass = n.type === 'hot' ? 'tag-hot' : n.type === 'rumor' ? 'tag-rumor' : n.type === 'policy' ? 'tag-policy' : 'tag-news';
                const stars = '★'.repeat(n.credibility) + '☆'.repeat(5 - n.credibility);
                return `
                    <div class="news-item">
                        <div class="news-header">
                            <span><span class="news-tag ${tagClass}">${n.type === 'rumor' ? '小作文' : n.type === 'hot' ? '热点' : n.type === 'policy' ? '政策' : '新闻'}</span> <span class="credibility">${stars}</span></span>
                            <span style="color:#64748b;font-size:11px;">${n.time}</span>
                        </div>
                        <div style="color:#e2e8f0;margin-top:4px;">${n.title}</div>
                        <div style="color:#64748b;font-size:11px;margin-top:4px;">${n.impact}</div>
                        <div class="news-stocks">关联: ${n.stocks.join(', ')}</div>
                    </div>
                `;
            }).join('');
        }
        
        // 加载龙头推荐
        async function loadLeaders() {
            try {
                const res = await fetch(`${API_BASE}/api/stocks/leaders`);
                const data = await res.json();
                if (data.success) renderLeaders(data.data);
            } catch (e) { console.error('Leaders error:', e); }
        }
        
        function renderLeaders(leaders) {
            const container = document.getElementById('leaderList');
            container.innerHTML = leaders.map(l => {
                const leader = l.leader;
                const cls = leader.change_pct >= 0 ? 'up' : 'down';
                const sign = leader.change_pct >= 0 ? '+' : '';
                return `
                    <div class="leader-item">
                        <div class="leader-sector">${l.sector} · ${l.theme}</div>
                        <div class="leader-name">${leader.name} (${leader.code})</div>
                        <div class="leader-detail">
                            价格: <span class="${cls}">${leader.price.toFixed(2)} ${sign}${leader.change_pct.toFixed(2)}%</span> · 
                            板块平均: ${l.sector_avg_change >= 0 ? '+' : ''}${l.sector_avg_change}%<br>
                            逻辑: ${l.logic}<br>
                            支撑: ${l.support}
                        </div>
                        <span class="leader-score" style="background:${leader.rating.color || '#1e293b'};color:#fff;">
                            ${leader.score}分 · ${leader.rating.text || '评估中'}
                        </span>
                    </div>
                `;
            }).join('');
        }
        
        // 加载资金流向
        async function loadCapitalFlow() {
            try {
                const res = await fetch(`${API_BASE}/api/capital/flow`);
                const data = await res.json();
                if (data.success) renderCapitalFlow(data.data);
            } catch (e) { console.error('Flow error:', e); }
        }
        
        function renderCapitalFlow(data) {
            const container = document.getElementById('flowContent');
            const nb = data.northbound;
            container.innerHTML = `
                <div style="margin-bottom:12px;padding:10px;background:#0f172a;border-radius:8px;">
                    <div style="font-size:12px;color:#94a3b8;margin-bottom:6px;">北向资金</div>
                    <div style="display:flex;gap:16px;font-size:13px;">
                        <span>沪股通: <span class="${nb.shanghai >= 0 ? 'up' : 'down'}">${nb.shanghai >= 0 ? '+' : ''}${nb.shanghai}亿</span></span>
                        <span>深股通: <span class="${nb.shenzhen >= 0 ? 'up' : 'down'}">${nb.shenzhen >= 0 ? '+' : ''}${nb.shenzhen}亿</span></span>
                        <span>合计: <span class="${nb.total >= 0 ? 'up' : 'down'}" style="font-weight:700;">${nb.total >= 0 ? '+' : ''}${nb.total}亿</span></span>
                    </div>
                </div>
                <div class="flow-grid">
                    <div class="flow-section">
                        <h4>🟢 主力净流入</h4>
                        ${data.main_force.inflow_sectors.map(s => `
                            <div class="flow-item">
                                <span>${s.name}</span>
                                <span class="up">+${s.amount}亿</span>
                            </div>
                        `).join('')}
                    </div>
                    <div class="flow-section">
                        <h4>🔴 主力净流出</h4>
                        ${data.main_force.outflow_sectors.map(s => `
                            <div class="flow-item">
                                <span>${s.name}</span>
                                <span class="down">${s.amount}亿</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
            `;
        }
        
        // 详情弹窗
        function showDetail(code) {
            const s = stockData[code];
            if (!s) return;
            
            const cls = s.change_pct >= 0 ? 'up' : 'down';
            const sign = s.change_pct >= 0 ? '+' : '';
            const factor = s.factor_score || {};
            const breakdown = factor.breakdown || {};
            
            document.getElementById('modalTitle').innerHTML = `${s.name} (${code}) <span style="font-size:14px;color:#64748b;">${s.sector} · ${s.theme}</span>`;
            
            document.getElementById('modalContent').innerHTML = `
                <div class="detail-grid">
                    <div class="detail-cell">
                        <div class="detail-label">最新价</div>
                        <div class="detail-value ${cls}">${s.price.toFixed(2)}</div>
                    </div>
                    <div class="detail-cell">
                        <div class="detail-label">涨跌幅</div>
                        <div class="detail-value ${cls}">${sign}${s.change_pct.toFixed(2)}%</div>
                    </div>
                    <div class="detail-cell">
                        <div class="detail-label">成交量</div>
                        <div class="detail-value">${(s.volume / 10000).toFixed(0)}万</div>
                    </div>
                    <div class="detail-cell">
                        <div class="detail-label">成交额</div>
                        <div class="detail-value">${(s.amount / 100000000).toFixed(2)}亿</div>
                    </div>
                </div>
                
                <div class="factor-breakdown">
                    <div style="font-size:13px;font-weight:700;margin-bottom:10px;">多因子评分拆解</div>
                    ${Object.entries(breakdown).map(([k, v]) => {
                        const colors = { technical: '#3b82f6', momentum: '#f43f5e', capital: '#f59e0b', volatility: '#8b5cf6', liquidity: '#10b981' };
                        const names = { technical: '技术面', momentum: '动量', capital: '资金面', volatility: '波动率', liquidity: '流动性' };
                        const maxScores = { technical: 30, momentum: 20, capital: 20, volatility: 15, liquidity: 15 };
                        return `
                            <div class="factor-row">
                                <div class="factor-name">${names[k] || k}</div>
                                <div class="factor-bar">
                                    <div class="factor-fill" style="width:${(v / maxScores[k]) * 100}%;background:${colors[k] || '#3b82f6'}"></div>
                                </div>
                                <div class="factor-score">${v}/${maxScores[k]}</div>
                            </div>
                        `;
                    }).join('')}
                </div>
                
                ${s.related_news && s.related_news.length > 0 ? `
                    <div style="margin-top:16px;">
                        <div style="font-size:13px;font-weight:700;margin-bottom:10px;">关联异动消息</div>
                        ${s.related_news.map(n => `
                            <div style="padding:8px;background:#0f172a;border-radius:6px;margin-bottom:6px;font-size:12px;">
                                <div style="color:#94a3b8;font-size:11px;">${n.time} · ${n.source} · 可信度${'★'.repeat(n.credibility)}</div>
                                <div style="margin-top:4px;">${n.title}</div>
                                <div style="color:#64748b;margin-top:2px;">${n.impact}</div>
                            </div>
                        `).join('')}
                    </div>
                ` : ''}
            `;
            
            document.getElementById('modalOverlay').classList.add('show');
        }
        
        function closeModal(e) {
            if (e.target === document.getElementById('modalOverlay')) {
                document.getElementById('modalOverlay').classList.remove('show');
            }
        }
        
        // 启动
        init();
    </script>
</body>
</html>
'''

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
