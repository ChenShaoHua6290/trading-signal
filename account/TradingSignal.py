import okx.MarketData as MarketData
import logging
import pandas as pd
import numpy as np
import talib
import requests
from concurrent.futures import ThreadPoolExecutor
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import json
import threading

# 配置日志记录器
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TradingSignal:
    def __init__(self):
        # 初始化 API 相关信息
        self.api_key = "531e1af3-6266-4f8e-8966-151414344866"
        self.secret_key = "5609668625511F975ECB08E4B950A339"
        self.passphrase = "Csh6290."
        self.marketDataAPI = MarketData.MarketAPI(flag="0", api_key=self.api_key, api_secret_key=self.secret_key,
                                                  passphrase=self.passphrase)
        # 初始化请求计数器和时间记录
        self.request_count = 0
        self.last_request_time = time.time()
        # 线程锁，用于线程同步
        self.lock = threading.Lock()
        # 初始化线程池
        self.executor = ThreadPoolExecutor(max_workers=5)
        # 初始化任务调度器
        self.scheduler = BlockingScheduler()

    def get_candlesticks(self, instId, bar, after='', limit=300):
        """
        获取 K 线数据，并控制请求频率
        """
        with self.lock:
            current_time = time.time()
            if current_time - self.last_request_time < 2:
                if self.request_count >= 40:
                    logging.info("达到请求限速，等待...")
                    time_to_wait = 2 - (current_time - self.last_request_time)
                    time.sleep(time_to_wait)
                    self.request_count = 0
                    self.last_request_time = time.time()
            else:
                self.request_count = 0
                self.last_request_time = current_time
            self.request_count += 1
        try:
            result = self.marketDataAPI.get_candlesticks(
                instId=instId + "-USDT-SWAP",
                bar=bar,
                after=after,
                limit=limit
            )
            return result
        except Exception as e:
            logging.error(f"请求K线数据出错: {e}")
            return None

    def process_data(self, final_results, instId, bar):
        """
        处理 K 线数据，计算指标并判断买卖信号
        """
        if len(final_results) > 650:
            logging.info("处理K线数据" + instId + ":" + bar)
            df1 = pd.DataFrame(final_results,
                               columns=['timestamp', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote',
                                        'confirm'])
            result = df1.query('confirm!= "0"')
            result['close'] = result['close'].astype(float)  # 将收盘价转换为浮点数
            df = result.iloc[::-1]
            # 本周期和 3/4/6 倍 Macd
            dif, dea, macd = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
            dif1, dea1, macd1 = talib.MACD(df['close'], fastperiod=36, slowperiod=78, signalperiod=27)
            dif2, dea2, macd2 = talib.MACD(df['close'], fastperiod=72, slowperiod=156, signalperiod=54)
            dif3, dea3, macd3 = talib.MACD(df['close'], fastperiod=48, slowperiod=104, signalperiod=36)
            # 信号转折点
            N1 = 4
            emaa = talib.EMA(df['close'], N1)
            emab = talib.EMA(emaa, N1)
            tr = talib.EMA(emab, timeperiod=N1)
            src1 = df['close']
            emal1 = 2 * talib.EMA(src1, timeperiod=int(N1 / 2)) - talib.EMA(src1, timeperiod=N1)
            ema7 = talib.EMA(emal1, timeperiod=6)
            # 计算 ATR，使用 ATR 的倍数来确定 MACD 接近 0 轴的范围
            atr = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
            is_near_zero = np.abs(dea) < atr * 0.5
            # 计算死叉和金叉
            dead_cross = (tr.shift(1) <= ema7.shift(1)) & (tr > ema7)
            golden_cross = (tr.shift(1) >= ema7.shift(1)) & (tr < ema7)
            # 信号 1
            sell = ((dea <= 0) | ((dea >= 0) & (dif < 0))) & is_near_zero & dead_cross & (
                    (dif1 < dea1) & ((dif2 < dea2) | (dif3 < dea3) | ((dif2 < dea2) & (dif3 < dea3))))
            buy = ((dea >= 0) | ((dif > 0) & (dea <= 0))) & is_near_zero & golden_cross & (
                    (dif1 > dea1) & ((dif2 > dea2) | (dif3 > dea3) | ((dif2 > dea2) & (dif3 > dea3))))
            # 输出结果
            df['sell'] = sell
            df['buy'] = buy
            # 检查最新的一条记录的 sell1 或 buy1 是否为 True
            latest_sell = df['sell'].iloc[-1]
            latest_buy = df['buy'].iloc[-1]
            ts = self.timestamp_to_string(df['timestamp'].iloc[-1])
            if latest_sell:
                # 发送 sell 信号的 webhook
                payload = ts + "｜" + instId + "｜" + bar + "｜空"
                self.sendMsg(payload)
            elif latest_buy:
                # 发送 buy 信号的 webhook
                payload = ts + "｜" + instId + "｜" + bar + "｜多"
                self.sendMsg(payload)
            else:
                logging.info("无信号发送")

    def timestamp_to_string(self, string_timestamp):
        """
        将时间戳转换为可读的时间格式
        """
        timestamp = int(string_timestamp)
        time_tuple = time.localtime(timestamp / 1000)
        time_string = time.strftime("%Y-%m-%d %H:%M", time_tuple)
        return time_string

    def sendMsg(self, content):
        """
        发送买卖信号的 Webhook 消息
        """
        # 构建消息的 JSON 数据
        message = {
            "msgtype": "text",
            "text": {
                "content": content,
                "mentioned_list": ["@all"]
            }
        }
        # 企业微信群机器人的 Webhook 地址
        webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=06a0fb80-ae9f-42aa-9d72-b9f8194bcafd"
        headers = {
            "Content-Type": "application/json"
        }
        try:
            # 发送 POST 请求
            response = requests.post(webhook_url, headers=headers, data=json.dumps(message))
            # 检查响应状态码
            response.raise_for_status()
            logging.info(f"发送 {content} 信号提醒. Response: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.info(f"消息发送失败 {content} 信号提醒异常. Response: {e}")

    def get_and_process_data(self, instId, bar):
        """
        获取并处理 K 线数据
        """
        total_limit = 670
        all_results = []
        after = ''
        retry_count = 3
        retry_interval = 5
        while len(all_results) < total_limit:
            for i in range(retry_count):
                result = self.get_candlesticks(instId, bar, after)
                if result:
                    all_results.extend(result['data'])
                    if len(result['data']) == 0:
                        break
                    after = result['data'][-1][0]
                    break
                else:
                    if i < retry_count - 1:
                        logging.info(f"获取K线失败, retrying in {retry_interval} seconds.")
                        time.sleep(retry_interval)
                        retry_interval *= 2
                    else:
                        logging.info(f"获取K线失败 after {retry_count} attempts.")
                        break
        final_results = all_results[:total_limit]
        self.process_data(final_results, instId, bar)

    def execute_task(self, instId, bar):
        """
        执行任务，启动线程处理数据
        """
        try:
            logging.info(f"开始执行任务: {instId} - {bar}")
            self.executor.submit(self.get_and_process_data, instId, bar)
        except Exception as e:
            logging.error(f"执行任务出错: {e}")

    def schedule_task(self, instId, bar, minutes, base_second, offset):
        """
        调度任务，设置任务执行时间
        """
        for minute in minutes:
            adjusted_second = (base_second + offset) % 60
            logging.info(f"执行{instId}：{bar}周期:{minute}, 秒: {adjusted_second}")
            self.scheduler.add_job(self.execute_task, 'cron', minute=minute, second=adjusted_second,
                                   args=[instId, bar])

    def start_system(self):
        """
        启动整个系统，包括任务调度
        """
        instIds = ["BTC", "ETH", "XRP", "SOL", "DOGE", "BCH",
                   "LDO", "GALA", "GRASS", "PEPE", "PYTH", "SHIB",
                   "SUI", "TRB", "UNI", "PNUT", "LINK", "MEME",
                   "MEW", "MKR", "NEIRO", "ORDI", "WLD", "LTC",
                   "YGG", "INJ", "SATS", "AAVE", "ETC", "OP",
                   "SUSHI", "NEAR", "AR", "SSV", "MASK", "BONK",
                   "BSV", "SAND", "DYDX", "CORE"]

        # 15分钟周期
        for instId in instIds:
            self.schedule_task(instId, "15m", [0, 15, 30, 45], 2, 0)
        # 30分钟周期
        for instId in instIds:
            self.schedule_task(instId, "30m", [0, 30], 50, 0)
        # 1小时周期
        for instId in instIds:
            self.schedule_task(instId, "1H", [2], 15, 0)
        # 2小时周期
        for instId in instIds:
            self.schedule_task(instId, "2H", [i for i in range(0, 24, 2)], 5, 20)
        # 4小时周期
        for instId in instIds:
            self.schedule_task(instId, "4H", [i for i in range(0, 24, 4)], 7, 30)

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.scheduler.shutdown()
            self.executor.shutdown()

if __name__ == "__main__":
    system = TradingSignal()
    system.start_system()
