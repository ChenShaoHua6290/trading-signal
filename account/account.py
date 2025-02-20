import okx.MarketData as MarketData
import logging
import pandas as pd
import numpy as np
import talib
import requests
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import json
import threading


# 配置日志记录器
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# 线程锁
request_lock = threading.Lock()
# 引入请求计数器和时间记录
request_count = 0
last_request_time = time.time()

timeframes = ['15m','30m' '1H', '2H', '4H']


instIds = ["BTC","ETH","XRP","SOL","DOGE","BCH","LDO","GALA","GRASS","PEPE",
           "PYTH","SHIB","SUI","TRB","UNI","PNUT","LINK","MEME","MEW","MKR",
           "NEIRO","ORDI","WLD","LTC","YGG","INJ","SATS","AAVE","ETC","OP",
           "SUSHI","NEAR","AR","SSV","MASK","BONK","BSV","SAND","DYDX","CORE"]

api_key = "531e1af3-6266-4f8e-8966-151414344866"
secret_key = "5609668625511F975ECB08E4B950A339"
passphrase = "Csh6290."


def query_kline_data(timeframe):
    global request_count
    for instId in instIds:
        # 检查是否达到限速
        with request_lock:
            if request_count >= 30:
                logging.info("达到限速，等待 3 秒...")
                time.sleep(3)
                request_count = 0
        logging.info("执行"+instId+"：周期:"+str(timeframe))
        api_request_worker(instId, timeframe)
        # 稍微延迟，避免请求过于集中
        time.sleep(1)

def api_request_worker(instId,bar):
    global request_count
    marketDataAPI = MarketData.MarketAPI(flag="0", api_key=api_key, api_secret_key=secret_key, passphrase=passphrase)
    total_limit = 670
    all_results = []
    # 初始起始时间，设置为 None 表示从最新数据开始获取
    after = ''
    while len(all_results) < total_limit:
        try:
            # 获取 K 线数据
            result = marketDataAPI.get_candlesticks(
                instId=instId+"-USDT-SWAP",
                bar=bar,
                after=after,
                limit=300
            )
            all_results.extend(result['data'])
            if len(result['data']) == 0:
                break
            # 获取最后一条 K 线的时间戳，作为下一次请求的起始时间
            after = result['data'][-1][0]
        except Exception as e:
            if "Too Many Requests" in str(e):
                logging.error(f"请求过于频繁...")
                break
            else:
                logging.info(f"获取K线失败 Error: {e}")
                break
        finally:
            # 线程安全地更新请求计数
            with request_lock:
                request_count += 1
                if request_count >= 30:
                    logging.error("达到限速，等待 3 秒...")
                    time.sleep(3)
                    request_count = 0

    # 只取前 500 条数据，以防获取的数据多于 500 条
    final_results = all_results[:total_limit]
    if len(final_results) > 650:
        logging.info("处理K线数据"+instId+":"+bar)
        df1 = pd.DataFrame(final_results,
                            columns=['timestamp', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm'])
        que_result = df1.query('confirm!= "0"')
        que_result['close'] = que_result['close'].astype(float)  # 将收盘价转换为浮点数
        df = que_result.iloc[::-1]
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
        #
        # # 计算 ATR，使用 ATR 的倍数来确定 MACD 接近 0 轴的范围
        atr = talib.ATR(df['high'], df['low'], df['close'], timeperiod=14)
        is_near_zero = np.abs(dea) < atr * 0.5
        #
        # # 计算死叉和金叉
        dead_cross = (tr.shift(1) <= ema7.shift(1)) & (tr > ema7)
        golden_cross = (tr.shift(1) >= ema7.shift(1)) & (tr < ema7)
        # # 信号 1
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
        ts = timestamp_to_string(df['timestamp'].iloc[-1])
        if latest_sell:
            # 发送 sell 信号的 webhook
            payload = ts+"｜"+instId+"｜"+bar+"｜空"
            sendMsg(payload)
        elif latest_buy:
            # 发送 buy 信号的 webhook
            payload = ts+"｜"+instId+"｜"+bar+"｜多"
            sendMsg(payload)
        else:
            logging.info("无信号发送")

def sendMsg(content):
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


def timestamp_to_string(string_timestamp):
    timestamp = int(string_timestamp)
    time_tuple = time.localtime(timestamp / 1000)
    time_string = time.strftime("%Y-%m-%d %H:%M", time_tuple)
    return time_string


def main():
    scheduler = BlockingScheduler()
        # 安排在每个小时的 0 分、15 分、30 分、45 分执行
    for minute in [0, 15, 30, 45]:
        scheduler.add_job(query_kline_data, 'cron', minute=minute, second=1, args=["15m"])
    # 30分钟周期 安排在每个小时的 0 分、30 分执行
    for minute in [1, 31]:
        scheduler.add_job(query_kline_data, 'cron', minute=minute, second=30, args=["30m"])
    # 1小时周期
    scheduler.add_job(query_kline_data, 'cron', minute=3, second=1, args=["1H"])
    # 2小时周期
    scheduler.add_job(query_kline_data, 'cron', hour='*/2', minute=4, second=30, args=["2H"])
    # 4小时周期
    scheduler.add_job(query_kline_data, 'cron', hour='*/4', minute=6, second=1, args=["4H"])
    # 启动调度器
    scheduler.start()
    try:
        # 保持主线程存活
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # 关闭调度器
        scheduler.shutdown()

if __name__ == "__main__":
    main()
