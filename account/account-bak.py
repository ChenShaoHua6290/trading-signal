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



# 配置日志记录器
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 引入请求计数器和时间记录
request_count = 0
last_request_time = time.time()

def getData(instId, bar):
    global request_count, last_request_time
    api_key = "531e1af3-6266-4f8e-8966-151414344866"
    secret_key = "5609668625511F975ECB08E4B950A339"
    passphrase = "Csh6290."
    marketDataAPI = MarketData.MarketAPI(flag="0", api_key=api_key, api_secret_key=secret_key, passphrase=passphrase)
    total_limit = 670
    all_results = []
    # 初始起始时间，设置为 None 表示从最新数据开始获取
    after = ''
    retry_count = 3  # 重试次数
    retry_interval = 5  # 重试间隔（秒）
    result = ''
    while len(all_results) < total_limit:
        try:
            for i in range(retry_count):
                try:
                    # 获取 K 线数据
                    # 检查请求频率
                    current_time = time.time()
                    if current_time - last_request_time < 1:
                        if request_count >= 20:
                            logging.info("达到请求限速，等待...")
                            time.sleep(1 - (current_time - last_request_time))
                            request_count = 0
                    last_request_time = current_time
                    request_count += 1
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
                        logging.error(f"请求过于频繁，等待 {retry_interval} 秒后重试...")
                        time.sleep(retry_interval)
                        retry_interval *= 2  # 指数退避，增加重试间隔
                    if i < retry_count - 1:
                        logging.info(f"获取K线失败, retrying in {retry_interval} seconds. Error: {e},mes:{result}")
                        time.sleep(retry_interval)
                    else:
                        logging.info(f"获取K线失败 after {retry_count} attempts. Error: {e},mes:{result}")
                        break
        except Exception as e:
            logging.error(f"General Error: {e}")
            break
    # 只取前 500 条数据，以防获取的数据多于 500 条
    final_results = all_results[:total_limit]

    if len(final_results)>650:
        logging.info("处理K线数据"+instId+":"+bar)
        df1 = pd.DataFrame(final_results,
                           columns=['timestamp', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm'])
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
        # 飞书 webhook 地址
        # webhook_url = "https://www.feishu.cn/flow/api/trigger-webhook/d150eac2dfb5683ae5bb99793887be8c"
        if latest_sell:
            # 发送 sell 信号的 webhook
            # payload = {"品种": instId, "周期": bar, "方向": "空", "时间": ts}
            # payload = "时间:"+ts+" 品种:" +instId+" 周期:"+ bar+" 方向:空"
            payload = ts+"｜"+instId+"｜"+bar+"｜空"
            sendMsg(payload)
            # try:
            #     response = requests.post(webhook_url, json=payload)
            #     if response.status_code == 200:
            #         logging.info(f"发送 {instId} ,{bar},空 信号提醒. Response: {response.status_code}")
            #     else:
            #         logging.info(f"发送 {instId} ,{bar},空 信号提醒失败. Status code: {response.status_code}")
            # except requests.exceptions.RequestException as e:
            #     logging.info(f"发送 空 信号提醒异常: {e}")
        elif latest_buy:

            # 发送 buy 信号的 webhook
            # payload = {"品种": instId, "周期": bar, "方向": "多", "时间": ts}
            payload = ts+"｜"+instId+"｜"+bar+"｜多"
            sendMsg(payload)
            # try:
            #     response = requests.post(webhook_url, json=payload)
            #     if response.status_code == 200:
            #         logging.info(f"发送 {instId} ,{bar},多 信号提醒. Response: {response.status_code}")
            #     else:
            #         logging.info(f"发送 {instId} ,{bar},多 信号提醒失败. Status code: {response.status_code}")
            # except requests.exceptions.RequestException as e:
            #     logging.info(f"发送 {instId} ,{bar},多 信号提醒异常: {e}")
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
    # 企业微信群机器人的 Webhook 地址，这里需要替换为你自己的 key
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

def execute(instId, bar, executor):
    try:
        logging.info(f"开始执行任务: {instId} - {bar}")
        executor.submit(getData, instId, bar)
    except Exception as e:
        logging.error(f"执行任务出错: {e}")


def schedule_task(instId, bar, minutes, base_second, offset):
    for minute in minutes:
        adjusted_second = (base_second + offset) % 60
        logging.info(f"执行{instId}：{bar}周期:{minute}, 秒: {adjusted_second}")
        scheduler.add_job(execute, 'cron', minute=minute, second=adjusted_second, args=[instId, bar, executor])


def main():
    instIds = ["BTC","ETH","XRP","SOL","DOGE","BCH",
               "LDO","GALA","GRASS","PEPE","PYTH","SHIB",
               "SUI","TRB","UNI","PNUT","LINK","MEME",
               "MEW","MKR","NEIRO","ORDI","WLD","LTC",
               "YGG","INJ","SATS","AAVE","ETC","OP",
               "SUSHI","NEAR","AR","SSV","MASK","BONK",
               "BSV","SAND","DYDX","CORE"]
    # 创建线程池，可根据需要调整线程池大小
    # # 15分钟周期
    executor = ThreadPoolExecutor(max_workers=5)
    global scheduler
    scheduler = BlockingScheduler()
    for instId in instIds:
        # 安排在每个小时的 0 分、15 分、30 分、45 分执行
        for minute in [0, 15, 30, 45]:
            logging.info("执行"+instId+"：15m周期:"+str(minute))
            scheduler.add_job(execute, 'cron', minute=minute, second=3, args=[instId, "15m",executor])
    # 30分钟周期
    for instId in instIds:
        # 安排在每个小时的 0 分、30 分执行
        for minute in [0, 30]:
            logging.info("执行"+instId+"：30m周期:"+str(minute))
            scheduler.add_job(execute, 'cron', minute=minute, second=5, args=[instId, "30m",executor])
    # 1小时周期
    for instId in instIds:
        logging.info("执行"+instId+"：1H周期")
        scheduler.add_job(execute, 'cron', minute=0, second=15, args=[instId, "1H",executor])
    # 2小时周期
    for instId in instIds:
        logging.info("执行"+instId+"：2H周期")
        scheduler.add_job(execute, 'cron', hour='*/2', minute=0, second=20, args=[instId, "2H",executor])
    # 4小时周期
    for instId in instIds:
        logging.info("执行"+instId+"：4H周期")
        scheduler.add_job(execute, 'cron', hour='*/4', minute=0, second=30, args=[instId, "4H",executor])
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    main()
