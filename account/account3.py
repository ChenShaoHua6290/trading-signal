import okx.MarketData as MarketData
import pandas as pd
import numpy as np
import talib
import requests
import datetime
import schedule
from concurrent.futures import ThreadPoolExecutor



def getData(instId, bar):
    flag = "0"  # 实盘:0, 模拟盘：1
    api_key = "531e1af3-6266-4f8e-8966-151414344866"
    secret_key = "5609668625511F975ECB08E4B950A339"
    passphrase = "Csh6290."
    marketDataAPI = MarketData.MarketAPI(flag=flag, api_key=api_key, api_secret_key=secret_key, passphrase=passphrase)
    total_limit = 670
    all_results = []
    # 初始起始时间，设置为 None 表示从最新数据开始获取
    after = ''
    retry_count = 3  # 重试次数
    retry_interval = 5  # 重试间隔（秒）
    while len(all_results) < total_limit:
        try:
            for i in range(retry_count):
                try:
                    # 获取 K 线数据
                    result = marketDataAPI.get_candlesticks(
                        instId=instId,
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
                    if i < retry_count - 1:
                        print(f"获取K线失败, retrying in {retry_interval} seconds. Error: {e}")
                        import time
                        time.sleep(retry_interval)
                    else:
                        print(f"获取K线失败 after {retry_count} attempts. Error: {e}")
                        break
        except Exception as e:
            print(f"General Error: {e}")
            break
    # 只取前 500 条数据，以防获取的数据多于 500 条
    final_results = all_results[:total_limit]

    if len(final_results)>500:
        df1 = pd.DataFrame(final_results,
                           columns=['timestamp', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'confirm'])
        result = df1.query('confirm!= 0')
        result['close'] = result['close'].astype(float)  # 将收盘价转换为浮点数
        df = df1.iloc[::-1]
        # 本周期和 3/6 倍 Macd
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
        # 飞书 webhook 地址
        webhook_url = "https://www.feishu.cn/flow/api/trigger-webhook/d150eac2dfb5683ae5bb99793887be8c"

        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if  latest_sell:
            # 发送 sell 信号的 webhook
            payload = {"品种": instId, "周期": bar, "方向": "空","当前时间":now}
            try:
                response = requests.post(webhook_url, json=payload)
                if response.status_code == 200:
                    print(f"发送 空 信号提醒. Response: {response.status_code}")
                else:
                    print(f"发送 空 信号提醒失败. Status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"发送 空 信号提醒异常: {e}")
        elif latest_buy:
            # 发送 buy 信号的 webhook
            payload = {"品种": instId, "周期": bar, "方向": "多","当前时间":now}
            try:
                response = requests.post(webhook_url, json=payload)
                if response.status_code == 200:
                    print(f"发送 多 信号提醒. Response: {response.status_code}")
                else:
                    print(f"发送 多 信号提醒失败. Status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"发送 多 信号提醒异常: {e}")
        else:
            print("无信号发送.")


def execute(instId, bar, executor):
    executor.submit(getData, instId, bar)


def main():
    instIds = ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
    # 创建线程池，可根据需要调整线程池大小
    executor = ThreadPoolExecutor(max_workers=5)
    # 15分钟周期
    for instId in instIds:
        # 安排在每个小时的 0 分、15 分、30 分、45 分执行
        for minute in [0, 15, 30, 45]:
            print("执行"+instId+"：15周期")
            schedule.every().hour.at(f":{minute:02d}").do(execute, instId=instId, bar="15m", executor=executor)
    # 30分钟周期
    for instId in instIds:
        # 安排在每个小时的 0 分、30 分执行
        for minute in [0, 30]:
            print("执行"+instId+"：30m周期")
            schedule.every().hour.at(f":{minute:02d}").do(execute, instId=instId, bar="30m", executor=executor)
    # 1小时周期
    for instId in instIds:
        print("执行"+instId+"：1H周期")
        schedule.every().hour.at(":00").do(execute, instId=instId, bar="1H", executor=executor)
    # 2小时周期
    for instId in instIds:
        print("执行"+instId+"：2H周期")
        schedule.every(2).hours.at(":00").do(execute, instId=instId, bar="2H", executor=executor)
    # 4小时周期
    for instId in instIds:
        print("执行"+instId+"：4H周期")
        schedule.every(4).hours.at(":00").do(execute, instId=instId, bar="4H", executor=executor)
    while True:
        schedule.run_pending()

if __name__ == "__main__":
    main()
