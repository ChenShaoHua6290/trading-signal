import okx.PublicData as PublicData
import json
def pubApi():
    flag = "0"  # 实盘:0, 模拟盘：1
    api_key = "531e1af3-6266-4f8e-8966-151414344866"
    secret_key = "5609668625511F975ECB08E4B950A339"
    passphrase = "Csh6290."
    marketDataAPI = PublicData.PublicAPI(flag=flag, api_key=api_key, api_secret_key=secret_key, passphrase=passphrase)
    res = marketDataAPI.get_instruments(instType='SWAP')
    # 检查 res['data'] 是否是列表
    if isinstance(res.get('data'), list):
        # 假设列表元素是字典，且包含 'instId' 键
        instIds = []
        for item in res['data']:
            if 'instId' in item:
                instIds.append(item['instId'])
        for i in instIds:
            print(i)
    else:
        print("res['data'] is not a list. Please check the API response.")


if __name__ == "__main__":

    instIds = ["BTC-USDT-SWAP", "ETH-USDT-SWAP","XRP-USDT-SWAP","SOL-USDT-SWAP","DOGE-USDT-SWAP","BCH-USDT-SWAP","LDO-USDT-SWAP","GALA-USDT-SWAP","GRASS-USDT-SWAP","PEPE-USDT-SWAP","PYTH-USDT-SWAP",
               "SHIB-USDT-SWAP","SUI-USDT-SWAP","TRB-USDT-SWAP","UNI-USDT-SWAP","PNUT-USDT-SWAP","LINK-USDT-SWAP","MEME-USDT-SWAP","MEW-USDT-SWAP","MKR-USDT-SWAP","NEIRO-USDT-SWAP","ORDI-USDT-SWAP",
               "WLD-USDT-SWAP","LTC-USDT-SWAP","YGG-USDT-SWAP","INJ-USDT-SWAP"]
    pubApi()
