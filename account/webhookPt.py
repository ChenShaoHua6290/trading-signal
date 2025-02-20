import requests
import json

def sendMsg(content):
    # 构建消息的 JSON 数据
    message = {
        "msgtype": "text",
        "text": {
            "content": content,
            # "mentioned_list": ["@all"]
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
        print("消息发送成功")
    except requests.exceptions.RequestException as e:
        print(f"消息发送失败，错误信息：{e}")

# 示例调用
if __name__ == "__main__":
    content = "这是一条测试消息。"
    sendMsg(content)
