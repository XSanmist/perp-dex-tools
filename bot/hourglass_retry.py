#!/usr/bin/env python3
"""
Script to retry API requests to Hourglass until successful (non-500 response)
"""

import requests
import time
import json
from datetime import datetime


def make_request():
    """Make a single request to the Hourglass API"""
    url = 'https://api.hourglass.com/auth/tos/sign'

    headers = {
        'sec-ch-ua-platform': '"macOS"',
        'Referer': 'https://www.hourglass.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
        'content-type': 'application/json',
        'sec-ch-ua-mobile': '?0'
    }

    data = {
        "address": "0x4814De70f14f253A5069267f82B56464FBCe6Ea0",
        "signature": "0x8090f018935d4a2fe43cbb7e7a96e916742c2a40b6f05cb1959bf042fb6f9c2a313bb1d6d3e330ce626caac07b68ecbefcbe98d5c2742c98938977b13cf6517b1c",
        "chainId": 1
    }

    response = requests.post(url, headers=headers, json=data)
    return response


def main():
    """Main function to retry requests until successful"""
    attempt = 0
    retry_delay = 2  # seconds between retries

    print(f"开始请求 Hourglass API...")
    print(f"每次重试间隔: {retry_delay} 秒\n")

    while True:
        attempt += 1
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            print(f"[{timestamp}] 尝试 #{attempt}...", end=" ")
            response = make_request()

            print(f"状态码: {response.status_code}")

            if response.status_code not in [500, 403]:
                print(f"\n✓ 成功! 收到非500/403响应")
                print(f"状态码: {response.status_code}")
                print(f"响应头: {dict(response.headers)}")
                print(f"\n响应内容:")

                try:
                    print(json.dumps(response.json(), indent=2, ensure_ascii=False))
                except json.JSONDecodeError:
                    print(response.text)

                break
            else:
                print(f"  ✗ 收到{response.status_code}错误，继续重试...")
                print(f"  响应内容: {response.text[:200]}")  # 只打印前200个字符

        except requests.exceptions.RequestException as e:
            print(f"  ✗ 请求异常: {e}")
        except Exception as e:
            print(f"  ✗ 发生错误: {e}")

        # 等待后重试
        time.sleep(retry_delay)

    print(f"\n总共尝试次数: {attempt}")


if __name__ == "__main__":
    main()
