import re
import boto3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

BASE_URL = "https://finance.naver.com"
UA = UserAgent()
HEADERS = {
    "User-Agent": UA.random,
}


def get_search_trend_stock(n: int = 30):
    """
    네이버 검색상위 n개 종목과 그 코드를 df로 출력
    :param n: 가져올 상위 종목의 갯수 (0 < n <= 30)
    :return:
    """
    print("Fetching search trend stock list")
    if (n > 30) or (n < 0):
        raise Exception("n must be between 0 and 30")

    url = BASE_URL + "/sise/lastsearch2.naver"
    r = requests.get(url, headers=HEADERS)
    html = r.text
    soup = BeautifulSoup(html, "html.parser")
    li = soup.find_all("a", "tltle")

    symbols = []
    codes = []
    for stock in li:
        symbol = stock.text
        code = re.search(r"(?<==).*", stock["href"])[0]

        symbols.append(symbol)
        codes.append(code)

    return zip(symbols, codes)


def main():
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='naver-financial-queue')
    trend_stock_df = get_search_trend_stock()

    for symbol, code in trend_stock_df:
        queue.send_message(MessageBody=f"{symbol}:{code}")


def lambda_handler(event, context):
    main()
