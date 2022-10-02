import re
from collections import namedtuple

import boto3
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

BASE_URL = "https://finance.naver.com"
UA = UserAgent()
HEADERS = {
    "User-Agent": UA.random,
}
Post = namedtuple(
    "Post",
    "ref_id, title, content, stock_name, author,reg_ts, views, likes, dislikes",
)


def fetch_by_post(top_post, symbol):
    """
    post의 내용을 크롤링하는 메소드
    comment의 경우 일시적으로 가져오지 않는다.
    :param session:
    :param top_post:
    :param symbol:
    :return:
    """
    href = top_post.get("href")
    response = requests.get(BASE_URL + href, headers=HEADERS)
    html = response.text
    content_soup = BeautifulSoup(html, "html.parser")

    author = content_soup.select_one(
        "th > span.gray03 > strong"
    ).text.replace(" ", "")
    date = content_soup.select_one("tr > th.gray03.p9.tah").text

    post_info = content_soup.select_one("tr > th:nth-of-type(2)")
    post_info = post_info.getText(",", strip=True).split(",")

    content = content_soup.select_one("#body")
    content = content.getText().replace("\xa0\r", "\n")
    content = content.replace("\r", "\n")
    nid = int(re.search(r'(?<=nid=)[0-9]+', href)[0])
    # comments = fetch_comments_by_post(nid) #비동기로 바꿔줘야함

    post = {}
    post["title"] = top_post.get("title")
    post["author"] = author
    post["content"] = content
    post["stock_name"] = symbol
    post["likes"] = post_info[3]
    post["dislikes"] = post_info[5]
    post["views"] = post_info[1]
    post["reg_ts"] = date.replace(".", "-") + ":00"
    post["ref_id"] = f"네이버파이낸스:{nid}"
    # post['comments'] = comments
    print(post)
    return send_to_ilgaminati(post=post)


def fetch_by_page(symbol: str, code: str, page: int, standard):
    """
    한 게시판 페이지 내의 글들을 크롤링하는 메소드
    :param code: 종목코드
    :param page: 페이지 번호
    :return: 한 페이지의 게시글들을 dict of list 로 반환.
    """
    print(f"Fetching {symbol} board's page #{page}")
    url = BASE_URL + "/item/board.naver?code=" + code + "&page=%d" % page
    response = requests.get(url, headers=HEADERS)
    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    title_list = soup.select("td.title > a")
    agree_list = soup.select("td:nth-child(5) > strong")
    idx_list = [
        idx
        for idx, agree in enumerate(agree_list)
        if int(agree.text) >= standard
    ]
    top_post_list = [title_list[x] for x in idx_list]

    for top_post in top_post_list:
        fetch_by_post(top_post, symbol)


def send_to_ilgaminati(post: dict):
    resp = requests.post(
        "https://1n848veode.execute-api.ap-northeast-2.amazonaws.com/api/crawling/posts",
        json={
            "ref_id": post["ref_id"],
            "title": post["title"],
            "author": post["author"],
            "content": post["content"],
            "stock_name": post["stock_name"],
            "likes": int(post["likes"]),
            "dislikes": int(post["dislikes"]),
            "views": int(post["views"]),
            "reg_ts": post["reg_ts"],
        },
    )
    print("Send Result: ", resp)


def lambda_handler(event, context):
    symbol, code = event["Records"][0]["body"].split(":")
    for i in range(30):
        fetch_by_page(
            symbol=symbol,
            code=code,
            page=i + 1,
            standard=20,
        )
