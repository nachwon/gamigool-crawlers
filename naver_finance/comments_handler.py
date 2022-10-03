import json
import re

import requests
from fake_useragent import UserAgent

BASE_URL = "https://finance.naver.com"
UA = UserAgent()
HEADERS = {
    "User-Agent": UA.random,
}


def send_to_ilgaminati(nid: int, comments_to_send: list):
    resp = requests.post(
        "https://1n848veode.execute-api.ap-northeast-2.amazonaws.com/api/crawling/comments",
        json={
            "ref_id": f"네이버파이낸스:{nid}",
            "comments": comments_to_send
        },
    )
    print("Send Result: ", resp)


def fetch_comments_by_post(nid):
    comment_url = (
        "https://apis.naver.com/commentBox/cbox/"
        "web_naver_list_jsonp.json?ticket=finance"
        "&templateId=default&pool=cbox12&lang=ko&"
        f"country=KR&objectId={nid}"
    )
    headers = {
        "User-Agent": UA.random,
        "referer": "https://finance.naver.com/item/board_read.naver?"
                   f"code=112040&nid={nid}&st=&sw=&page=1",
    }
    response = requests.get(comment_url, headers=headers)
    html = response.text

    comments_list = re.findall(
        r'(?<={"commentList":).*(?=,"pageModel")', html
    )[0]
    comments_list = json.loads(comments_list)

    comments_to_send = []
    keys = [
        "commentNo",
        "contents",
        "replyAllCount",
        "userName",
        "modTime",
        "sympathyCount",
        "antipathyCount",
    ]

    for comment in comments_list:
        d = {x: comment[x] for x in keys}
        comments_to_send.append(
            {
                "comment_ref_id": f"네이버파이낸스:{nid}:{d['commentNo']}",
                "content": d["contents"],
                "author": d["userName"],
                "reg_ts": d["modTime"],
                "upd_ts": d["modTime"],
                "likes": d["sympathyCount"],
                "dislikes": d["antipathyCount"]
            }
        )
    send_to_ilgaminati(nid, comments_to_send)


def lambda_handler(event, context):
    nids = event["Records"][0]["Sns"]["Message"].split(",")
    for nid in nids:
        fetch_comments_by_post(nid)
