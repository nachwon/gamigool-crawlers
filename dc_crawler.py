import asyncio
import re
from asyncio import as_completed
from collections import namedtuple
from urllib.parse import parse_qs, urlparse

import requests
from pyppeteer import launch
from pyppeteer.browser import Browser

Post = namedtuple("Post", "ref_id, title, content, author, reg_ts, views, likes, dislikes")


async def get_post_urls(browser: Browser):
    url = "https://gall.dcinside.com/mgallery/board/lists/?id=stockus&sort_type=N&exception_mode=recommend&search_head=110&page=1"
    page = await browser.newPage()
    await page.goto(url, options={"timeout": 0})
    await page.content()
    await page.waitForSelector("tbody")
    tbody = await page.J("tbody")
    trs = await tbody.JJ(".us-post")

    links = []
    for tr in trs:
        href = await tr.Jeval("a", "(el) => el.href")
        print("Link Found", href)
        links.append(href)
        if len(links) == 3:
            print("YIELD!")
            yield links
            links = []
    else:
        print("FINAL!", links)

        yield links
    await page.close()


async def get_post(link: str, browser: Browser):
    print(link, " started")
    page = await browser.newPage()
    await page.goto(link, options={"timeout": 0})
    await page.waitForSelector(".view_content_wrap")
    article = await page.J(".view_content_wrap")
    header = await article.J("header")
    post_id = parse_qs(urlparse(link).query)['no'][0]
    ref_id = f"디씨:{post_id}"
    title = await header.Jeval(".title_subject", "(el) => el.textContent")
    sanitized_title = re.sub(r"\[\d+]", "", title)
    author = await header.Jeval(".nickname", "(el) => el.title")
    reg_ts = await header.Jeval(".gall_date", "(el) => el.title")
    views = await header.Jeval(".gall_count", "(el) => el.textContent")

    content = await page.Jeval(".write_div", "(el) => el.innerText")

    count_box = await page.J(".btn_recommend_box")
    likes = await count_box.Jeval(".up_num", "(el) => el.textContent")
    dislikes = await count_box.Jeval(".down_num", "(el) => el.textContent")

    post = Post(ref_id, sanitized_title, content, author, reg_ts, views.split(" ")[-1], likes, dislikes)
    print(post)
    await page.close()
    return post


def send_to_gamigool(post: Post):
    return requests.post("https://1n848veode.execute-api.ap-northeast-2.amazonaws.com/api/crawling/posts", json={
        "ref_id": post.ref_id,
        "title": post.title,
        "author": post.author,
        "content": post.content,
        "stock_name": "해외 주식",
        "likes": int(post.likes),
        "dislikes": int(post.dislikes),
        "views": int(post.views),
        "reg_ts": post.reg_ts
    })


async def main():
    browser = await launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--single-process",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-zygote",
        ],
    )
    async for links in get_post_urls(browser):
        print("crawling links chunk", links)
        tasks = [get_post(link, browser) for link in links]
        for task in as_completed(tasks):
            try:
                post = await task
                res = send_to_gamigool(post)
                print("SENT TO GMG", res)
            except Exception as e:
                print("timeout!", e)
                raise e

        print("chunk done!")
        await asyncio.sleep(5)

    await browser.close()


def lambda_handler(event, context):
    asyncio.run(main())


if __name__ == '__main__':
    asyncio.run(main())
