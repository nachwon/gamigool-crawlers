import asyncio
import re
from collections import namedtuple

import requests
from pyppeteer.browser import Browser

from pyppeteer import launch
from pyppeteer.errors import ElementHandleError

Post = namedtuple("Post", "ref_id, title, content, author, reg_ts, views, likes, dislikes")

MIN_LIKES = 2
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"


async def get_post_urls(browser: Browser):
    url = "https://finance.daum.net/talks"
    page = await browser.newPage()
    await page.setUserAgent(USER_AGENT)
    await page.goto(url)

    await page.waitForSelector("tbody")
    tbody = await page.J("tbody")
    trs = await tbody.querySelectorAll("tr")

    links = []
    for tr in trs[1:]:
        try:
            tds = await tr.JJ("td")
            title, likes = tds[0], tds[3]
            link: str = await title.querySelectorEval("a", "(el) => el.href")
            likes_count: int = await likes.Jeval("span", "(el) => el.textContent")
            if link.startswith("http") and int(likes_count) >= MIN_LIKES:
                links.append(link)
        except ElementHandleError:
            pass

    print(links)
    return links


async def get_post_detail(url: str, browser: Browser):
    post_id = url.split("#")[-1]
    url = f"https://board2.finance.daum.net/gaia/do/dunamu/pc/read?bbsId=10000&articleId={post_id}"
    print("start", post_id)

    try:
        print("launched!", post_id)
        page = await browser.newPage()
        await page.setUserAgent(USER_AGENT)
        await page.goto(url)
        print("goto!", post_id)
        await page.waitForSelector("#bbsTitle", timeout=10000)
    except Exception as e:
        print("Failed", e)
        return
    else:
        print("el found!")

    try:
        title: str = await page.Jeval("#bbsTitle", "(el) => el.textContent")
        content: str = await page.Jeval("#bbsContent", "(el) => el.innerText")
        author: str = await page.Jeval(".nick", "(el) => el.textContent")
        dt: str = await page.Jeval(".datetime", "(el) => el.textContent")
        views_container = await page.Jx("/html/body/div[1]/div/div[2]/span[1]/span[3]")
        views = await page.evaluate("(el) => el.textContent", views_container[0])
        likes = await page.Jeval("#approCnt", "(el) => el.textContent")
        dislikes = await page.Jeval("#opposCnt", "(el) => el.textContent")
        return Post(f"다음:{post_id}", title.strip(), content, author, dt, views, likes, dislikes)
    except ElementHandleError:
        print("not found")


def send_to_gamigool(post: Post):
    return requests.post("https://1n848veode.execute-api.ap-northeast-2.amazonaws.com/api/crawling/posts", json={
        "ref_id": post.ref_id,
        "title": re.sub(r"\[\d+]", "", post.title),
        "author": post.author,
        "content": post.content,
        "stock_name": "삼성전자",
        "likes": int(post.likes),
        "dislikes": int(post.dislikes),
        "views": int(post.views),
        "reg_ts": post.reg_ts.replace(".", "-") + ":00"
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

    links = await get_post_urls(browser)
    tasks = [get_post_detail(it, browser) for it in links]
    for task in asyncio.as_completed(tasks):
        res: Post = await task
        if not res:
            continue
        resp = send_to_gamigool(res)
        print(resp)
    await browser.close()


def lambda_handler():
    asyncio.run(main())


if __name__ == '__main__':
    lambda_handler()
