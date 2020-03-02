#!/usr/bin/env python3
# articles_scraper.py

import asyncio
import json
import logging
import re
import sys
import urllib
import requests
import aiofiles
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("articles_scraper")
logging.getLogger("chardet.charsetprober").disabled = True
"tk.koti?p_artikkeli=far00607&p_teos=far"
ROOT_URL = "https://www.terveyskirjasto.fi/terveyskirjasto/tk.koti"
API_URL = "https://www.terveyskirjasto.fi/terveyskirjasto/terveyskirjasto.kasp_api.selaus_json?p_teos={teos}&p_selaus="


async def get_page(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                assert resp.status == 200
                return await resp.text()
    except Exception as e:
        logger.exception("Exception occurred:  %s", getattr(e, "__dict__", {}))
    return None


def parse_categories(main_page):
    """
    Parse main page left menu to populate list of dict with categories, subcategories and corespondent URLs
    :param main_page: URL
    :return: List of dicts
    e.g. {
            "category_main": cat_name,
            "subcategory_name": sub_cat_name,
            "teos": teos # link to articles list
        }
    """
    if not main_page:
        logger.error("Main page is empty. Nothing to parse")
    categories_output = []

    bs = BeautifulSoup(main_page, 'html.parser')
    categories_root = bs.find(id="vakionavi")

    try:
        for cat in categories_root.children:
            if cat != "\n":
                for a in cat.find_all("a"):
                    # validate whether it is a main menu item
                    if "class" in a.attrs and 'main-menu-item' in a.attrs["class"]:
                        cat_name = a.text
                    else:
                        teos = a.attrs['href'].split("=")[1]
                        sub_cat_name = a.text
                        categories_output.append(
                            {
                                "category_main": cat_name,
                                "subcategory_name": sub_cat_name,
                                "teos": teos
                            }
                        )
    except Exception as e:
        logger.exception("Parsing Exception occurred:  %s", getattr(e, "__dict__", {}))
        return None
    logging.info(f"Successfully Parsed {len(categories_output)} categories")
    return categories_output


async def fetch_articles_list_page(teos, session, **kwargs):
    # api_url = API_URL.format(teos=teos)
    api_url = "https://www.terveyskirjasto.fi/terveyskirjasto/terveyskirjasto.kasp_api.selaus_json?p_teos=far&p_selaus="
    return await fetch_html(api_url, session, **kwargs)


async def fetch_html(api_url, session, **kwargs):
    try:
        resp = await session.request(method="GET", url=api_url, **kwargs)
    except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            api_url,
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return None
    except Exception as e:
        logger.exception(
            "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        )
        return None

    resp.raise_for_status()
    logger.info(f"Got response [{resp.status}] for URL: {api_url}")
    return await resp.text()

def recursive_article_list_processing(root_node_name, tree, result_article_data):
    current_node_name = tree['text']
    if 'nodes' in tree:
        for node in tree['nodes']:
            list_name = ' ^ '.join([root_node_name, current_node_name])
            recursive_article_list_processing(list_name, node, result_article_data)
    else:
        if current_node_name == "New article":
            return
        result_article_data.append((root_node_name, current_node_name, tree['href']))


async def parse_articles_lists(category_url, session, **kwargs):
    """
    :param category_url: relative articles list url for specific category
    :param session: aiohttp session
    :param kwargs:
    :return: Tuple( concatenated list name, article name, article relative url)
    """
    articles_list_content = await fetch_articles_list_page(category_url, session=session, **kwargs)
    try:
        articles_list = json.loads(articles_list_content)
    except Exception as e:
        logger.exception(
            "JSON Encode ecxeption:  %s", getattr(e, "__dict__", {})
        )
        logger.debug(f"Failed json:\n: {articles_list_content}")

    # TODO Candidate to be async
    if articles_list:
        root_list_name = articles_list[0]['text']
        logger.info(f"Start Parsing {root_list_name} articles list")
        result_articles_list = []
        for node in articles_list[0]['nodes']:
            recursive_article_list_processing(root_list_name, node, result_articles_list)
        logger.info(f"[{root_list_name}]: Discovered {len(result_articles_list)} articles")
        return result_articles_list


async def fetch_article_page(category_url, session, **kwargs):
    articles_lists = await parse_articles_lists(category_url, session)

    # article
    # Tuple: concatenated list name, article name, article relative url
    for article in articles_lists:
        article_url = ROOT_URL.replace("tk.koti", article[2])
        return await fetch_html(article_url, session, **kwargs)


async def parse_article(category, session):
    article_content = await fetch_article_page(category['teos'], session)

    # article_content = requests.get(article_url).content
    bs = BeautifulSoup(article_content, 'html.parser')
    article = bs.find(id="duo-article")
    meta_keywords = article.find('meta', {'name':'keywords'})
    if meta_keywords:
        keywords = meta_keywords.attrs['content']
    return {"keywords": keywords}


async def store_to_db(db, category, session, **kwargs):
    # category obj provides
    # "category_main": cat_name,
    # "subcategory_name": sub_cat_name,
    # "teos": teos --- URL
    articles_obj = await parse_article(category, session)
    assert articles_obj

    # store to DB (ID, main_category, sub_category, list_name, article_name, keywords, h2, h3, content_id)
    # (content_id, article_id, article_name, tag, text)


async def bulk_crawl_and_store(db, categories, **kwargs):
    """
    Crawl each subcategory page with list of articles, parsing eash article,
    format required data structure and flush it to specified DB
    :param db:
    :param categories:
    :return:
    """
    async with ClientSession() as session:
        tasks = []
        # TODO
        # for cat in categories:
        #     tasks.append(
        #         store_to_db(db=db, category=cat, session=session, **kwargs)
        #     )
        tasks.append(
            store_to_db(db=db, category=categories[0], session=session, **kwargs))
        await asyncio.gather(*tasks)


if __name__ == "__main__":

    # Get Categories-URLs tree
    main_page = asyncio.run(get_page(ROOT_URL))
    categories = parse_categories(main_page)
    db = None
    asyncio.run(bulk_crawl_and_store(db=db, categories=categories))