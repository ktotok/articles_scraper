#!/usr/bin/env python3
# articles_scraper.py

import asyncio
import json
import logging
import re
import sys
import time

import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup

from pathlib import Path
import aiomysql

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("articles_scraper")
logging.getLogger("chardet.charsetprober").disabled = True

ROOT_URL = "https://www.terveyskirjasto.fi/terveyskirjasto/tk.koti"
API_URL = "https://www.terveyskirjasto.fi/terveyskirjasto/terveyskirjasto.kasp_api.selaus_json?p_teos={teos}&p_selaus="
MAX_PARAGRAPH_LENGTH = 2048
DEFAULT_CONFIG_FILE_PATH = './config.json'


async def get_page(url):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                assert resp.status == 200
                return await resp.text()
    except Exception as e:
        logger.exception(f"Exception occurred:  {e}")
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
    api_url = API_URL.format(teos=teos)
    resp = await fetch_page(api_url, session, **kwargs)
    if resp:
        if resp.content_type == 'application/json':
            return await resp.text()


async def fetch_page(api_url, session, **kwargs):
    try:
        resp = await session.request(method="GET", url=api_url, **kwargs)
        resp.raise_for_status()
    except (
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError,
    ) as e:
        logger.error(f"aiohttp exception for {api_url}\nException: {e}")
        return None
    except Exception as e:
        logger.exception(
            f"Non-aiohttp exception occurred:  [{type(e).__name__}]: {e}\n"
            f"URL: {api_url}"
        )
        return None
    logger.info(f"Got response [{resp.status}] for URL: {api_url}\n"
                f"Content-type: {resp.content_type}")
    return resp


def recursive_article_list_processing(root_node_name, tree, result_article_data):
    if 'text' not in tree:
        logger.error("Current article list node does not contain 'text' ")
        logger.debug(f"Corrupted articles list json: {result_article_data}")
        raise KeyError()

    current_node_name = tree['text']
    try:
        if 'nodes' in tree:
            for node in tree['nodes']:
                list_name = ' ^ '.join([root_node_name, current_node_name])
                recursive_article_list_processing(list_name, node, result_article_data)
        else:
            if current_node_name == "New article" or 'href' not in tree:
                return
            result_article_data.append(
                {
                    'list_name': root_node_name,
                    'title': current_node_name,
                    'article_href': tree['href']
                })
    except Exception as ex:
        logger.exception(f"Failed to process articles list: [{type(ex).__name__}]: {ex}")
        logger.debug(f"Current node: {current_node_name}")
        logger.debug(f"Current sub tree: {tree}")
        logger.debug(f"Currently processed articles nodes: {result_article_data}")
        return


async def parse_articles_lists(category_url, session, **kwargs):
    """
    :param category_url: relative articles list url for specific category
    :param session: aiohttp session
    :param kwargs:
    :return: Dict:
            {
                'list_name': "concatenated list name",
                'title': current_node_name,
                'article_href': "article relative url"]
            }
    """
    articles_list_content = await fetch_articles_list_page(category_url, session=session, **kwargs)
    if not articles_list_content:
        logger.error('No articles list obtained')
        return

    try:
        articles_list = json.loads(articles_list_content)
    except Exception as e:
        logger.exception(f"JSON Encode exception:  {e}\n"
                         f"Category URL: {API_URL.format(teos=category_url)}")
        logger.debug(f"Failed json:\n: {articles_list_content}")
        return

    # TODO Candidate to be async
    if articles_list:
        root_list_name = articles_list[0]['text']
        logger.info(f"Start Parsing {root_list_name} articles list")
        result_articles_list = []
        for node in articles_list[0]['nodes']:
            recursive_article_list_processing(root_list_name, node, result_articles_list)
        logger.info(f"[{root_list_name}]: Discovered {len(result_articles_list)} articles")
        return result_articles_list
    else:
        logger.error("Empty article list obtained")


async def fetch_articles_page(category, session, **kwargs):
    articles_lists = await parse_articles_lists(category['teos'], session)

    if articles_lists:
        try:
            for article in articles_lists:
                article_url = ROOT_URL.replace("tk.koti", article['article_href'])
                html = await fetch_page(article_url, session, **kwargs)
                if not html:
                    logger.error(f"No article html obtained.\n"
                                 f"Category: {category['category_main']} - {category['subcategory_name']}\n"
                                 f"Title: {article['title']}")
                    continue

                article_id = re.search(r'p_artikkeli=(\w+)', article['article_href']).groups()[0]
                yield {
                    'list_name': article['list_name'],
                    'article_id': article_id,
                    'title': article['title'],
                    'article_html': await html.text()
                }
        except Exception as ex:
            logger.exception(f"Failed to process articles list.\n{ex}\n"
                             f"Category {category['category_main']} - {category['subcategory_name']}")
    else:
        logger.info(
            f"No articles list data obtained for category {category['category_main']} - {category['subcategory_name']}")
        return


async def parse_article(category, session):
    """
    Parse article html page provided bu sub coroutine
    :param category:
    :param session:
    :return: {
    'list_name' : 'list path'
    'title': "_article_title_"
    'keywords': '',
    'article_paragraphs';
        [
            'name': '',
            'content': ''
        ]
    }
    """
    article_contents = fetch_articles_page(category, session)

    async for article_content in article_contents:
        bs = BeautifulSoup(article_content['article_html'], 'html.parser')
        article = bs.find(id='duo-article')

        parsed_article = {
            'list_name': article_content['list_name'],
            'title': article_content['title'],
            'article_id': article_content['article_id']
            }
        meta_keywords = article.find('meta', {'name': 'keywords'})
        if meta_keywords:
            keywords = meta_keywords.attrs['content']
            parsed_article['keywords'] = keywords

        h1 = article.h1
        sections = article.select(".section")
        for sec in sections:
            name = h1.text[:8]

            h2_name = ""
            h3_name = ""
            h2 = sec.find('h2', recursive=False)

            if h2:
                h2_name = h2.text

            h3 = sec.find('h3', recursive=False)
            if h3:
                h3_name = h3.text

            all_paragraphs = sec.find_all("p", recursive=False)
            p_content = []

            current_p_length = 0
            for p in all_paragraphs:
                if len(p.text) + current_p_length > MAX_PARAGRAPH_LENGTH:
                    break
                p_content.append(p.text)
                current_p_length += len(p.text)
            p_content_str = "".join(p_content)

            if 'article_paragraphs' not in parsed_article:
                parsed_article['article_paragraphs'] = []
            parsed_article['article_paragraphs'].append({
                'name': name,
                'content': p_content_str,
                'h2': h2_name,
                'h3': h3_name
            })
        yield parsed_article


def load_config(filepath=DEFAULT_CONFIG_FILE_PATH):
    config = {}
    config_file = Path(filepath)
    if config_file.exists():
        with open(filepath) as f:
            config = json.load(f)
    if not config:
        print(f'Error: Failed to read config file "{filepath}"\n')
        sys.exit(3)
    return config


async def store_to_db(db_cfg, category, session, **kwargs):
    async for articles_obj in parse_article(category, session):

        async with aiomysql.create_pool(host=db_cfg['host'], port=db_cfg['port'],
                                        user=db_cfg['user'], password=db_cfg['password'],
                                        db=db_cfg['dbname'], echo=db_cfg['echo']) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    create_content_table = """
                        CREATE TABLE IF NOT EXISTS content (
                          id INT AUTO_INCREMENT, 
                          description TEXT, 
                          text TEXT,
                          PRIMARY KEY (id)
                        ) ENGINE = InnoDB
                        """
                    await cur.execute(create_content_table)

                    create_articles_table = """
                        CREATE TABLE IF NOT EXISTS articles (
                          id INT AUTO_INCREMENT, 
                          main_category TEXT NOT NULL, 
                          sub_category TEXT, 
                          list_name TEXT, 
                          article_id TEXT NOT NULL, 
                          article_name TEXT NOT NULL,
                          h2_name TEXT NOT NULL,
                          h3_name TEXT NOT NULL, 
                          keywords TEXT,
                          content_id INTEGER NOT NULL, 
                          PRIMARY KEY (id),
                          FOREIGN KEY fk_content_id (content_id) REFERENCES content(id)
                        ) ENGINE = InnoDB
                        """

                    await cur.execute(create_articles_table)

                    for a in articles_obj['article_paragraphs']:
                        try:
                            add_article_content = f"INSERT INTO content (description, text)" \
                                                  f"VALUES (%s, %s)"
                            await cur.execute(add_article_content, [a['name'], a['content']])
                            await conn.commit()
                        except Exception as ex:
                            logger.exception(f"Failed to add article content: {ex}. \n{add_article_content}")

                        try:
                            add_article = f"INSERT INTO articles " \
                                          f"(main_category, sub_category, list_name, article_id, article_name, " \
                                          f"h2_name, h3_name, keywords, content_id)" \
                                          f"VALUES (" \
                                          f"'{category['category_main']}', " \
                                          f"'{category['subcategory_name']}', " \
                                          f"'{articles_obj['list_name']}'," \
                                          f"'{articles_obj['article_id']}'," \
                                          f"'{articles_obj['title']}', " \
                                          f"'{a['h2']}', " \
                                          f"'{a['h3']}', " \
                                          f"'{articles_obj['keywords']}', " \
                                          f"{cur.lastrowid})"

                            await cur.execute(add_article)
                            await conn.commit()
                        except Exception as ex:
                            logger.exception(f"Failed to add article meta: {ex}. \n{add_article}")



async def bulk_crawl_and_store(db_cfg, categories, **kwargs):
    """
    Crawl each subcategory page with list of articles, parsing eash article,
    format required data structure and flush it to specified DB
    :param db_cfg:
    :param categories:
    :return:
    """

    async with ClientSession() as session:
        tasks = []
        for cat in categories:
            tasks.append(
                store_to_db(db_cfg=db_cfg, category=cat, session=session, **kwargs)
            )
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    config = load_config()
    if 'LOGGING' in config:
        log_cfg = config['LOGGING']
        if 'level' in log_cfg:
            logger.setLevel(log_cfg['level'])

    if 'DATABASE' not in config:
        raise Exception("Database configuration does not should. make sure your config json is correct")

    main_page = asyncio.run(get_page(ROOT_URL))
    categories = parse_categories(main_page)

    start = time.perf_counter()
    asyncio.run(bulk_crawl_and_store(db_cfg=config['DATABASE'], categories=categories))
    duration = time.perf_counter() - start
    logger.info("Completed for {:4.2f} seconds.".format(duration))