Web srapper script.

Prerequisite:
Python >=3.7.1 

Script parse catogories list and gather links to each of them.

Concurrently fetched list for wach of category and coresponden subcategory.
Gather articke name, path in a list and link to article.

Get article content ant scrape its content dividing by hader tags.

Discoverd data stores to MySQL DB as following tables:

CREATE TABLE IF NOT EXISTS content (
                          id INT AUTO_INCREMENT,
                          description TEXT,
                          text TEXT,
                          PRIMARY KEY (id)
                        ) ENGINE = InnoDB

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

![Image of web page]
(https://share.getcloudapp.com/RBudyvvd)
