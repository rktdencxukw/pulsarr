#!/usr/bin/env bash

curl -X POST --location "http://127.0.0.1:8182/api/x/e" -H "Content-Type: text/plain" -d "
select
  dom_all_attrs(dom, 'a.news-flash-item-title', 'href') as ids,
  dom_all_texts(dom, '.news-flash-item-title') as titles,
  dom_all_texts(dom, '.news-flash-item-content') as contents
from load_and_select('https://www.theblockbeats.info/newsflash?a=27', 'body');"
