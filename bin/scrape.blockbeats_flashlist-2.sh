#!/usr/bin/env bash

curl -X POST --location "http://localhost:8182/api/x/e" -H "Content-Type: text/plain" -d "
  select
      dom_all_texts(dom, '.news-flash-item-title') as Titles,
      dom_all_texts(dom, '.news-flash-item-content') as Contents
  from load_and_select('https://www.theblockbeats.info/newsflash?a=2', 'div.flash-list');"
