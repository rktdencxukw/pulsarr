#!/usr/bin/env bash

curl -X POST --location "http://127.0.0.1:8182/api/x/e" -H "Content-Type: text/plain" -d "
  select
      dom_base_uri(dom) as url,
      dom_first_text(dom, '.flash-title') as title,
      dom_first_text(dom, '.flash-content') as content
  from load_and_select('https://www.theblockbeats.info/flash/134063 -i 20s -njr 3', 'body');"
