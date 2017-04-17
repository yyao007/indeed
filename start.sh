#!/bin/bash
source ~/ENV/bin/activate
screen -dmS resume
screen -S resume -p 0 -X stuff $'scrapy crawl resume 2>&1 | tee "$(echo "$(date +%Y-%m-%d).log")" &\r'
