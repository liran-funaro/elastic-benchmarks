#!/bin/bash

PS=$(ps -aux)
echo -e "${PS}" | grep --color=always -P "(?<!query_)python"
