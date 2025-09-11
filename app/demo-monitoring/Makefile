.PHONY: up down restart logs nuke

up:
\tdocker compose up -d --build

down:
\tdocker compose down --remove-orphans

restart: down up

logs:
\tdocker compose logs -f --tail=200

nuke:
\tdocker compose down --remove-orphans -v || true
\tdocker ps -aq | xargs -r docker rm -f
\tdocker volume ls -q | xargs -r docker volume rm
\tdocker network ls --filter "name=monitor" -q | xargs -r docker network rm || true
