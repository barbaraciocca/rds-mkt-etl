init:
	docker compose up airflow-init

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

restart:
	docker compose down
	docker compose up -d


clean:
	docker compose down -v
	rm -rf logs/*

