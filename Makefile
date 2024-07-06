start: ## Make your container install the depencies
	docker compose up airflow-init

up: ## Start the app
	docker compose up -d

down: ## Shut down the app
	docker compose down