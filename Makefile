.PHONY: test
test:
	@docker compose -f docker/docker-compose-test.yml down
	@docker compose -f docker/docker-compose-test.yml up --exit-code-from ingestion_script_test_runner

.PHONY: docker-build
docker-build:
	docker build -f docker/Dockerfile -t ingestion_script_prod .

.PHONY: docker-run
docker-run:
	docker run --rm python_docker_local

.PHONY: start-postgres
start-postgres:
	@docker compose -f docker/docker-compose-test.yml up -d postgres

.PHONY: stop-postgres
stop-postgres:
	@docker compose -f docker/docker-compose-test.yml down

.PHONY: ci-test
ci-test:
	@make start-postgres
	@uv run --frozen pytest -vv --cov-report term --cov-report xml:coverage.xml --cov=src --color=yes
	@make stop-postgres
