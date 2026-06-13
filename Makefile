.PHONY: test
test:
	@uv run pytest

.PHONY: docker-build
docker-build:
	docker build -f docker/Dockerfile -t ingestion_script_prod .

.PHONY: docker-run
docker-run:
	docker run --rm python_docker_local
