# Lints all python files
.PHONY: lint
lint: 
	black app.py

.PHONY: venv
venv:
	pipenv shell

.PHONY: test
test:
	pytest

.PHONY: docker-build
docker-build:
	docker build -t python_docker_local .

.PHONY: docker-run
docker-run:
	docker run --rm python_docker_local