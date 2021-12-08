# Lints all python files
.PHONY: lint
lint: 
	black app.py utils.py tests/conftest.py tests/unit_test.py

.PHONY: create-venv
create-venv:
	pipenv install

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

# use to untrack all files and subsequently retrack all files, using up to date .gitignore
.PHONY: git-reset
git-reset:
	git rm -r --cached .
	git add .