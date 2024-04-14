FROM python:3.11-slim as base

ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1

FROM base AS python-deps

COPY docker/install_packages.sh .
COPY pyproject.toml .
COPY poetry.lock .

RUN ./install_packages.sh
RUN pip install poetry
RUN poetry install

FROM base AS runtime

COPY --from=python-deps ./.venv ./.venv
ENV PATH="/.venv/bin:$PATH"

RUN useradd --create-home appuser
WORKDIR /home/appuser
USER appuser

COPY src/ ./src

RUN mkdir logs
RUN cat > logs/example.log
RUN ["python3", "-c", "import nltk; nltk.download('vader_lexicon')"]

CMD ["python3", "-m", "src.app"]
