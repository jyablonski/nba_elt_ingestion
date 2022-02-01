FROM python:3.8-slim as base

# Setup env
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONFAULTHANDLER 1


FROM base AS python-deps

# Install security updates, git, and compilation dependencies for pipenv
COPY install_packages.sh .
RUN ./install_packages.sh

# Install python dependencies in /.venv
COPY Pipfile .
COPY Pipfile.lock .
RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy


FROM base AS runtime

# only copy the env, not what we used to build it
COPY --from=python-deps /.venv /.venv
ENV PATH="/.venv/bin:$PATH"

# Create and switch to a new user for ,,, security reasons
RUN useradd --create-home appuser
WORKDIR /home/appuser
USER appuser

# copy all files in repo over
COPY . .
RUN mkdir logs
RUN cat > logs/example.log
RUN [ "python3", "-c", "import nltk; nltk.download('vader_lexicon')" ]

CMD ["python3", "app.py"]
