# syntax=docker/dockerfile:1
#
# Завдання 1 — Dockerfile для ingestor. Специфікація: ../SPEC.md → «Dockerfile».
#
# Що має бути (див. SPEC.md для деталей і балів):
#   * multi-stage build: окремий builder-стейдж ставить залежності, runtime-стейдж
#     копіює тільки готовий venv + код (без pip/uv-кешу у фінальному образі);
#   * залежності з app/requirements.txt (psycopg);
#   * НЕ запускати від root — створіть і використайте non-root користувача;
#   * точка монтування кешу (/cache) має належати цьому користувачу;
#   * ENTRYPOINT запускає app/gh_ingest.py.
#
# Перевірте збірку: docker build -t gh-ingest:latest .
# Скрипт app/gh_ingest.py — ДАНО, не редагуйте його.

# TODO: реалізуйте multi-stage Dockerfile тут.
# ---- builder: ставимо залежності в окремий venv -------------------------
FROM python:3.12-slim AS builder

WORKDIR /build
COPY app/requirements.txt .

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r requirements.txt

# ---- runtime: лише готовий venv + код, без build-інструментів -----------
FROM python:3.12-slim AS runtime

RUN useradd --uid 1000 --create-home --shell /usr/sbin/nologin appuser

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY app/gh_ingest.py .

RUN mkdir -p /cache && chown -R appuser:appuser /app /cache

USER appuser

# Бонус: перевіряє, що рантайм-образ справний — venv на місці, psycopg
# імпортується. Для короткоживучого job-контейнера це не "чи процес ще
# відповідає", а "чи образ, з якого стартували, взагалі робочий".
HEALTHCHECK --interval=5s --timeout=3s --start-period=2s --retries=3 \
    CMD python -c "import psycopg" || exit 1

ENTRYPOINT ["python", "gh_ingest.py"]
