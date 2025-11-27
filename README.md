With this web application you can manage your Gmail with natural-language commands: list, delete, archive, label, restore, and view stats.

Works in English and Hebrew. No data is stored on the server; actions run via your Google account.

## Run with Docker

1. Copy `.env.example` to `.env` and fill in your keys (`GOOGLE_OAUTH_*`, `DJANGO_SECRET_KEY`, optional `GEMINI_API_KEY`).
2. If you use local OAuth credentials, place `credentials.json` at the project root (and uncomment the volume in `docker-compose.yml`).
3. Build and start:
   ```bash
   docker compose build
   docker compose up
   ```
   The container will run migrations on start and serve on <http://localhost:8000/agent/>.
4. To stop: `docker compose down`.

One-off commands (e.g., createsuperuser/migrations) can be run with:
```bash
docker compose run --rm web python manage.py migrate
```
