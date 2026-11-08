# Putting the dashboard online (for free, on Render)

[Français](../DEPLOIEMENT.md) · **English**

The full stack (Kafka, Spark, PostGIS, Airflow, Grafana) needs about 6 GB of memory, which is too much
for free hosting. So the online version works like this:

```
your computer: full stack ──export──▶ dashboard/data/snapshot.json ──git push──▶ Render (free)
```

- `scripts/export_snapshot.py` runs **the same SQL queries** as the live dashboard
  (`dashboard/queries.py`), for each time range (1 h, 6 h, 24 h), in one consistent database snapshot.
- The result is saved in `dashboard/data/snapshot.json` (a few hundred KB), which is committed.
- On Render, the same container runs with `DEMO_MODE=1`: it answers from that file, with no database.
  A banner at the top of the page shows when the copy was made.

No online database, no password to manage, no credit card. It takes about 15 minutes.

---

## Step 1: Have the project on GitHub

The repository must be on GitHub (public or private). `.env` is in `.gitignore`, so your local
passwords are never pushed.

## Step 2 (optional): Refresh the data copy

The repository already contains a copy. To make a new one from your own run:

1. Start the stack and let it run (the 24-hour history is sent automatically on the first start).
2. Run the Airflow DAG once so the `analytics` tables are up to date:

```bash
docker compose exec airflow airflow dags test mobility_analytics
```

3. Export the copy (the script reads the password from `.env`):

```bash
python scripts/export_snapshot.py
```

4. Commit `dashboard/data/snapshot.json` and push.

## Step 3: Create the service on Render

1. Go to https://render.com and sign in with your **GitHub** account.
2. Click **New** → **Blueprint**.
3. Pick the `smart-city-mobility-data-platform` repository. Render reads `render.yaml` at the root:
   - type: web service, **Free** plan
   - image: the `Dockerfile` at the repository root
   - environment variable: `DEMO_MODE=1`
   - health check: `/healthz`
4. Click **Apply**. The first build takes 3 to 5 minutes.
5. Your dashboard is online at an address like `https://smart-city-mobility-data-platform-1.onrender.com`.

**Without a Blueprint (simplest):** New → Web Service → pick the repository → Language **Docker** →
keep the defaults (Dockerfile path `./Dockerfile`, Docker build context `.`) → Instance type **Free** →
Create Web Service. The `Dockerfile` at the repository root already runs the dashboard in demo mode.
Do **not** set `render.yaml` as the Dockerfile path: it is not a Dockerfile.

## Step 4: Add the link to the README

At the top of `README.md` and `README.en.md`, replace the demo address with yours, then commit and push.
Render redeploys automatically on every push to `main`.

---

## Good to know

- **Sleep mode:** on the free plan, Render puts the service to sleep after 15 minutes without visits.
  The next visit wakes it up in about 30 to 60 seconds. Open the link yourself a few minutes
  before sending it to a recruiter.
- **Other free hosts:** the same `dashboard/Dockerfile` works on other platforms that accept a Docker
  image (for example Koyeb or Fly.io). Just set `DEMO_MODE=1` and listen on the port given by `PORT`.
- **Live version:** a real live dashboard online would need Kafka, Spark and PostGIS hosted somewhere
  (for example on a virtual machine). That is not possible for free, which is why the online demo is a
  copy, and the page says so clearly.
- **A problem?** In Render, open the service → **Logs**.
