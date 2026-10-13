# Distributed Task Scheduler — Cloud-ready Streamlit app

A browser-based micro-project demo for distributed-computing concepts: task submission, a pending queue, least-loaded-worker selection, concurrent worker pools, task monitoring, retry, and SQLite task history.

## Run it without installing anything locally

Deploy it to Streamlit Community Cloud from GitHub:

1. Download and extract this ZIP.
2. Sign in to GitHub and create a **new repository** (for example `distributed-task-scheduler`).
3. Upload all files and folders from this project to the repository root. Make sure `app.py`, `requirements.txt`, and `.streamlit/config.toml` are at the root / expected path.
4. Open <https://share.streamlit.io/> and sign in with GitHub.
5. Select **Create app**, choose your repository, branch (`main`), and main file path `app.py`.
6. Select **Deploy**. Streamlit installs `requirements.txt` in its hosted environment; nothing needs to be installed on your computer.
7. When deployment finishes, open the generated app URL and submit sample tasks.

## Demo tasks

| Task | Input | Expected result |
|---|---|---|
| Uppercase | `distributed systems` | `DISTRIBUTED SYSTEMS` |
| Sum | `10,20,30` | `60` |
| Fibonacci | `30` | `832040` |
| Sleep | `3` | Finishes after about 3 seconds |
| Word count | `hello distributed world` | 3 words |
| Prime count | `10000` | 1229 |

Submit 10 sleep tasks to see jobs queued and distributed across workers with free capacity. Change a worker's status to Offline to demonstrate worker availability and load-aware assignment. A task already running when its worker is toggled offline is not forcibly terminated.

## Project files

- `app.py` — Streamlit UI, scheduler, worker pools, task execution, SQLite records.
- `requirements.txt` — hosted Python dependencies.
- `.streamlit/config.toml` — theme and hosted server settings.
- `.gitignore` — excludes generated local files and secrets.

## Architecture and important honesty note

`Browser → Streamlit Client → Scheduler / Queue → Worker 1 / Worker 2 / Worker 3 → SQLite task history`

This is the **cloud-deployable demonstration edition**. To make deployment simple and avoid requiring the user to install or operate several services, worker pools are separate concurrent executors inside the same hosted app process. It demonstrates scheduling, concurrent processing, worker availability toggles, results and persistence during the app instance lifetime. It does **not** implement gRPC between separate machines, replicated scheduling, or durable storage across all cloud restarts. Streamlit Community Cloud may reset local SQLite data after a restart or redeploy.

For a strict implementation matching a distributed-cluster diagram, the next deployment tier should use separate scheduler and worker services communicating over gRPC, with a managed database. This needs a multi-service host rather than a single Streamlit app deployment.

## Troubleshooting

- **App fails to start:** check that `app.py` is at the repository root and the deployed branch contains `requirements.txt`.
- **Changes don't appear:** push/commit changes to GitHub; Streamlit Cloud normally redeploys automatically.
- **Task is queued:** all workers may be at capacity or offline. Wait for slots to free up or bring a worker online.
- **History disappeared after restart:** this can happen because hosted local storage is ephemeral. Use a managed database for durable history.
