from __future__ import annotations
import json, sqlite3, threading, time, uuid, math
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
import streamlit as st
import pandas as pd

APP_DIR = Path(__file__).parent
DB_PATH = Path(__import__('os').environ.get('TASK_DB_PATH', str(APP_DIR / 'tasks.db')))

TASK_TYPES = ['uppercase', 'sum', 'fibonacci', 'sleep', 'word_count', 'prime_count']

class Scheduler:
    """Cloud-friendly scheduler demo: independent worker pools within one hosted service."""
    def __init__(self, worker_count=3, capacity=2):
        self.lock = threading.RLock()
        self.worker_count = worker_count
        self.capacity = capacity
        self.workers = {}
        self.tasks = {}
        self.executors = {}
        self.running = True
        self._init_db()
        self._make_workers(worker_count, capacity)
        self._load_recent_tasks()

    @contextmanager
    def _connect(self):
        con = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _init_db(self):
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute('''CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY, task_type TEXT NOT NULL, input_value TEXT NOT NULL,
                status TEXT NOT NULL, worker_id TEXT, result TEXT, error TEXT,
                created_at REAL NOT NULL, started_at REAL, finished_at REAL,
                attempts INTEGER NOT NULL DEFAULT 0, duration REAL)''')

    def _make_workers(self, count, capacity):
        with self.lock:
            for ex in self.executors.values(): ex.shutdown(wait=False, cancel_futures=True)
            self.worker_count, self.capacity = count, capacity
            self.workers, self.executors = {}, {}
            for i in range(1, count + 1):
                wid = f'worker-{i}'
                self.workers[wid] = {'worker_id': wid, 'status': 'Online', 'capacity': capacity,
                                     'active': 0, 'completed': 0, 'failed': 0, 'last_seen': time.time()}
                self.executors[wid] = ThreadPoolExecutor(max_workers=capacity, thread_name_prefix=wid)

    def configure(self, count, capacity):
        if count == self.worker_count and capacity == self.capacity: return
        self._make_workers(count, capacity)

    def _load_recent_tasks(self):
        try:
            with self._connect() as con:
                rows = con.execute('SELECT * FROM tasks ORDER BY created_at DESC LIMIT 100').fetchall()
            for row in rows:
                item = dict(row)
                if item['status'] in ('Queued', 'Running'):
                    item['status'] = 'Interrupted'
                    item['error'] = 'App restarted before task completion; submit again to retry.'
                    self._persist(item)
                self.tasks[item['id']] = item
        except sqlite3.Error:
            pass

    def _persist(self, t):
        with self._connect() as con:
            con.execute('''INSERT INTO tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET status=excluded.status, worker_id=excluded.worker_id,
            result=excluded.result, error=excluded.error, started_at=excluded.started_at,
            finished_at=excluded.finished_at, attempts=excluded.attempts, duration=excluded.duration''',
            (t['id'], t['task_type'], t['input_value'], t['status'], t.get('worker_id'), t.get('result'),
             t.get('error'), t['created_at'], t.get('started_at'), t.get('finished_at'),
             t.get('attempts', 0), t.get('duration')))

    def submit(self, task_type, input_value):
        tid = uuid.uuid4().hex[:10]
        task = {'id': tid, 'task_type': task_type, 'input_value': str(input_value), 'status': 'Queued',
                'worker_id': None, 'result': None, 'error': None, 'created_at': time.time(),
                'started_at': None, 'finished_at': None, 'attempts': 0, 'duration': None}
        with self.lock:
            self.tasks[tid] = task
            self._persist(task)
        self._dispatch(tid)
        return tid

    def submit_batch(self, task_type, input_value, count):
        return [self.submit(task_type, input_value) for _ in range(count)]

    def _dispatch(self, tid):
        with self.lock:
            task = self.tasks.get(tid)
            if not task or task['status'] not in ('Queued', 'Retrying'): return
            online = [w for w in self.workers.values() if w['status'] == 'Online' and w['active'] < w['capacity']]
            if not online:
                # Leave it queued; completion of a running task will trigger another dispatch pass.
                return
            worker = min(online, key=lambda w: (w['active'] / max(w['capacity'], 1), w['active'], w['worker_id']))
            wid = worker['worker_id']
            worker['active'] += 1; worker['last_seen'] = time.time()
            task.update(status='Running', worker_id=wid, started_at=time.time(), attempts=task.get('attempts', 0)+1)
            self._persist(task)
            self.executors[wid].submit(self._execute, tid, wid)

    def _run_task(self, task_type, raw):
        if task_type == 'uppercase': return raw.upper()
        if task_type == 'sum':
            nums = [float(x.strip()) for x in raw.split(',') if x.strip()]
            if not nums: raise ValueError('Enter comma-separated numbers, e.g. 10,20,30')
            total = sum(nums)
            return int(total) if total.is_integer() else total
        if task_type == 'fibonacci':
            n = int(raw)
            if not 0 <= n <= 5000: raise ValueError('Choose n between 0 and 5000.')
            a, b = 0, 1
            for _ in range(n): a, b = b, a+b
            return a
        if task_type == 'sleep':
            seconds = float(raw)
            if not 0 <= seconds <= 30: raise ValueError('Sleep duration must be 0–30 seconds.')
            time.sleep(seconds)
            return f'Finished simulated workload after {seconds:g} seconds'
        if task_type == 'word_count': return {'words': len(raw.split()), 'characters': len(raw), 'input': raw}
        if task_type == 'prime_count':
            n = int(raw)
            if not 2 <= n <= 200000: raise ValueError('Choose an upper limit between 2 and 200000.')
            sieve = bytearray(b'\x01') * (n + 1); sieve[:2] = b'\x00\x00'
            for p in range(2, int(math.isqrt(n))+1):
                if sieve[p]: sieve[p*p:n+1:p] = b'\x00' * (((n-p*p)//p)+1)
            return sum(sieve)
        raise ValueError('Unknown task type')

    def _execute(self, tid, wid):
        started = time.time()
        try:
            with self.lock: task = dict(self.tasks[tid])
            result = self._run_task(task['task_type'], task['input_value'])
            status, error = 'Completed', None
            result_text = json.dumps(result, ensure_ascii=False) if isinstance(result, (dict, list)) else str(result)
        except Exception as exc:
            status, error, result_text = 'Failed', str(exc), None
        ended = time.time()
        with self.lock:
            worker = self.workers.get(wid)
            if worker:
                worker['active'] = max(0, worker['active']-1)
                worker['last_seen'] = time.time()
                worker['completed' if status == 'Completed' else 'failed'] += 1
            task = self.tasks[tid]
            task.update(status=status, result=result_text, error=error, finished_at=ended, duration=round(ended-started, 4))
            self._persist(task)
            # Drain the queue as capacity becomes free. Selection is recalculated for every task.
            queued = sorted((x for x in self.tasks.values() if x['status'] == 'Queued'), key=lambda x: x['created_at'])
            for pending in queued:
                candidates = [w for w in self.workers.values() if w['status'] == 'Online' and w['active'] < w['capacity']]
                if not candidates: break
                self._dispatch(pending['id'])

    def retry(self, tid):
        with self.lock:
            task = self.tasks.get(tid)
            if not task or task['status'] not in ('Failed', 'Interrupted'): return False
            task.update(status='Queued', error=None, result=None, worker_id=None, created_at=time.time(),
                        started_at=None, finished_at=None, duration=None, attempts=0)
            self._persist(task)
        self._dispatch(tid)
        return True

    def toggle_worker(self, wid):
        with self.lock:
            if wid in self.workers:
                self.workers[wid]['status'] = 'Offline' if self.workers[wid]['status'] == 'Online' else 'Online'
                self.workers[wid]['last_seen'] = time.time()
                if self.workers[wid]['status'] == 'Online':
                    queued = sorted((x for x in self.tasks.values() if x['status'] == 'Queued'), key=lambda x: x['created_at'])
                    for pending in queued:
                        self._dispatch(pending['id'])

    def snapshot(self):
        with self.lock:
            tasks = [dict(t) for t in self.tasks.values()]
            workers = [dict(w) for w in self.workers.values()]
        tasks.sort(key=lambda x: x['created_at'], reverse=True)
        workers.sort(key=lambda x: x['worker_id'])
        return tasks, workers

@st.cache_resource
def get_scheduler():
    return Scheduler(worker_count=3, capacity=2)

st.set_page_config(page_title="Distributed Task Scheduler", page_icon="⚙️", layout="wide")
st.markdown("""
<style>
.block-container {padding-top: 1.8rem; max-width: 1400px;}
.hero {padding: 1.3rem 1.5rem; border: 1px solid rgba(128,128,128,.25); border-radius: 14px; margin-bottom: 1rem;}
.small-muted {color: #7b8490; font-size: .9rem;}
</style>
""", unsafe_allow_html=True)

scheduler = get_scheduler()
with st.sidebar:
    st.title("⚙️ Control panel")
    st.caption("Configure the logical worker pool")
    worker_count = st.slider("Number of workers", 1, 6, scheduler.worker_count)
    capacity = st.slider("Concurrent tasks per worker", 1, 4, scheduler.capacity)
    if st.button("Apply worker configuration", use_container_width=True):
        scheduler.configure(worker_count, capacity)
        st.success(f"Configured {worker_count} workers × {capacity} task slots.")
    st.divider()
    st.markdown("**Task types**")
    st.caption("Uppercase · Sum · Fibonacci · Sleep · Word count · Prime count")
    st.divider()
    auto_refresh = st.toggle("Auto-refresh dashboard", value=True)
    refresh_seconds = st.selectbox("Refresh interval", [2, 3, 5, 10], index=1)

st.markdown('<div class="hero"><h1>Distributed Task Scheduler</h1><p>Submit jobs, observe dynamic load balancing, and monitor worker execution in one cloud-hosted dashboard.</p><span class="small-muted">Scheduler · Pending queue · Least-loaded worker selection · Concurrent execution · SQLite task history</span></div>', unsafe_allow_html=True)

# Submission panel
with st.container(border=True):
    st.subheader("Submit a task")
    left, right = st.columns([1, 2])
    with left:
        task_type = st.selectbox("Task type", TASK_TYPES, format_func=lambda x: {
            'uppercase':'Uppercase text', 'sum':'Sum numbers', 'fibonacci':'Fibonacci',
            'sleep':'Simulated workload (sleep)', 'word_count':'Word / character count',
            'prime_count':'Count primes'}[x])
        batch_count = st.number_input("Number of identical tasks", min_value=1, max_value=30, value=1, step=1)
    with right:
        defaults = {'uppercase':'distributed systems', 'sum':'10,20,30', 'fibonacci':'30', 'sleep':'3', 'word_count':'Distributed systems share work across multiple workers', 'prime_count':'10000'}
        input_value = st.text_input("Task input", value=defaults[task_type], help="For Sum, separate numbers with commas. For Sleep, use 0–30 seconds.", key=f'input_{task_type}')
        st.caption({'uppercase':'Converts text to uppercase.', 'sum':'Enter comma-separated numbers, e.g. 10,20,30.', 'fibonacci':'Computes Fibonacci(n), where n is 0–5000.', 'sleep':'Useful for demonstrating concurrency and load balancing.', 'word_count':'Counts whitespace-separated words and characters.', 'prime_count':'Counts primes up to the chosen limit (2–200,000).'}[task_type])
    if st.button("▶ Submit task" if batch_count == 1 else f"▶ Submit {batch_count} tasks", type="primary", use_container_width=True):
        ids = scheduler.submit_batch(task_type, input_value, int(batch_count))
        st.success(f"Submitted {len(ids)} task(s): " + ", ".join(ids[:8]) + ("…" if len(ids)>8 else ""))

# Snapshot and metrics
tasks, workers = scheduler.snapshot()
counts = {status: sum(t['status'] == status for t in tasks) for status in ['Queued','Running','Completed','Failed','Interrupted']}
metrics = st.columns(5)
for col, label, value in zip(metrics, ['Queued','Running','Completed','Failed','Interrupted'], [counts[x] for x in ['Queued','Running','Completed','Failed','Interrupted']]):
    col.metric(label, value)

st.subheader("Worker cluster")
worker_cols = st.columns(min(3, max(1, len(workers))))
for idx, worker in enumerate(workers):
    with worker_cols[idx % len(worker_cols)]:
        online = worker['status'] == 'Online'
        with st.container(border=True):
            st.markdown(f"### {'🟢' if online else '⚪'} {worker['worker_id']}")
            st.caption(worker['status'])
            st.progress(min(1.0, worker['active']/max(1,worker['capacity'])), text=f"Load: {worker['active']} / {worker['capacity']} slots")
            a, b = st.columns(2)
            a.metric("Completed", worker['completed'])
            b.metric("Failed", worker['failed'])
            if st.button("Take offline" if online else "Bring online", key=f"toggle_{worker['worker_id']}", use_container_width=True):
                scheduler.toggle_worker(worker['worker_id'])
                st.rerun()

st.subheader("Task history")
if tasks:
    df = pd.DataFrame(tasks)
    view = df[['id','task_type','input_value','status','worker_id','attempts','duration','result','error','created_at']].copy()
    view['created_at'] = pd.to_datetime(view['created_at'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    view['duration'] = view['duration'].map(lambda x: f"{x:.3f}s" if pd.notna(x) else '—')
    view = view.rename(columns={'id':'Task ID','task_type':'Type','input_value':'Input','status':'Status','worker_id':'Worker','attempts':'Attempts','duration':'Duration','result':'Result','error':'Error','created_at':'Submitted'})
    st.dataframe(view, use_container_width=True, hide_index=True, column_config={
        'Result': st.column_config.TextColumn(width='large'), 'Error': st.column_config.TextColumn(width='medium'),
        'Input': st.column_config.TextColumn(width='medium')})
    retryable = [t for t in tasks if t['status'] in ('Failed','Interrupted')]
    if retryable:
        chosen = st.selectbox("Retry a failed/interrupted task", options=[t['id'] for t in retryable], format_func=lambda tid: f"{tid} · {next(t['task_type'] for t in retryable if t['id']==tid)}")
        if st.button("↻ Retry selected task"):
            if scheduler.retry(chosen): st.success(f"Task {chosen} queued again."); st.rerun()
    csv = pd.DataFrame(tasks).to_csv(index=False).encode('utf-8')
    st.download_button("Download task history (CSV)", data=csv, file_name='task_history.csv', mime='text/csv')
else:
    st.info("No tasks yet. Submit a task above to see scheduling and results here.")

with st.expander("How this demo works / architecture notes"):
    st.markdown("""
    1. **Client:** this Streamlit page submits task requests.
    2. **Scheduler:** records jobs, maintains the queue, and assigns work to the least-loaded worker with free capacity.
    3. **Workers:** each worker has its own thread pool and executes tasks concurrently.
    4. **Results:** task status, worker assignment, output, duration, and errors are recorded in SQLite.
    5. **Recovery demonstration:** switch a worker offline before submitting tasks to observe the scheduler use the remaining online workers. Failed tasks can be retried from the history section.

    **Deployment note:** this edition is designed for one-click Streamlit cloud hosting, so workers are independent logical worker pools inside the same hosted app process—not separate network services. It demonstrates queueing, concurrent work, load-aware scheduling and monitoring, but it does not claim to implement gRPC or survive a full app-instance failure. Streamlit Community Cloud's local disk may reset on redeploy/restart. For a strict multi-machine gRPC implementation, deploy the scheduler and workers as separate services on a container platform and use external persistent storage.
    """)

st.caption("Distributed Task Scheduler · Cloud-ready demo · Do not use for production workloads")
if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
