import os, json, time, uuid, random, threading, queue, signal, sys, csv, io, hmac, hashlib, argparse
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from prometheus_client import start_http_server, Counter, Histogram, Gauge

def env(k, d=None):
    v = os.getenv(k)
    if v is None or v == "": return d
    return v

# --- Config ---
APP   = env("APP", "demo")
VER   = env("VER", "v1")
PORT  = int(env("PORT", "8080"))
MPORT = int(env("METRICS_PORT", "9000"))
DATA_PATH = env("DATA_PATH", "/tmp/demo-data.json")
TOKEN = env("API_TOKEN", "devtoken")
RATE  = float(env("RATE_LIMIT_RPS", "50"))
HTML_EN = env("HTML_ENABLE", "1") == "1"
LOG_PATH = env("LOG_PATH", "")
CFG_MUT = {"RATE_LIMIT_RPS", "API_TOKEN", "HTML_ENABLE"}

# --- Metrics ---
REQ = Counter("demo_hits_total", "Total requests received", ["path","code","method"])
LAT = Histogram("demo_request_seconds", "Request latency", ["path","code","method"],
                buckets=(0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2,5,10))
INP = Gauge("demo_inprogress", "In-progress requests", ["path"])

KVN = Gauge("demo_kv_items", "Total items in the key-value store")
ORDN = Gauge("demo_orders_total", "Total orders stored")
USR = Gauge("demo_users_total", "Total registered users")

TICK = Counter("demo_bg_ticks_total", "Background worker ticks")
CPUH = Histogram("demo_compute_seconds", "CPU work time", buckets=(0.01,0.02,0.05,0.1,0.2,0.5,1,2,5))

POLL_CREATED = Counter("demo_polls_created_total", "Polls created")
POLL_VOTES   = Counter("demo_poll_votes_total", "Poll votes", ["poll_id","option"])
POLL_ACTIVE  = Gauge("demo_polls_total", "Active polls")
AB_FLAGS     = Gauge("demo_ab_flags_total", "Total A/B test flags")
AB_ASSIGN    = Counter("demo_ab_assign_total", "A/B test assignments", ["flag","variant"])
AB_EVENT     = Counter("demo_ab_events_total", "A/B test events", ["flag","variant","type"])

# --- Logging ---
def log_line(d):
    d["ts"] = d.get("ts") or int(time.time())
    s = json.dumps(d, separators=(",", ":"))
    sys.stdout.write(s + "\n")
    try:
        if LOG_PATH:
            with open(LOG_PATH, "a") as f: f.write(s + "\n")
    except Exception:
        pass

# --- Utils ---
def tok_ok(h):
    if not h: return False
    t = h[7:] if h.startswith("Bearer ") else h
    return hmac.compare_digest(t.encode(), TOKEN.encode())

def p_hash(p, s=None):
    s = s or os.urandom(8).hex()
    d = hashlib.sha256((s + ":" + p).encode()).hexdigest()
    return s + "$" + d

def p_check(p, hp):
    try: s, _ = hp.split("$", 1)
    except Exception: return False
    return p_hash(p, s) == hp

class TTL:
    def __init__(self, ttl=60):
        self.ttl=float(ttl); self.d={}; self.m={}; self.lock=threading.Lock()
    def set(self,k,v):
        with self.lock: self.d[k]=v; self.m[k]=time.time()+self.ttl
    def get(self,k):
        with self.lock:
            if k in self.m and self.m[k]>=time.time(): return self.d.get(k)
            if k in self.d: del self.d[k]; del self.m[k]
            return None
    def keys(self):
        with self.lock: return [k for k in self.d.keys() if self.m.get(k,0)>=time.time()]
    def clear(self):
        with self.lock: self.d.clear(); self.m.clear()

def v_not_empty(s): return isinstance(s,str) and len(s.strip())>0
def v_list(x): return isinstance(x,list) and len(x)>0
def v_email(s): return isinstance(s,str) and "@" in s and "." in s and " " not in s
def v_float(x):
    try: float(x); return True
    except Exception: return False

def exp_jsonl(arr):
    b=io.StringIO()
    for x in arr: b.write(json.dumps(x, separators=(",", ":"))+"\n")
    return b.getvalue()

def exp_csv(arr, fields):
    b=io.StringIO(); w=csv.DictWriter(b, fieldnames=fields); w.writeheader()
    for x in arr: w.writerow({k:x.get(k,"") for k in fields})
    return b.getvalue()

def imp_jsonl(s):
    out=[]
    for line in s.splitlines():
        line=line.strip()
        if not line: continue
        out.append(json.loads(line))
    return out

# --- Store ---
class Store:
    def __init__(self, path):
        self.path=path; self.lock=threading.Lock()
        self.kv={}; self.orders={}; self.users={}; self.polls={}
        self.votes_ip={}; self.flags={}; self.assign={}; self.events=[]
        self.rollup={"polls":{},"ab":{}}
    def load(self):
        try:
            with open(self.path,"r") as f: d=json.load(f)
            with self.lock:
                self.kv=d.get("kv",{}); self.orders=d.get("orders",{}); self.users=d.get("users",{})
                self.polls=d.get("polls",{}); self.votes_ip=d.get("votes_ip",{})
                self.flags=d.get("flags",{}); self.assign=d.get("assign",{})
                self.events=d.get("events",[]); self.rollup=d.get("rollup",{"polls":{},"ab":{}})
                KVN.set(len(self.kv)); ORDN.set(len(self.orders)); USR.set(len(self.users))
                POLL_ACTIVE.set(len(self.polls)); AB_FLAGS.set(len(self.flags))
        except Exception:
            pass
    def save(self):
        with self.lock:
            d={"kv":self.kv,"orders":self.orders,"users":self.users,"polls":self.polls,
               "votes_ip":self.votes_ip,"flags":self.flags,"assign":self.assign,
               "events":self.events,"rollup":self.rollup,"ts":int(time.time())}
        try:
            with open(self.path,"w") as f: json.dump(d,f,separators=(",",":"))
            return True
        except Exception:
            return False
    # KV
    def kv_put(self,k,v):
        with self.lock: self.kv[k]=v; KVN.set(len(self.kv))
    def kv_all(self):
        with self.lock: return dict(self.kv)
    def kv_del(self,k):
        with self.lock:
            ok=k in self.kv
            if ok: del self.kv[k]; KVN.set(len(self.kv))
            return ok
    # Users
    def user_add(self,u,hp):
        with self.lock:
            if u in self.users: return False
            self.users[u]={"u":u,"hp":hp,"ts":int(time.time())}; USR.set(len(self.users)); return True
    def user_list(self):
        with self.lock: return list(self.users.keys())
    # Orders
    def order_add(self,user,items,total=None):
        oid=str(uuid.uuid4()); t=float(total) if total is not None else float(len(items))*10.0
        o={"id":oid,"user":user,"items":items,"total":t,"ts":int(time.time())}
        with self.lock: self.orders[oid]=o; ORDN.set(len(self.orders))
        return o
    def order_list(self):
        with self.lock: return list(self.orders.values())
    def order_search(self,user=None,min_total=None,since=None):
        with self.lock:
            out=[]
            for o in self.orders.values():
                if user and o.get("user")!=user: continue
                if min_total is not None and float(o.get("total",0))<float(min_total): continue
                if since is not None and int(o.get("ts",0))<int(since): continue
                out.append(o)
            return out
    # Polls
    def poll_create(self,q,opts,close_ts=None):
        pid=str(uuid.uuid4())
        p={"id":pid,"q":q,"opts":list(opts),"votes":{o:0 for o in opts},"ts":int(time.time())}
        if close_ts: p["close_ts"]=int(close_ts)
        with self.lock: self.polls[pid]=p; POLL_ACTIVE.set(len(self.polls))
        return p
    def poll_vote(self,pid,opt,ip=None):
        with self.lock:
            p=self.polls.get(pid)
            if not p: return False,"not_found"
            if "close_ts" in p and int(time.time())>=int(p["close_ts"]): return False,"closed"
            if opt not in p["opts"]: return False,"bad_opt"
            if ip:
                k=f"{pid}:{ip}"
                if time.time()-int(self.votes_ip.get(k,0))<5: return False,"throttle"
                self.votes_ip[k]=int(time.time())
            p["votes"][opt]=int(p["votes"].get(opt,0))+1
            POLL_VOTES.labels(poll_id=pid, option=opt).inc()
            return True,"ok"
    def poll_get(self,pid):
        with self.lock: return self.polls.get(pid)
    def poll_list(self):
        with self.lock: return list(self.polls.values())
    # A/B
    def flag_create(self,name,variants,ratio):
        fid=name.strip() or str(uuid.uuid4())
        r=[float(x) for x in ratio]
        if sum(r)<=0 or len(r)!=len(variants): r=[1.0]*len(variants)
        p={"id":fid,"variants":list(variants),"ratio":r,"ts":int(time.time())}
        with self.lock: self.flags[fid]=p; AB_FLAGS.set(len(self.flags))
        return p
    def flag_list(self):
        with self.lock: return list(self.flags.values())
    def _assign_det(self,user,fid):
        h=hashlib.sha256((user+"|"+fid).encode()).hexdigest()
        x=int(h[:8],16)/0xffffffff
        f=self.flags.get(fid); vs,rs=f["variants"],f["ratio"]; tot=sum(rs); acc=0.0
        for v,w in zip(vs,rs):
            acc+=w/tot
            if x<=acc: return v
        return vs[-1]
    def flag_assign(self,user,fid,override=None):
        with self.lock:
            if not self.flags.get(fid): return None
            if user not in self.assign: self.assign[user]={}
            v=override if override else self._assign_det(user,fid)
            self.assign[user][fid]=v; AB_ASSIGN.labels(flag=fid, variant=v).inc(); return v
    def flag_get_assign(self,user,fid):
        with self.lock: return (self.assign.get(user) or {}).get(fid)
    def event_add(self,user,fid,var,typ):
        e={"flag":fid,"variant":var,"user":user,"type":typ,"ts":int(time.time())}
        with self.lock: self.events.append(e); AB_EVENT.labels(flag=fid,variant=var,type=typ).inc()
        return e
    def rollup_compute(self):
        now=int(time.time()); rp={}
        with self.lock:
            for pid,p in self.polls.items():
                tot=sum(int(v) for v in p["votes"].values())
                rp[pid]={"id":pid,"q":p["q"],"total":tot,"by":dict(p["votes"])}
        ra={}
        with self.lock:
            by={}
            for e in self.events:
                k=(e["flag"],e["variant"],e["type"]); by[k]=by.get(k,0)+1
            for (flag,var,typ),n in by.items():
                ra.setdefault(flag,{}).setdefault(var,{})[typ]=n
        with self.lock: self.rollup={"polls":rp,"ab":ra,"ts":now}

# --- Background worker ---
class Jobs:
    def __init__(self):
        self.q=queue.Queue(maxsize=1000); self.stop=threading.Event(); self.t=None; self.snap=None
    def start(self,snap_fn=None):
        self.snap=snap_fn; self.t=threading.Thread(target=self.run, daemon=True); self.t.start()
    def put(self,x):
        try: self.q.put_nowait(x); return True
        except queue.Full: return False
    def run(self):
        last=time.time()
        while not self.stop.is_set():
            try:
                it=self.q.get(timeout=0.5); self.work(it); self.q.task_done()
            except queue.Empty:
                pass
            finally:
                TICK.inc()
            if self.snap and time.time()-last>10:
                try: self.snap()
                except Exception: pass
                last=time.time()
    def work(self,it):
        n=random.randint(30,36); t0=time.perf_counter()
        a,b=0,1
        for _ in range(n):
            a,b=b,a+b
        CPUH.observe(time.perf_counter()-t0)
    def stop_now(self): self.stop.set()

# --- HTTP helpers ---
def html_page(title,body):
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>{title}</title>
<style>body{{font-family:sans-serif;padding:20px}}.box{{padding:12px;border:1px solid #ddd;border-radius:8px}}table{{border-collapse:collapse}}td,th{{border:1px solid #ddd;padding:6px}}</style>
</head><body><h2>{title}</h2><div class="box">{body}</div></body></html>"""

def rep_summary(orders):
    import collections
    total=0.0; by_user=collections.Counter()
    for o in orders:
        total+=float(o.get("total",0)); by_user[o.get("user","")]+=1
    return {"count":len(orders),"sum":total,"top":by_user.most_common(5),"ts":int(time.time())}

def rep_recent(orders,n=10): return sorted(orders, key=lambda x:x.get("ts",0), reverse=True)[:n]

class Limiter:
    def __init__(self,rps):
        self.rps=max(1.0,float(rps)); self.n=0; self.t=time.time(); self.lock=threading.Lock()
    def ok(self):
        now=time.time()
        with self.lock:
            if now-self.t>=1.0: self.t=now; self.n=0
            if self.n<self.rps: self.n+=1; return True
            return False

def jb(o): return json.dumps(o, separators=(",", ":")).encode()

# --- HTTP handler ---
class Handler(BaseHTTPRequestHandler):
    s=None; jobs=None
    ready={"r":True}
    lim=Limiter(RATE)
    cache=TTL(30)

    def log_message(self,f,*a): pass
    def do_GET(self): self._d("GET")
    def do_POST(self): self._d("POST")
    def do_DELETE(self): self._d("DELETE")

    def _j(self,c,o,h=None):
        b=jb(o); self.send_response(c); self.send_header("Content-Type","application/json")
        self.send_header("Content-Length",str(len(b)))
        if h: [self.send_header(k,v) for k,v in h.items()]
        self.end_headers(); self.wfile.write(b); self._resp_code=c
    def _t(self,c,s,ct="text/plain"):
        b=s.encode() if not isinstance(s,bytes) else s
        self.send_response(c); self.send_header("Content-Type",ct)
        self.send_header("Content-Length",str(len(b))); self.end_headers()
        self.wfile.write(b); self._resp_code=c

    def _body(self):
        l=int(self.headers.get("Content-Length","0") or "0")
        if l<=0: return {}
        try: return json.loads(self.rfile.read(l).decode())
        except Exception: return {}
    def _ip(self):
        try: return str(self.client_address[0])
        except Exception: return "0.0.0.0"

    def _d(self,m):
        t0=time.perf_counter(); u=urlparse(self.path); p=u.path
        rid=self.headers.get("X-Request-Id") or str(int(time.time()*1000))
        INP.labels(path=p).inc(); sc=200
        try:
            # Basics
            if p == "/": self._t(200, f"hello from {APP} {VER}\n")
            elif p == "/home" and HTML_EN:
                body="<ul><li>/version</li><li>/healthz</li><li>/store</li><li>/users</li><li>/orders</li>...</ul>"
                self._t(200, html_page("demo", body), "text/html")
            elif p == "/version": self._j(200, {"app":APP,"ver":VER}, {"X-Request-Id": rid})
            elif p == "/healthz": self._j(200, {"ok":True,"ts":int(time.time())})
            elif p == "/ready": self._j(200 if self.ready["r"] else 503, {"ready": bool(self.ready["r"])})
            elif p == "/ready/set" and m=="POST":
                d=self._body(); self.ready["r"]=bool(d.get("ready",True)); self._j(200, {"ready": self.ready["r"]})
            elif p == "/limit":
                ok=self.lim.ok(); self._j(200 if ok else 429, {"ok": ok})

            # Demos
            elif p == "/slow":
                q=parse_qs(u.query); time.sleep(max(0, int(q.get("ms",[250])[0]))/1000.0); self._j(200,{})
            elif p == "/error":
                q=parse_qs(u.query); self._t(int(q.get("code",[500])[0]), "error\n")
            elif p == "/compute":
                q=parse_qs(u.query); n=max(1, min(int(q.get("n",[30])[0]), 38))
                a,b=0,1
                for _ in range(n):
                    a,b=b,a+b
                self._j(200,{})
            elif p == "/queue" and m=="POST":
                ok=self.jobs.put({"t":time.time()}); self._j(202 if ok else 429, {"queued": ok})

            # KV
            elif p == "/store" and m=="GET": self._j(200, {"kv": self.s.kv_all()})
            elif p == "/store" and m=="POST":
                d=self._body(); k=str(d.get("key","")); v=d.get("value"); self.s.kv_put(k,v); self._j(200, {"ok":True})
            elif p == "/store" and m=="DELETE":
                q=parse_qs(u.query); ok=self.s.kv_del(q.get("key",[""])[0]); self._j(200, {"deleted": ok})

            # Users
            elif p == "/users" and m=="POST":
                d=self._body(); uo=d.get("u",""); pw=d.get("p",""); ok=self.s.user_add(uo, p_hash(pw)); self._j(200 if ok else 400, {"ok": ok})
            elif p == "/users" and m=="GET": self._j(200, {"users": self.s.user_list()})

            # Orders
            elif p == "/orders" and m=="GET":
                q=parse_qs(u.query); arr=self.s.order_search(user=q.get("user",[None])[0]); self._j(200, {"orders": arr})
            elif p == "/orders" and m=="POST":
                if not tok_ok(self.headers.get("Authorization","")): self._j(401, {"error":"auth"})
                else:
                    d=self._body(); o=self.s.order_add(d.get("user",""), d.get("items",[])); self._j(201, o)

            # Export/Import
            elif p == "/export/jsonl": self._t(200, exp_jsonl(self.s.order_list()))
            elif p == "/export/csv": self._t(200, exp_csv(self.s.order_list(), ["id","user","total","ts"]))
            elif p == "/import" and m=="POST":
                d=self._body(); arr=imp_jsonl(d.get("data","")); self._j(200, {"count": len(arr)})

            # Admin & reports
            elif p == "/dump" and m=="POST":
                ok=self.s.save(); self._j(200 if ok else 500, {"saved": ok})
            elif p == "/reports":
                r=self.cache.get("rep")
                if r is None:
                    o=self.s.order_list()
                    r={"summary": rep_summary(o), "recent": rep_recent(o,10),
                       "polls": self.s.rollup.get("polls",{}), "ab": self.s.rollup.get("ab",{})}
                    self.cache.set("rep", r)
                self._j(200, r)

            # Runtime config (fixed: no walrus on attribute)
            elif p == "/config" and m=="GET":
                self._j(200, {"RATE_LIMIT_RPS": self.lim.rps})
            elif p == "/config" and m=="POST":
                d=self._body(); ch={}
                if "RATE_LIMIT_RPS" in d:
                    try:
                        self.lim = Limiter(float(d["RATE_LIMIT_RPS"]))
                        ch["RATE_LIMIT_RPS"] = d["RATE_LIMIT_RPS"]
                    except Exception:
                        pass
                self._j(200, {"changed": ch})

            # Polls
            elif p == "/polls" and m=="POST":
                if not tok_ok(self.headers.get("Authorization","")): self._j(401, {"error":"auth"})
                else:
                    d=self._body(); pr=self.s.poll_create(d.get("q",""), d.get("opts",[]), d.get("close_ts"))
                    POLL_CREATED.inc(); self._j(201, pr)
            elif p == "/polls" and m=="GET": self._j(200, {"polls": self.s.poll_list()})
            elif p == "/polls/result" and m=="GET":
                q=parse_qs(u.query); pr=self.s.poll_get(q.get("id",[None])[0]); self._j(200 if pr else 404, pr or {})
            elif p == "/polls/vote" and m=="POST":
                d=self._body(); ok,msg=self.s.poll_vote(d.get("id",""), d.get("opt",""), self._ip()); self._j(200 if ok else 400, {"ok":ok,"msg":msg})

            # A/B
            elif p == "/ab/flags" and m=="POST":
                if not tok_ok(self.headers.get("Authorization","")): self._j(401, {"error":"auth"})
                else:
                    d=self._body(); f=self.s.flag_create(d.get("name",""), d.get("variants",[]), d.get("ratio",[])); self._j(201, f)
            elif p == "/ab/flags" and m=="GET": self._j(200, {"flags": self.s.flag_list()})
            elif p == "/ab/assign" and m=="POST":
                d=self._body(); v=self.s.flag_assign(d.get("user",""), d.get("flag",""), d.get("variant")); self._j(200, {"variant": v})
            elif p == "/ab/events" and m=="POST":
                d=self._body(); e=self.s.event_add(d.get("user",""), d.get("flag",""), d.get("variant",""), d.get("type","")); self._j(201, e)
            elif p == "/ab/report" and m=="GET":
                self._j(200, self.s.rollup.get("ab", {}))

            else:
                sc=404; self._t(404, "not found\n"); return
        finally:
            INP.labels(path=p).dec()
            dt=max(0.0, time.perf_counter()-t0)
            rc=getattr(self, "_resp_code", sc)
            REQ.labels(path=p, code=str(rc), method=m).inc()
            LAT.labels(path=p, code=str(rc), method=m).observe(dt)
            log_line({"m":m,"p":p,"c":rc,"ms":int(dt*1000),"rid":rid})

# --- Server ---
def serve():
    st=Store(DATA_PATH); st.load()
    jb=Jobs(); jb.start(lambda:(st.rollup_compute(), st.save()))
    Handler.s=st; Handler.jobs=jb

    start_http_server(MPORT)
    srv=HTTPServer(("", PORT), Handler)

    def stop(sig, frm):
        try: jb.stop_now()
        except Exception: pass
        try: st.rollup_compute(); st.save()
        except Exception: pass
        try: srv.shutdown()
        except Exception: pass
    signal.signal(signal.SIGINT, stop); signal.signal(signal.SIGTERM, stop)

    srv.serve_forever()

# --- CLI helpers ---
def rstr(n=6):
    import string; return "".join(random.choice(string.ascii_lowercase) for _ in range(n))
def cli_seed(base):
    import requests; [requests.post(base+"/users", json={"u":rstr(),"p":"pass"}) for _ in range(3)]
def cli_load(base, secs):
    import requests; t0=time.time(); i=0
    while time.time()-t0 < secs:
        try:
            requests.get(base+"/", timeout=2)
            if i%3==0: requests.get(base+"/version", timeout=2)
            if i%5==0: requests.get(base+"/slow?ms=120", timeout=2)
        except Exception: pass
        i+=1; time.sleep(0.1)
    print(json.dumps({"hits": i}))
def cli_orders(base, token, n):
    import requests; h={"Authorization":"Bearer "+token}; ok=0
    for _ in range(n):
        try:
            r=requests.post(base+"/orders", json={"user":rstr(),"items":["x","y"],"total":20.0}, headers=h, timeout=3)
            if r.ok: ok+=1
        except Exception: pass
    print(json.dumps({"orders": ok}))
def cli_polls(base, token):
    import requests; h={"Authorization":"Bearer "+token}
    r=requests.post(base+"/polls", json={"q":"Best tool?","opts":["A","B","C"]}, headers=h)
    pid=r.json().get("id")
    if pid:
        [requests.post(base+"/polls/vote", json={"id":pid,"opt":o}) for o in ["A","B","A","C","A"]]
        rr=requests.get(base+"/polls/result?id="+pid)
        print(json.dumps({"poll": pid, "result": rr.json()}))
def cli_ab(base, token):
    import requests; h={"Authorization":"Bearer "+token}
    r=requests.post(base+"/ab/flags", json={"name":"cta","variants":["red","blue"]}, headers=h)
    fid=r.json().get("id","cta")
    for u in [rstr() for _ in range(10)]:
        a=requests.post(base+"/ab/assign", json={"user":u,"flag":fid}).json()
        requests.post(base+"/ab/events", json={"user":u,"flag":fid,"variant":a.get("variant"),"type":"view"})
        if random.random()<0.5: requests.post(base+"/ab/events", json={"user":u,"flag":fid,"variant":a.get("variant"),"type":"click"})
    print(json.dumps({"report": requests.get(base+"/ab/report").json()}))

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--serve", action="store_true", help="Run the web server (default)")
    ap.add_argument("--seed", action="store_true")
    ap.add_argument("--load", type=int, default=0)
    ap.add_argument("--orders", type=int, default=0)
    ap.add_argument("--polls", action="store_true")
    ap.add_argument("--ab", action="store_true")
    ap.add_argument("--base", default="http://localhost:8080")
    ap.add_argument("--token", default=TOKEN)
    args=ap.parse_args()

    if args.serve or not any([args.seed, args.load, args.orders, args.polls, args.ab]):
        serve()
    else:
        if args.seed: cli_seed(args.base)
        if args.load>0: cli_load(args.base, args.load)
        if args.orders>0: cli_orders(args.base, args.token, args.orders)
        if args.polls: cli_polls(args.base, args.token)
        if args.ab: cli_ab(args.base, args.token)
