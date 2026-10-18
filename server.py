"""DMS Beauty booking backend. Local SQLite or PostgreSQL via DATABASE_URL.
Run: DMS_ADMIN_PASSWORD='a-strong-password' python server.py
"""
from __future__ import annotations
import os, json, re, secrets, hmac, hashlib, time, sqlite3, threading
from pathlib import Path
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse
from pydantic import BaseModel
import uvicorn

ROOT=Path(__file__).resolve().parent
DB_URL=os.getenv('DATABASE_URL', '').strip()
if os.getenv('RENDER_SERVICE_ID') and not DB_URL:
    raise RuntimeError('Cloud hosting requires DATABASE_URL; SQLite is not persistent on Render free instances.')
LOCAL_DB=Path(os.getenv('DMS_DB_FILE',str(ROOT/'dms.sqlite3')))
ADMIN_PASSWORD=os.getenv('DMS_ADMIN_PASSWORD','')
SESSION_SECRET=os.getenv('DMS_SESSION_SECRET','') or secrets.token_hex(32)
TBILISI=ZoneInfo('Asia/Tbilisi')
_LOCK=threading.RLock()
app=FastAPI(title='DMS Beauty Booking', docs_url=None, redoc_url=None)

class StateDB:
    def __init__(self):
        if DB_URL:
            try: import psycopg
            except ImportError as exc: raise RuntimeError('Install psycopg[binary] for DATABASE_URL') from exc
            self.pg=psycopg
        else: self.pg=None
        with self.conn() as conn:
            if self.pg:
                conn.execute('CREATE TABLE IF NOT EXISTS dms_state (id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, body JSONB NOT NULL)')
            else:
                conn.execute('CREATE TABLE IF NOT EXISTS dms_state (id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, body TEXT NOT NULL)')
            q=conn.execute('SELECT revision FROM dms_state WHERE id=1').fetchone()
            if q is None:
                seed=json.loads((ROOT/'seed.json').read_text('utf-8'))
                val=json.dumps(seed,ensure_ascii=False)
                conn.execute('INSERT INTO dms_state (id, revision, body) VALUES (1,0,%s::jsonb)' if self.pg else 'INSERT INTO dms_state (id, revision, body) VALUES (1,0,?)', (val,))
            conn.commit()

    @contextmanager
    def conn(self):
        if self.pg: con=self.pg.connect(DB_URL)
        else:
            con=sqlite3.connect(str(LOCAL_DB), timeout=15,check_same_thread=False)
            con.execute('PRAGMA busy_timeout=15000')
            con.execute('PRAGMA journal_mode=WAL')
        try:
            yield con
        finally:
            con.close()

    def read(self):
        with self.conn() as con:
            rev, raw=con.execute('SELECT revision,body FROM dms_state WHERE id=1').fetchone()
            return int(rev), raw if isinstance(raw,dict) else json.loads(raw)

    def write(self, callback):
        with _LOCK:
            with self.conn() as con:
                try:
                    if not self.pg: con.execute('BEGIN IMMEDIATE')
                    sql='SELECT revision, body FROM dms_state WHERE id=1 FOR UPDATE' if self.pg else 'SELECT revision, body FROM dms_state WHERE id=1'
                    rev, raw=con.execute(sql).fetchone()
                    state=raw if isinstance(raw,dict) else json.loads(raw)
                    result=callback(state,int(rev))
                    state['version']=4
                    val=json.dumps(state,ensure_ascii=False)
                    con.execute('UPDATE dms_state SET revision=%s,body=%s::jsonb WHERE id=1' if self.pg else 'UPDATE dms_state SET revision=?,body=? WHERE id=1', (rev+1,val))
                    con.commit()
                    return result
                except Exception:
                    con.rollback();raise

store=StateDB()

class BookingInput(BaseModel):
    branchId:str
    serviceId:str
    staffId:str='any'
    date:str
    time:str
    name:str
    phone:str
    note:str=''
    website:str=''  # honeypot
class LookupInput(BaseModel):
    id:str
    phone:str
class LoginInput(BaseModel):
    password:str
class SnapshotInput(BaseModel):
    revision:int
    state:dict


def now(): return datetime.now(TBILISI)
def today(): return now().date()
def clean_phone(s): return re.sub(r'\D','',s or '')[-9:]
def fail(code,msg): raise HTTPException(code,msg)
def valid_slot_date(t):
    try:d=date.fromisoformat(t)
    except Exception:fail(422,'არასწორი თარიღი')
    if d<today() or d>today()+timedelta(days=45):fail(422,'აირჩიეთ თარიღი მომდევნო 45 დღიდან')
    return d

def minute(t):
    if not re.fullmatch(r'(?:1\d|20):[0-5]\d',t or ''):fail(422,'არასწორი საათი')
    h,m=map(int,t.split(':'))
    return h*60+m

def by_id(collection, id): return next((x for x in collection if x['id']==id),None)
def available(state,item):
    d=valid_slot_date(item['date']);start=minute(item['time'])
    service=by_id(state['services'],item['serviceId']);branch=by_id(state['branches'],item['branchId'])
    if not service or not service.get('active') or not branch:fail(422,'მომსახურება ან ფილიალი არ მოიძებნა')
    dur=int(service['duration'])
    if start<600 or start%30 or start+dur>1200:fail(422,'საათი სამუშაო გრაფიკს არ შეესაბამება')
    t=now()
    if d==t.date() and start<=t.hour*60+t.minute+15:fail(409,'ამ დროის დაჯავშნა უკვე შეუძლებელია')
    people=[]
    for worker in state['staff']:
        if not worker.get('active') or worker['branch']!=item['branchId'] or service['category'] not in worker['skills']:continue
        if item.get('staffId','any') not in ('any',worker['id']):continue
        taken=False
        for booked in state['bookings']:
            if booked['staffId']!=worker['id'] or booked['date']!=item['date'] or booked['status'] not in ('pending','confirmed'):continue
            old=by_id(state['services'],booked['serviceId'])
            bs=minute(booked['time']);be=bs+int(old['duration'] if old else 60)
            if start<be and start+dur>bs:
                taken=True;break
        if not taken:people.append(worker)
    if not people:fail(409,'არჩეული დრო უკვე დაკავებულია — გთხოვთ აირჩიოთ სხვა')
    return people[0]['id']


def public_state(state,rev):
    return {'version':4,'revision':rev,'branches':state['branches'],'services':state['services'],
            'staff':state['staff'],'settings':{'hoursStart':state['settings'].get('hoursStart','10:00'),'hoursEnd':state['settings'].get('hoursEnd','20:00')},
            'bookings':[{'id':'busy-'+hashlib.sha256(b['id'].encode()).hexdigest()[:16],'branchId':b['branchId'],'serviceId':b['serviceId'],'staffId':b['staffId'],'date':b['date'],'time':b['time'],'status':b['status']}
                        for b in state['bookings'] if b['status'] in ('pending','confirmed') and b['date']>=str(today())]}

def jwt_cookie(token):
    try:
        expiry,sig=token.split('.',1)
        if int(expiry)<time.time():return False
        digest=hmac.new(SESSION_SECRET.encode(),expiry.encode(),hashlib.sha256).hexdigest()
        return hmac.compare_digest(sig,digest)
    except Exception:return False

def admin_required(req):
    if not ADMIN_PASSWORD:fail(503,'სერვერზე ადმინისტრატორის პაროლი არაა დაყენებული')
    if not jwt_cookie(req.cookies.get('dms_admin','')):fail(401,'ადმინისტრატორის ავტორიზაცია აუცილებელია')

@app.get('/')
def root():return RedirectResponse('/client')
@app.get('/client')
def client():return FileResponse(ROOT/'client.html',media_type='text/html')
@app.get('/admin')
def admin():return FileResponse(ROOT/'admin.html',media_type='text/html')
@app.get('/assets/{filename}')
def asset(filename:str):
    if filename not in ('logo.jpg','cover.jpg'):fail(404,'Not found')
    return FileResponse(ROOT/'assets'/filename)
@app.get('/api/health')
def health():return {'ok':True,'storage':'postgres' if store.pg else 'sqlite','authConfigured':bool(ADMIN_PASSWORD)}
@app.get('/api/public')
def api_public():
    rev,state=store.read()
    return public_state(state,rev)
@app.post('/api/bookings')
def create_booking(item:BookingInput):
    if item.website:fail(422,'ფორმა ვერ დამუშავდა')
    name=item.name.strip();phone=clean_phone(item.phone)
    if len(name)<2 or len(name)>80 or len(phone)!=9:fail(422,'შეამოწმე სახელი და 9-ციფრიანი ტელეფონი')
    if len(item.note)>300:fail(422,'კომენტარი ძალიან გრძელია')
    def action(state,rev):
        worker=available(state,item.model_dump())
        booking={'id':'DMS-'+secrets.token_hex(5).upper(),'branchId':item.branchId,'serviceId':item.serviceId,'staffId':worker,
             'date':item.date,'time':item.time,'name':name,'phone':phone,'note':item.note.strip(),
             'createdAt':now().isoformat(),'status':'pending','demo':False}
        state['bookings'].append(booking)
        return booking
    return store.write(action)
@app.post('/api/lookup')
def lookup(item:LookupInput):
    _,state=store.read()
    b=by_id(state['bookings'],item.id.strip())
    if not b or not hmac.compare_digest(clean_phone(b.get('phone','')),clean_phone(item.phone)) or len(clean_phone(item.phone))!=9:fail(404,'ამ ნომრითა და ჯავშნის კოდით ჩანაწერი ვერ მოიძებნა')
    return b
@app.post('/api/cancel')
def cancel(item:LookupInput):
    def action(state,rev):
        b=by_id(state['bookings'],item.id.strip())
        if not b or not hmac.compare_digest(clean_phone(b.get('phone','')),clean_phone(item.phone)) or len(clean_phone(item.phone))!=9:fail(404,'ჯავშანი ვერ მოიძებნა')
        if b['status'] not in ('pending','confirmed'):fail(409,'ამ ჯავშნის გაუქმება შეუძლებელია')
        b['status']='cancelled';return {'ok':True,'booking':b}
    return store.write(action)
@app.post('/api/login')
def login(item:LoginInput,request:Request,response:Response):
    if not ADMIN_PASSWORD:fail(503,'ჯერ DMS_ADMIN_PASSWORD უნდა დააყენო სერვერზე')
    if not hmac.compare_digest(item.password,ADMIN_PASSWORD):fail(401,'არასწორი პაროლი')
    expiry=str(int(time.time()+7*24*3600))
    signature=hmac.new(SESSION_SECRET.encode(),expiry.encode(),hashlib.sha256).hexdigest()
    response.set_cookie('dms_admin',expiry+'.'+signature,max_age=7*24*3600,httponly=True,secure=(request.url.scheme=='https' or request.headers.get('x-forwarded-proto')=='https'),samesite='lax',path='/')
    return {'ok':True}
@app.post('/api/logout')
def logout(response:Response):
    response.delete_cookie('dms_admin',path='/');return {'ok':True}
@app.get('/api/admin/state')
def admin_state(request:Request):
    admin_required(request)
    rev,state=store.read();return {'revision':rev,'state':state}
@app.post('/api/admin/state')
def update_admin(item:SnapshotInput,request:Request):
    admin_required(request)
    def action(state,rev):
        if rev!=item.revision:fail(409,'შეიცვალა მონაცემები. განაახლე გვერდი და სცადე თავიდან')
        nxt=item.state
        for key in ('services','staff','bookings','branches'):
            if not isinstance(nxt.get(key),list):fail(422,'დაზიანებული მონაცემები')
        if len(nxt['bookings'])>100000 or len(nxt['services'])>500:fail(422,'ძალიან ბევრი ჩანაწერი')
        # Protect immutable IDs & booking PII from accidental deletion by stale admin snapshots.
        old={b['id']:b for b in state['bookings']}; new={b.get('id'):b for b in nxt['bookings']}
        for id,oldb in old.items():
            if id not in new:fail(422,'ჯავშნების წაშლა არ არის დაშვებული')
            for k in ('id','phone','name','date','time','branchId','serviceId','staffId'):
                if new[id].get(k)!=oldb.get(k):fail(422,'არსებული ჯავშნის მონაცემების შეცვლა შეზღუდულია')
            if new[id].get('status') not in ('pending','confirmed','done','cancelled'):fail(422,'არასწორი სტატუსი')
        # Owner can add manual bookings with same collision validation; but the original
        # appointment insertion uses whichever staff the server selects.
        for id,newb in new.items():
            if id not in old:
                if len(clean_phone(newb.get('phone',''))) not in (0,9):fail(422,'არასწორი ტელეფონი')
                worker=available(state,newb)
                newb['staffId']=worker
                newb['createdAt']=now().isoformat()
                newb['demo']=False
                state['bookings'].append(newb)
        for id,newb in new.items():
            if id in old:old[id]['status']=newb['status']
        state['services']=nxt['services'];state['staff']=nxt['staff']
        return {'ok':True,'revision':rev+1}
    return store.write(action)

if __name__=='__main__':
    if not ADMIN_PASSWORD: print('WARNING: set DMS_ADMIN_PASSWORD to enable the admin panel.')
    uvicorn.run(app,host='0.0.0.0',port=int(os.getenv('PORT','8000')))