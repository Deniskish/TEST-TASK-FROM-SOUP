#!/usr/bin/env python3
"""Проверки RabbitMQ и наблюдаемости на одноразовом Compose-стенде.

Дублирует сообщения, проверяет DLQ, останавливает брокер и БД, убивает publisher
и воркер после COMMIT до ACK. Проверяет состояние БД, реальные метрики Prometheus,
дашборд Grafana через API, тестовый webhook и сквозную трассу в Tempo.
"""
import argparse,base64,json,time,urllib.request,urllib.error
from common import run
from integration import wait_for

def scenario(h,args):
 h.base.extend(['--profile','observability'])
 h.env['OTEL_EXPORTER_OTLP_ENDPOINT']='http://collector:4318'
 with h.override.open('a') as f:
  for service,port in [('prometheus',9090),('tempo',3200),('rabbitmq',15672)]:
   f.write(f"  {service}:\n    ports: ['127.0.0.1::{port}']\n")
  f.write("  grafana:\n    ports: !override ['127.0.0.1::3000']\n")
 h.start(4)
 urls={n:'http://'+h.compose('port',n,str(p)).splitlines()[0] for n,p in [('prometheus',9090),('tempo',3200),('rabbitmq',15672),('grafana',3000)]}
 def get(service,path,data=None):
  if service=='rabbitmq':
   current='http://'+h.compose('port','rabbitmq','15672').splitlines()[0]
   if current!=urls[service]:h.event('management_port_changed',previous=urls[service],current=current)
   urls[service]=current
  headers={}
  if service in ('grafana','rabbitmq'):
   creds='admin:local-grafana-change-me' if service=='grafana' else 'soup:local-development-only'
   headers['Authorization']='Basic '+base64.b64encode(creds.encode()).decode()
  if data is not None:headers['Content-Type']='application/json'
  req=urllib.request.Request(urls[service]+path,data=None if data is None else json.dumps(data).encode(),headers=headers)
  with urllib.request.urlopen(req,timeout=5) as r:return json.load(r)
 def query(q):
  from urllib.parse import quote
  return get('prometheus','/api/v1/query?query='+quote(q))['data']['result']
 def publish(body):
  return get('rabbitmq','/api/exchanges/%2F/amq.default/publish',{'properties':{'delivery_mode':2},'routing_key':'soup.jobs','payload':json.dumps(body),'payload_encoding':'string'})
 def ready():
  try:return get('rabbitmq','/api/overview')
  except (OSError,urllib.error.HTTPError):return False
 wait_for(ready,90)
 j=h.post('v2-duplicate',{'model':'v2','duration_ms':1000})
 assert publish({'job_id':j['id']})['routed']
 assert publish({'job_id':j['id']})['routed']
 assert publish({'bad':'message'})['routed']
 h.drain(90);h.verify()
 wait_for(lambda:get('rabbitmq','/api/queues/%2F/soup.dead')['messages']>0,30)
 h.event('duplicate_and_invalid_message_verified')
 time.sleep(1)
 h.post('after-idle',{'model':'idle','duration_ms':20})
 h.drain(5)
 h.event('publisher_after_idle_verified')
 # Broker outage must not discard accepted jobs; pending outbox is authoritative.
 import concurrent.futures
 with concurrent.futures.ThreadPoolExecutor(max_workers=16) as clients:
  inflight=[clients.submit(h.post,f'publish-flight-{i}',{'model':'flight','duration_ms':40}) for i in range(200)]
  h.compose('stop','rabbitmq')
  published_jobs=[f.result() for f in inflight]
 accepted={}
 for i in range(20):
  job=h.post(f'outage-{i}',{'model':'outage','duration_ms':50});accepted[f'outage-{i}']=job['id']
 assert int(h.sql('SELECT count(*) FROM outbox WHERE published_at IS NULL'))>=20
 wait_for(lambda:any(x['metric'].get('alertname')=='RabbitMQUnavailable' and x['metric'].get('alertstate')=='firing' for x in query('ALERTS')),90)
 wait_for(lambda:'RabbitMQUnavailable' in h.compose('logs','--no-color','alert-receiver'),30)
 h.event('rabbit_down_outbox_and_real_alert_verified')
 h.compose('start','rabbitmq');h.drain(120);rows=h.verify()
 assert len(rows)==222
 assert all(any(r['id']==j['id'] for r in rows) for j in published_jobs)
 # Restart broker with a live, leased job.
 j=h.post('restart-running',{'model':'restart','duration_ms':4000})
 wait_for(lambda:h.request('GET',f"/jobs/{j['id']}")[1].get('status')=='running')
 h.compose('restart','rabbitmq');h.drain(120);h.verify()
 # Worker loses PostgreSQL after delivery. API cannot claim acceptance during outage.
 j=h.post('postgres-loss',{'model':'pg','duration_ms':4000})
 wait_for(lambda:h.request('GET',f"/jobs/{j['id']}")[1].get('status')=='running')
 h.compose('stop','db')
 assert h.request('POST','/jobs',{'model':'must-fail'},'pg-unavailable')[0]==503
 h.compose('start','db');h.drain(120);h.verify()
 h.event('broker_restart_and_database_outage_verified')
 pub=h.compose('ps','-q','publisher');h.owned(pub,'publisher')
 h.command(['docker','kill','--signal','KILL',pub])
 late=h.post('publisher-down',{'model':'publisher','duration_ms':20})
 assert int(h.sql(f"SELECT count(*) FROM outbox WHERE job_id={late['id']} AND published_at IS NULL"))==1
 h.command(['docker','start',pub]);h.drain(90);h.verify()
 h.event('publisher_crash_recovered')
 # Force death AFTER durable result but BEFORE ACK.
 h.compose('stop','worker')
 cid=h.compose('run','-d','--no-deps','-e','TEST_BEFORE_ACK_DELAY=20s','worker').splitlines()[-1]
 h.owned(cid)
 try:
  j=h.post('commit-before-ack',{'model':'ack','duration_ms':20})
  wait_for(lambda:h.request('GET',f"/jobs/{j['id']}")[1].get('status')=='succeeded')
  h.signal(cid,'KILL');h.compose('start','worker')
  wait_for(lambda:get('rabbitmq','/api/queues/%2F/soup.jobs')['messages']==0,90)
  assert h.request('GET',f"/jobs/{j['id']}")[1]['attempts']==1
 finally:
  (h.directory/'ack-worker.log').write_text(h.command(['docker','logs',cid],combined=True))
  h.command(['docker','rm','-f',cid])
 h.verify();h.event('commit_before_ack_crash_verified')
 wait_for(lambda:query('soup_http_requests_total'),60)
 metrics=query('soup_jobs{state="succeeded"}')
 assert metrics and float(metrics[0]['value'][1])>0
 assert query('soup_http_duration_seconds_bucket')
 assert query('rabbitmq_queue_consumers{queue="soup.jobs"}')
 dashboard=get('grafana','/api/dashboards/uid/soup');assert len(dashboard['dashboard']['panels'])>=10
 (h.directory/'dashboard.json').write_text(json.dumps(dashboard))
 (h.directory/'prometheus.json').write_text(json.dumps({'jobs':metrics,'targets':get('prometheus','/api/v1/targets')}))
 # Search actual exported traces and require all lifecycle services in one trace.
 def traced():
  data=get('tempo','/api/search?limit=100')
  for tr in data.get('traces',[]):
   detail=get('tempo','/api/traces/'+tr['traceID']);raw=json.dumps(detail)
   if all(name in raw for name in ['postgres.Create','outbox.publish','worker.process','postgres.Finish']):
    (h.directory/'end-to-end-trace.json').write_text(raw);return True
  return False
 wait_for(traced,90)
 h.event('prometheus_grafana_tempo_verified')
 h.verify()
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--seed',type=int,default=42);run('v2',p.parse_args(),scenario)
