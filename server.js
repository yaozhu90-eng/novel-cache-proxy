import http from 'node:http';
import crypto from 'node:crypto';

const env=process.env, PORT=Number(env.PORT||3000), API=env.OPENROUTER_API_KEY, PASS=env.PROXY_PASSWORD;
const SB=(env.SUPABASE_URL||'').replace(/\/+$/,''), KEY=env.SUPABASE_SECRET_KEY, DIAG=env.CACHE_DIAGNOSTICS==='true';
const T='cherry_cache_breakpoints',locks=new Map(),sha=s=>crypto.createHash('sha256').update(s).digest('hex');
const omit=(k,v)=>k==='prompt_cache_breakpoint'?undefined:v;
const log=(name,x)=>console.log(name,JSON.stringify(x));

function reply(res,n,x){
  res.writeHead(n,{'Content-Type':'application/json','Cache-Control':'no-store'});
  res.end(JSON.stringify(x));
}

function authorized(req){
  if(!PASS)return false;
  const a=Buffer.from(req.headers.authorization||'');
  const b=Buffer.from('Bearer '+PASS);
  return a.length===b.length&&crypto.timingSafeEqual(a,b);
}

async function read(req){
  let n=0,parts=[];
  for await(const c of req){
    n+=c.length;
    if(n>12582912)throw Error('Request too large');
    parts.push(c);
  }
  return JSON.parse(Buffer.concat(parts).toString('utf8'));
}

function identity(ms){
  let i=ms.findIndex(m=>m?.role==='user');
  return 'cherry-'+sha(JSON.stringify({
    initialMessages:ms.slice(0,i<0?1:i+1)
  })).slice(0,40);
}

function fingerprint(m){
  return m?.role==='user'
    ?sha(typeof m.content==='string'
      ?m.content:JSON.stringify(m.content??null,omit))
    :null;
}

function normalize(ms){
  for(const m of ms)
    if(m?.role==='user'&&typeof m.content==='string'&&m.content)
      m.content=[{type:'text',text:m.content}];
}

function mark(ms,i){
  const m=ms[i];
  if(!m)return false;
  if(typeof m.content==='string'&&m.content)
    m.content=[{type:'text',text:m.content}];
  if(!Array.isArray(m.content))return false;
  for(let j=m.content.length-1;j>=0;j--){
    const p=m.content[j];
    if(p?.type==='text'&&p.text){
      m.content[j]={
        ...p,
        prompt_cache_breakpoint:{mode:'explicit',ttl:'12h'}
      };
      return true;
    }
  }
  return false;
}

function setCache(b,state,fp){
  const ms=b.messages;
  for(const m of ms)
    if(Array.isArray(m?.content))
      for(const p of m.content)
        if(p&&typeof p==='object')
          delete p.prompt_cache_breakpoint;

  b.prompt_cache_options={mode:'explicit',ttl:'12h'};

  const users=ms.flatMap((m,i)=>m?.role==='user'?[i]:[]);
  const cur=users.at(-1);
  let old=null;

  if(state?.message_fingerprint){
    const i=state.message_index;
    if(Number.isInteger(i)&&i<cur&&ms[i]?.role==='user'&&
       fp[i]===state.message_fingerprint){
      old=i;
    }else{
      for(let j=users.length-2;j>=0;j--)
        if(fp[users[j]]===state.message_fingerprint){
          old=users[j];
          break;
        }
    }
  }

  const fromState=old!==null;
  if(old===null)old=users.at(-2)??null;

  const marks=[];
  for(const i of [old,cur])
    if(Number.isInteger(i)&&!marks.includes(i)&&mark(ms,i))
      marks.push(i);

  return {old,cur,marks,fromState};
}

function snap(b,i){
  const ms=b.messages,m=ms[i];
  if(!Array.isArray(m?.content))return null;
  const j=m.content.findLastIndex(p=>p?.prompt_cache_breakpoint);
  if(j<0)return null;
  const p=[
    ...ms.slice(0,i),
    {...m,content:m.content.slice(0,j+1)}
  ];
  return {
    h:sha(JSON.stringify(p,omit)),
    parts:p.map(x=>sha(JSON.stringify(x,omit)))
  };
}

function context(b){
  const x={...b};
  for(const k of [
    'messages','session_id','stream','stream_options',
    'prompt_cache_options'
  ])delete x[k];
  return sha(JSON.stringify(x));
}

function firstDiff(a,b){
  if(!Array.isArray(a)||!Array.isArray(b))return null;
  for(let i=0;i<Math.max(a.length,b.length);i++)
    if(a[i]!==b[i])return i;
  return null;
}

async function db(path,opts={}){
  const r=await fetch(SB+'/rest/v1/'+path,{
    ...opts,
    headers:{
      apikey:KEY,
      Authorization:'Bearer '+KEY,
      ...opts.headers
    },
    signal:AbortSignal.timeout(4000)
  });
  if(!r.ok)throw Error('Supabase '+r.status);
  const s=await r.text();
  return s?JSON.parse(s):null;
}

async function load(s,k){
  const q=new URLSearchParams({
    select:'breakpoint_data',
    session_id:'eq.'+s,
    model_key:'eq.'+k,
    limit:'1'
  });
  return (await db(T+'?'+q))?.[0]?.breakpoint_data??null;
}

async function save(s,k,v){
  await db(T+'?on_conflict=session_id,model_key',{
    method:'POST',
    headers:{
      'Content-Type':'application/json',
      Prefer:'resolution=merge-duplicates,return=minimal'
    },
    body:JSON.stringify({
      session_id:s,
      model_key:k,
      breakpoint_data:v,
      updated_at:new Date().toISOString()
    })
  });
}

async function guarded(s,k,v){
  const id=s+'|'+k,p=locks.get(id)||Promise.resolve();
  const task=p.catch(()=>{}).then(async()=>{
    const prev=await load(s,k);
    if(prev?.started_ms>v.started_ms)return 'newer_exists';
    if(prev?.started_ms===v.started_ms&&
       prev?.message_index>v.message_index)return 'newer_index';
    if(prev?.prefix_sha256===v.prefix_sha256&&
       prev?.message_index===v.message_index)return 'same';
    await save(s,k,v);
    return 'saved';
  });
  locks.set(id,task);
  try{return await task;}
  finally{if(locks.get(id)===task)locks.delete(id);}
}

function usageCount(u){
  const p=u?.prompt_tokens_details||{};
  return {
    cached:p.cached_tokens??u?.cached_tokens??null,
    write:p.cache_write_tokens??u?.cache_write_tokens??null
  };
}
async function forward(req,res,path,b,trace){
  if(DIAG&&b)log('CACHE_DIAG_REQUEST',{
    trace_id:trace,
    model:b.model,
    provider:b.provider,
    service_tier:b.service_tier,
    session_id:b.session_id,
    breakpoints:b.messages.flatMap((m,i)=>
      Array.isArray(m.content)&&
      m.content.some(p=>p?.prompt_cache_breakpoint)?[i]:[]
    ),
    prompt_cache_options:b.prompt_cache_options
  });

  const r=await fetch('https://openrouter.ai/api/v1'+path,{
    method:req.method,
    headers:{
      Authorization:'Bearer '+API,
      'Content-Type':'application/json'
    },
    ...(b?{body:JSON.stringify(b)}:{})
  });

  const type=r.headers.get('content-type')||'application/json';
  res.writeHead(r.status,{
    'Content-Type':type,
    'Cache-Control':'no-store'
  });

  if(!r.body){
    res.end();
    return {status:r.status,ok:true,usage:null};
  }

  let buffer='',usage=null,finished=false,aborted=false;
  const sse=type.includes('text/event-stream');
  const dec=new TextDecoder();

  const record=o=>{
    if(o?.usage){
      usage=o.usage;
      if(DIAG)log('CACHE_DIAG_USAGE',{trace_id:trace,usage});
    }
  };

  function event(s){
    const x=s.split('\n')
      .filter(x=>x.startsWith('data:'))
      .map(x=>x.slice(5).trimStart())
      .join('\n');
    if(x&&x!=='[DONE]')
      try{record(JSON.parse(x));}catch{}
  }

  try{
    for await(const c of r.body){
      if(res.destroyed){
        aborted=true;
        break;
      }
      res.write(c);
      if(!b)continue;

      buffer+=dec.decode(c,{stream:true});
      if(sse){
        buffer=buffer.replace(/\r\n/g,'\n');
        let j;
        while((j=buffer.indexOf('\n\n'))>=0){
          event(buffer.slice(0,j));
          buffer=buffer.slice(j+2);
        }
        if(buffer.length>131072)buffer='';
      }else if(buffer.length>131072){
        buffer='';
      }
    }

    finished=!aborted;
    if(b){
      buffer+=dec.decode();
      if(sse){
        if(buffer.trim())event(buffer);
      }else{
        try{record(JSON.parse(buffer));}catch{}
      }
    }
  }finally{
    if(!res.destroyed)res.end();
  }

  if(DIAG&&b)log('CACHE_DIAG_RESULT',{
    trace_id:trace,
    status:r.status,
    finished,
    ...usageCount(usage)
  });

  return {status:r.status,ok:finished,usage};
}

http.createServer(async(req,res)=>{
  try{
    const path=new URL(req.url,'http://localhost').pathname;

    if(path==='/health'&&req.method==='GET')
      return reply(res,200,{
        status:'ok',
        version:'route-isolated-v3',
        ttl:'12h',
        routes:{
          standard:'openai',
          floor:'openai/flex'
        },
        diagnostics:DIAG
      });

    if(!API||!PASS||!SB||!KEY)
      return reply(res,503,{error:'Environment variables missing'});

    if(!authorized(req))
      return reply(res,401,{error:'Unauthorized'});

    if(path==='/v1/models'&&req.method==='GET'){
      await forward(req,res,'/models',null,crypto.randomUUID());
      return;
    }

    if(path!=='/v1/chat/completions'||req.method!=='POST')
      return reply(res,404,{error:'Not found'});

    const b=await read(req);
    if(!b||!Array.isArray(b.messages))
      return reply(res,400,{error:'messages required'});

    const trace=crypto.randomUUID();
    const started=Date.now();
    const floor=typeof b.model==='string'&&b.model.endsWith(':floor');

    b.service_tier=floor?'flex':'default';
    b.provider={
      only:floor?['openai/flex']:['openai'],
      allow_fallbacks:false
    };
    delete b.models;

    const base=identity(b.messages);
    b.session_id=base+(floor?'-flex':'');

    const key=JSON.stringify([b.model||'',b.service_tier]);
    const fp=b.messages.map(fingerprint);
    normalize(b.messages);

    log('CACHE_ROUTE_PIN',{
      trace_id:trace,
      model:b.model,
      service_tier:b.service_tier,
      provider_only:b.provider.only
    });

    let state=null;
    try{
      state=await load(b.session_id,key);
      log('CACHE_STATE_LOAD_OK',{
        trace_id:trace,
        found:!!state,
        service_tier:b.service_tier
      });
    }catch(e){
      log('CACHE_STATE_LOAD_ERROR',{
        trace_id:trace,
        error:e.message
      });
    }

    const {old,cur,marks,fromState}=setCache(b,state,fp);
    const before=snap(b,old);
    const after=snap(b,cur);
    const ctx=context(b);

    if(DIAG)log('CACHE_PREFIX_COMPARE',{
      trace_id:trace,
      session_id:b.session_id,
      previous_index:old,
      current_index:cur,
      previous_message_matches_saved:fromState,
      previous_prefix_match:
        state?.prefix_sha256&&before
          ?state.prefix_sha256===before.h
          :null,
      first_different_message_index:
        firstDiff(state?.prefix_message_hashes,before?.parts),
      other_request_fields_match:
        state?.request_context_sha256
          ?state.request_context_sha256===ctx
          :null
    });

    if(b.stream===true)
      b.stream_options={
        ...(b.stream_options||{}),
        include_usage:true
      };

    const result=await forward(
      req,res,'/chat/completions',b,trace
    );

    const {cached,write}=usageCount(result.usage);

    if(!(
      result.status>=200&&
      result.status<300&&
      result.ok&&
      after&&
      marks.includes(cur)&&
      fp[cur]&&
      typeof write==='number'&&
      write>0
    )){
      log('CACHE_STATE_SAVE_SKIPPED',{
        trace_id:trace,
        cache_write_tokens:write,
        cached_tokens:cached
      });
      return;
    }

    try{
      const outcome=await guarded(b.session_id,key,{
        message_index:cur,
        message_fingerprint:fp[cur],
        prefix_sha256:after.h,
        prefix_message_hashes:after.parts,
        request_context_sha256:ctx,
        started_ms:started,
        route_tier:b.service_tier
      });

      log(
        outcome==='saved'
          ?'CACHE_STATE_SAVE_OK'
          :'CACHE_STATE_SAVE_SKIPPED',
        {
          trace_id:trace,
          outcome,
          message_index:cur,
          cache_write_tokens:write,
          cached_tokens:cached
        }
      );
    }catch(e){
      log('CACHE_STATE_SAVE_ERROR',{
        trace_id:trace,
        error:e.message
      });
    }
  }catch(e){
    console.error('Proxy error:',e.message);
    if(!res.headersSent)
      reply(res,502,{error:e.message});
    else
      res.destroy();
  }
}).listen(PORT,'0.0.0.0',()=>{
  console.log('Proxy listening on port '+PORT);
});
