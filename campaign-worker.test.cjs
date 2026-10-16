// 验收（真实沙箱）：批内有界并发、每款每波恰好一个执行位置、超时安全收尾、
// 业务拒绝暂停保持、部分通过的原件不丢且不重放、429/网络退避不提前恢复。
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),{execFileSync}=require('node:child_process');
const campaignId='fixture-jl-batch';
function sandbox(games,config={}){
  const temp=fs.mkdtempSync(path.join(os.tmpdir(),'jl-batch-worker-'));
  for(const name of ['campaign-worker.cjs','campaign-contract.cjs','campaign-state.cjs'])fs.copyFileSync(path.join(__dirname,name),path.join(temp,name));
  fs.writeFileSync(path.join(temp,'campaign-manifest.json'),JSON.stringify({campaignId,targets:{normal:300001,feature:20001},games,blocked:[]}));
  fs.writeFileSync(path.join(temp,'receipts.json'),JSON.stringify({campaignId,artifacts:[],confirmed:config.confirmed||{},batches:{},clientFlowStates:config.clientFlowStates||{}}));
  fs.writeFileSync(path.join(temp,'campaign-pauses.json'),JSON.stringify(config.pauses||{}));
  const state=require('./campaign-state.cjs').emptyState(campaignId);
  for(const [module,entry] of Object.entries(config.states||{}))state.games[module]=entry;
  fs.mkdirSync(path.join(temp,'state'),{recursive:true});
  fs.writeFileSync(path.join(temp,'state/queue-state.json'),JSON.stringify(state));
  const timing=path.join(temp,'timing.log'),counters=path.join(temp,'counters.log');
  fs.writeFileSync(timing,'');fs.writeFileSync(counters,'');
  fs.writeFileSync(path.join(temp,'test-config.json'),JSON.stringify({campaignId,timing,counters,sleepMs:config.sleepMs??120,fail:config.fail||[],ids:config.ids||{}}));
  // 采集子进程桩：真实创建原件目录与 accepted.json，可注入网络/业务拒绝与耗时。
  fs.writeFileSync(path.join(temp,'capture-jl.cjs'),`const fs=require('node:fs'),path=require('node:path');
const code=process.argv[2],count=Number(process.argv[3]||0),mode=process.argv[4]||'0';
const config=JSON.parse(fs.readFileSync(path.join(__dirname,'test-config.json')));
const root=path.resolve(process.env.JL_CAPTURE_PRIVATE_DIR||path.join(__dirname,'private'));
fs.mkdirSync(root,{recursive:true,mode:0o700});
const out=fs.mkdtempSync(path.join(root,code+'-native-'));
fs.appendFileSync(config.counters,code+String.fromCharCode(10));
fs.appendFileSync(config.timing,JSON.stringify({code,event:'start',at:Date.now()})+'\\n');
const finish=(status)=>{fs.appendFileSync(config.timing,JSON.stringify({code,event:'end',at:Date.now()})+'\\n');process.exit(status);};
setTimeout(()=>{
 const failure=config.fail.find(item=>item.code===code);
 if(failure){
  if(failure.rounds)fs.writeFileSync(path.join(out,'accepted.json'),JSON.stringify({evidenceVersion:1,campaignId:config.campaignId,gameId:config.ids[code],gameCode:code,rounds:failure.rounds,mode:Number(mode)}));
  if(failure.http!==undefined)fs.writeFileSync(path.join(out,'http-failure.json'),JSON.stringify({status:failure.http,retryAfterSeconds:failure.retryAfterSeconds??null}));
  if(failure.ret!==undefined)fs.writeFileSync(path.join(out,'source-failure.json'),JSON.stringify({ret:failure.ret,reason:failure.reason}));
  fs.writeFileSync(path.join(out,'failure.txt'),failure.reason||'来源失败');
  return finish(1);
 }
 fs.writeFileSync(path.join(out,'accepted.json'),JSON.stringify({evidenceVersion:1,campaignId:config.campaignId,gameId:config.ids[code],gameCode:code,rounds:count,mode:Number(mode)}));
 fs.writeFileSync(path.join(out,'spin-0.envelope.json'),JSON.stringify({round:0}));
 finish(0);
},config.sleepMs);
`);
  // 离线复验桩：真实读取 accepted.json 的局数，复验失败可注入。
  fs.writeFileSync(path.join(temp,'jl-data.cjs'),`const fs=require('node:fs'),path=require('node:path');
const file=path.join(process.argv[3],'accepted.json');
if(!fs.existsSync(file)){process.stdout.write(JSON.stringify({rounds:0}));process.exit(0);}
const data=JSON.parse(fs.readFileSync(file));
if(JSON.parse(fs.readFileSync(path.join(__dirname,'test-config.json'))).verifyFail)process.exit(1);
process.stdout.write(JSON.stringify({rounds:data.rounds}));
`);
  return temp;
}
function runWorker(temp,batchIndex,ids,env={}){
  const result=execFileSync(process.execPath,[path.join(temp,'campaign-worker.cjs'),String(batchIndex),ids.join(',')],{
    cwd:temp,encoding:'utf8',timeout:60000,
    env:{PATH:process.env.PATH,LANG:'C.UTF-8',JL_CAMPAIGN_ID:campaignId,JL_PHASE:'normal',JL_SPIN_INTERVAL_MS:'5000',JL_REQUEST_INTERVAL_MS:'500',...env},
  });
  return result.trim().split('\n').map(line=>{try{return JSON.parse(line);}catch{return line;}});
}
function stateOf(temp){return JSON.parse(fs.readFileSync(path.join(temp,'state/queue-state.json')));}
function countersOf(temp){const counts={};for(const line of fs.readFileSync(path.join(temp,'counters.log'),'utf8').split('\n').filter(Boolean))counts[line]=(counts[line]||0)+1;return counts;}
function recordOf(temp,index){return JSON.parse(fs.readFileSync(path.join(temp,`state/batch-${index}.json`)));}
function maxConcurrency(temp){
  const events=fs.readFileSync(path.join(temp,'timing.log'),'utf8').trim().split('\n').filter(Boolean).map(line=>JSON.parse(line)).sort((a,b)=>a.at-b.at);
  let live=0,peak=0;
  for(const event of events){live+=event.event==='start'?1:-1;peak=Math.max(peak,live);}
  return peak;
}
const games=[{id:1,code:'a',module:'m1'},{id:2,code:'b',module:'m2'},{id:3,code:'c',module:'m3'}];
test('有界并发与每款每波一个执行位置：3 款并发 2、3 波共 6 个位置',()=>{
  const temp=sandbox(games,{ids:{a:1,b:2,c:3},sleepMs:120});
  try{
    const lines=runWorker(temp,0,[1,2,3],{JL_BATCH_CONCURRENCY:'2',JL_BATCH_MINUTES:'2',JL_BATCH_MAX_WAVES:'3',JL_ROUND_BATCH:'30'});
    const summary=lines.find(item=>item&&item.batchIndex===0&&Array.isArray(item.gameIds));
    assert.equal(summary.waves,3);assert.equal(summary.slots,6,'3 波 × 2 并发 = 6 个执行位置');
    assert.equal(summary.captured,180,'6 次 × 30 局');
    assert.ok(maxConcurrency(temp)<=2,`实际并发 ${maxConcurrency(temp)} 不得超过配置 2`);
    const state=stateOf(temp);
    for(const module of ['m1','m2','m3'])assert.equal(state.games[module].counts['0'],60,module+' 必须获得相等的执行位置');
    const record=recordOf(temp,0);
    assert.deepEqual(record.gameIds,[1,2,3]);assert.equal(record.concurrency,2);assert.equal(record.roundBatch,30);
    assert.equal(record.source.runId,0);
    assert.equal(fs.readdirSync(path.join(temp,'samples/batch-0')).length,6,'6 次采集的原件都必须落盘');
    for(const module of ['m1','m2','m3'])assert.equal(state.games[module].status,'running');
  }finally{if(!process.env.JL_KEEP_SANDBOX)fs.rmSync(temp,{recursive:true,force:true});else console.log('沙箱保留：'+temp);}
});
test('运行时间到点安全收尾并保留本批原件',()=>{
  const temp=sandbox(games,{ids:{a:1,b:2,c:3},sleepMs:250});
  try{
    const startedAt=Date.now();
    runWorker(temp,0,[1,2,3],{JL_BATCH_CONCURRENCY:'2',JL_BATCH_MINUTES:'0.01',JL_ROUND_BATCH:'30'});
    const elapsed=Date.now()-startedAt;
    const record=recordOf(temp,0);
    assert.ok(record.waves>=1,'截止时间前必须至少完成一波，不能零进展收尾');
    assert.ok(elapsed<20000,'到点必须及时收尾，不能超过本批时间预算太多');
    assert.ok(fs.readdirSync(path.join(temp,'samples/batch-0')).length>=2,'已通过的原件必须保留');
    assert.ok(record.finishedAt>=record.startedAt);
  }finally{if(!process.env.JL_KEEP_SANDBOX)fs.rmSync(temp,{recursive:true,force:true});else console.log('沙箱保留：'+temp);}
});
test('业务拒绝暂停保持、部分通过原件不丢且不重放',()=>{
  const temp=sandbox([{id:1,code:'a',module:'m1'},{id:4,code:'fail',module:'m4'}],{ids:{a:1,fail:4},sleepMs:120,fail:[{code:'fail',ret:304,reason:'下注限额（范围待核实）',rounds:0}]});
  try{
    runWorker(temp,0,[1,4],{JL_BATCH_CONCURRENCY:'2',JL_BATCH_MINUTES:'1',JL_BATCH_MAX_WAVES:'3',JL_ROUND_BATCH:'30'});
    const state=stateOf(temp),counters=countersOf(temp);
    assert.equal(state.games.m4.status,'blocked','官方业务拒绝必须暂停该游戏');
    assert.match(state.games.m4.reason,/下注限额/);
    assert.equal(state.games.m4.counts['0']||0,0);
    assert.equal(counters.fail,1,'业务拒绝后不得重放该游戏');
    assert.equal(counters.a,3,'其他游戏继续按波次轮转，不被暂停游戏拖住');
    assert.ok(fs.readdirSync(path.join(temp,'samples/batch-0')).length>=1,'被拒原件必须保留在批次内');
  }finally{if(!process.env.JL_KEEP_SANDBOX)fs.rmSync(temp,{recursive:true,force:true});else console.log('沙箱保留：'+temp);}
});
test('部分通过 + 网络失败：原件保留、计数保留、按退避暂停不重放',()=>{
  const temp=sandbox([{id:1,code:'a',module:'m1'},{id:5,code:'net',module:'m5'}],{ids:{a:1,net:5},sleepMs:80,fail:[{code:'net',http:0,reason:'来源网络失败（未重放 Spin）',rounds:2}]});
  try{
    runWorker(temp,0,[1,5],{JL_BATCH_CONCURRENCY:'2',JL_BATCH_MINUTES:'1',JL_BATCH_MAX_WAVES:'2',JL_ROUND_BATCH:'30'});
    const state=stateOf(temp),counters=countersOf(temp);
    assert.equal(state.games.m5.counts['0'],2,'已通过的部分局数必须保留');
    assert.equal(state.games.m5.status,'paused');
    assert.ok(state.games.m5.retryAt>Date.now(),'网络失败必须留退避时间');
    assert.equal(counters.net,1,'退避期内不得重放该游戏');
    assert.ok(fs.readdirSync(path.join(temp,'samples/batch-0')).length>=1);
  }finally{if(!process.env.JL_KEEP_SANDBOX)fs.rmSync(temp,{recursive:true,force:true});else console.log('沙箱保留：'+temp);}
});
test('已暂停/已达标游戏不占执行位置，暂停保持不被清除',()=>{
  const temp=sandbox(games,{ids:{a:1,b:2,c:3},sleepMs:80,pauses:{m2:{status:'blocked',reason:'官方业务拒绝 ret=304：下注限额'}},states:{m3:{counts:{'0':300001},status:'running'}}});
  try{
    runWorker(temp,0,[1,2,3],{JL_BATCH_CONCURRENCY:'2',JL_BATCH_MINUTES:'1',JL_BATCH_MAX_WAVES:'1',JL_ROUND_BATCH:'30'});
    const state=stateOf(temp),counters=countersOf(temp);
    assert.equal(counters.b,undefined,'暂停游戏不得被采集');
    assert.equal(counters.c,undefined,'普通已达标游戏不得继续普通采集');
    assert.equal(state.games.m2.status,'blocked');assert.equal(state.games.m2.reason,'官方业务拒绝 ret=304：下注限额');
    assert.equal(counters.a,1);
    const record=recordOf(temp,0);assert.equal(record.slots,1,'只有真正执行的游戏占用执行位置');
  }finally{if(!process.env.JL_KEEP_SANDBOX)fs.rmSync(temp,{recursive:true,force:true});else console.log('沙箱保留：'+temp);}
});
console.log('批内有界并发、超时收尾、暂停保持与不重放检查通过');
