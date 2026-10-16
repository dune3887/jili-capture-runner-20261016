// 验收（真实脚本）：检查点只在连续上传成功时推进；工作流只有一个采集作业，没有 20 节点 matrix；
// 采集调度不再按旧泳道取模分片。
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path'),{execFileSync}=require('node:child_process');
const root=__dirname;
function deliverySandbox(delivered){
  const temp=fs.mkdtempSync(path.join(os.tmpdir(),'jl-delivery-'));
  fs.mkdirSync(path.join(temp,'output'),{recursive:true});
  fs.writeFileSync(path.join(temp,'checkpoint-finalize.cjs'),fs.readFileSync(path.join(root,'checkpoint-finalize.cjs')));
  fs.writeFileSync(path.join(temp,'output/delivery.json'),JSON.stringify({campaignId:'fixture',source:{runId:1,roundAt:'2026-10-15T14:41:15Z'},delivered}));
  for(const item of delivered)fs.writeFileSync(path.join(temp,`output/jl-state-${item.index}.enc.json`),JSON.stringify({state:item.index}));
  return temp;
}
function runFinalize(temp,outcomes){
  const result=execFileSync(process.execPath,[path.join(temp,'checkpoint-finalize.cjs')],{cwd:temp,encoding:'utf8',env:{...process.env,U0:outcomes[0],U1:outcomes[1],U2:outcomes[2],U3:outcomes[3]}});
  return JSON.parse(result.trim().split('\n').at(-1));
}
test('连续上传成功才推进检查点：中间一批失败时停在失败前一批',()=>{
  const temp=deliverySandbox([{index:2,gameIds:[1]},{index:3,gameIds:[2]},{index:4,gameIds:[3]}]);
  try{
    const summary=runFinalize(temp,['success','failure','success',undefined]);
    assert.deepEqual(summary.delivered,[2,3,4]);assert.deepEqual(summary.uploaded,[2]);
    assert.equal(summary.checkpoint,true);assert.equal(summary.failedSlot,1);
    assert.equal(JSON.parse(fs.readFileSync(path.join(temp,'state.enc.json'))).state,2,'检查点必须停在最后一批连续成功交付的批次');
  }finally{fs.rmSync(temp,{recursive:true,force:true});}
});
test('首批上传失败不得推进任何检查点',()=>{
  const temp=deliverySandbox([{index:0,gameIds:[1]},{index:1,gameIds:[2]}]);
  try{
    const summary=runFinalize(temp,['failure','success',undefined,undefined]);
    assert.equal(summary.checkpoint,false);assert.deepEqual(summary.uploaded,[]);
    assert.ok(!fs.existsSync(path.join(temp,'state.enc.json')),'上传失败时不得写出任何检查点');
  }finally{fs.rmSync(temp,{recursive:true,force:true});}
});
test('全部上传成功时检查点取最高批次',()=>{
  const temp=deliverySandbox([{index:5,gameIds:[1]},{index:6,gameIds:[2]}]);
  try{
    const summary=runFinalize(temp,['success','success',undefined,undefined]);
    assert.equal(summary.checkpoint,true);assert.equal(summary.lastBatch,6);
    assert.equal(JSON.parse(fs.readFileSync(path.join(temp,'state.enc.json'))).state,6);
  }finally{fs.rmSync(temp,{recursive:true,force:true});}
});
test('工作流只有一个采集作业，没有 20 节点 matrix 或旧泳道分片',()=>{
  // 本机保留在采集目录根，仓库里在 .github/workflows；两种布局都要能检查。
  const workflowFile=[path.join(root,'.github/workflows/jili-capture.yml'),path.join(root,'github-capture.yml')].find(file=>fs.existsSync(file));
  assert.ok(workflowFile,'必须能找到工作流文件');
  const workflow=fs.readFileSync(workflowFile,'utf8');
  assert.ok(!/matrix:/.test(workflow),'不得保留任何 matrix');
  assert.ok(!/strategy:/.test(workflow),'不得保留 strategy/matrix 配置');
  assert.ok(!/max-parallel/.test(workflow));
  assert.ok(!/fromJSON/.test(workflow));
  assert.equal((workflow.match(/runs-on: ubuntu-latest/g)||[]).length,1,'只允许一个标准 Linux 采集节点');
  assert.match(workflow,/group: jili-capture-campaign/,'必须与旧轮共用互斥组');
  assert.match(workflow,/cancel-in-progress: false/,'不得取消在途原件');
  assert.match(workflow,/github-runner\.cjs/);
  assert.ok(!/campaign-worker\.cjs/.test(workflow),'批次由运行器调度，工作流不再直接启动泳道进程');
  assert.ok((workflow.match(/actions\/upload-artifact@/g)||[]).length>=4,'每批独立上传，上传失败不能推进检查点');
  assert.match(workflow,/steps\.finalize\.outputs\.checkpoint == 'true'/,'只有上传成功才保存检查点');
  for(const name of ['campaign-contract.cjs','campaign-worker.cjs','github-runner.cjs','campaign-state.cjs']){
    const source=fs.readFileSync(path.join(root,name),'utf8');
    assert.ok(!/laneGames/.test(source),name+' 不得再按旧 20 节点×5 泳道取模分片');
    assert.ok(!/lane\b/.test(source)||/legacyLanes|lane-\\d|泳道|旧/.test(source),name+' 不得把旧泳道编号当成队列状态');
  }
  const runner=fs.readFileSync(path.join(root,'github-runner.cjs'),'utf8');
  assert.match(runner,/planBatches|queuePlan/,'运行器必须用真实 gameId 分批');
  assert.equal((runner.match(/spawn\(/g)||[]).length,1,'运行器只启动批次工作进程');
  assert.match(runner,/jl-batch-/,'必须按批独立加密落盘');
  assert.ok(!/length:5|,5\)/.test(runner),'不得固定启动 5 个泳道进程');
});
console.log('检查点只在连续上传成功时推进、单节点单作业与分批调度检查通过');
