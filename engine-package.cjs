// 网页大文件提交不稳定时分两份发布；只拆密文，AES-GCM仍校验拼回后的完整包。
const fs=require('node:fs'),path=require('node:path'),assert=require('node:assert/strict');
function writeParts(directory,envelope){
  assert.equal(typeof envelope.data,'string');
  const middle=Math.ceil(envelope.data.length/2);
  fs.writeFileSync(path.join(directory,'engine.enc.1.json'),JSON.stringify({...envelope,data:envelope.data.slice(0,middle)}));
  fs.writeFileSync(path.join(directory,'engine.enc.2.json'),JSON.stringify({data:envelope.data.slice(middle)}));
}
function read(directory){
  const first=path.join(directory,'engine.enc.1.json');
  if(!fs.existsSync(first))return JSON.parse(fs.readFileSync(path.join(directory,'engine.enc.json')));
  const envelope=JSON.parse(fs.readFileSync(first)),second=JSON.parse(fs.readFileSync(path.join(directory,'engine.enc.2.json')));
  assert.equal(typeof envelope.data,'string');assert.equal(typeof second.data,'string');
  return {...envelope,data:envelope.data+second.data};
}
module.exports={read,writeParts};
