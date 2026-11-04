// Pure rendering and mocked async interaction; not browser or layout testing.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '..', 'prototype', 'app.js'), 'utf8');
const start = source.indexOf('function encryptedBackupPanel()');
const end = source.indexOf('async function checkIntegrity(', start);
assert.ok(start >= 0 && end > start);

function setup() {
  const values = {password:'test-password-long',password_repeat:'test-password-long',acknowledge:'on'};
  const status = {textContent:''}, button = {disabled:false};
  const form = {dataset:{},isConnected:true,querySelector:()=>button,reset(){values.password='';values.password_repeat='';values.acknowledge=null;}};
  const link = {click(){this.clicked=true;},remove(){}};
  const c = {tag:x=>x,backend:{csrf:'csrf-secret',ready:true},connectionEpoch:0,state:{page:'data'},el:id=>id==='encrypted-backup-status'?status:null,
    FormData:class {get(key){return values[key];}},
    document:{addEventListener(){},createElement:()=>link,body:{dataset:{},append(){}}},
    URL:{createObjectURL:()=> 'blob:test',revokeObjectURL(){}},setTimeout(){}};
  vm.runInNewContext(source.slice(start,end),c);
  return {c,values,status,button,form,link};
}

test('backup form describes original data, passphrase and incomplete recovery',()=>{
  const {c}=setup(), html=c.encryptedBackupPanel();
  assert.match(html,/不是脱敏导出/);
  assert.match(html,/口令遗失后无法恢复/);
  assert.match(html,/完整恢复验收尚未完成/);
  assert.match(html,/type="password"/);
});

test('encrypted download uses authenticated request and clears form',async()=>{
  const {c,values,status,button,form,link}=setup();
  let posted;
  c.fetch=async(url,options)=>{posted={url,options};return {ok:true,blob:async()=>({encrypted:true})};};
  await c.downloadEncryptedBackup(form);
  assert.equal(posted.url,'/api/encrypted-backup');
  assert.equal(posted.options.headers['X-DLT-CSRF'],'csrf-secret');
  assert.equal(JSON.parse(posted.options.body).password,'test-password-long');
  assert.equal(values.password,'');
  assert.equal(link.clicked,true);
  assert.equal(button.disabled,false);
  assert.match(status.textContent,/交给浏览器下载/);
  assert.doesNotMatch(status.textContent,/test-password-long/);
});

test('password mismatch and server failure do not download a file',async()=>{
  const {c,values,status,form,link}=setup();
  values.password_repeat='different-password';
  c.fetch=()=>{throw Error('must not call');};
  await c.downloadEncryptedBackup(form);
  assert.match(status.textContent,/不一致/);
  assert.equal(link.clicked,undefined);
  values.password_repeat=values.password;
  c.fetch=async()=>({ok:false,json:async()=>({error:'数据库不可用'})});
  await c.downloadEncryptedBackup(form);
  assert.match(status.textContent,/数据库不可用/);
  assert.equal(values.password,'');
  assert.equal(link.clicked,undefined);
});
