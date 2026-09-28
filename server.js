// ElmForge realtime server: node server.js  ->  http://localhost:3000
const http=require('http'),fs=require('fs'),path=require('path'),crypto=require('crypto');
const FILE=path.join(__dirname,'data.json'),PORT=process.env.PORT||3000;
let db={users:{},sess:{},servers:{},msgs:{},notifs:{}};
try{Object.assign(db,JSON.parse(fs.readFileSync(FILE,'utf8')))}catch(e){}
let tm;const save=()=>{clearTimeout(tm);tm=setTimeout(()=>fs.writeFile(FILE,JSON.stringify(db),()=>{}),300)};
const rid=n=>crypto.randomBytes(n).toString('hex'),hp=(p,s)=>crypto.scryptSync(p,s,32).toString('hex');
const socks=new Map(),U=id=>db.users[id],isOn=id=>socks.has(id);
const send=(w,o)=>w.readyState===1&&w.send(JSON.stringify(o));
const to=(id,o)=>(socks.get(id)||[]).forEach(w=>send(w,o));
const pub=id=>{const u=U(id);return u&&{id:u.id,name:u.name,user:u.user,on:isOn(id)}};
function state(u){return{t:'state',me:pub(u.id),friends:u.friends.map(pub),inReq:u.inn.map(pub),outReq:u.out.map(pub),
 servers:Object.values(db.servers).filter(s=>s.members.includes(u.id)).map(s=>({id:s.id,name:s.name,code:s.code,owner:s.owner,ch:s.ch,members:s.members.map(pub)})),
 notifs:(db.notifs[u.id]||[]).slice(-40),
 people:[...socks.keys()].filter(i=>i!==u.id).slice(0,30).map(i=>({...pub(i),fr:+u.friends.includes(i),pend:+u.out.includes(i)})),online:socks.size}}
const push=id=>{if(isOn(id)&&U(id))to(id,state(U(id)))},pushAll=()=>[...socks.keys()].forEach(i=>push(i));
function notify(id,type,text){const n={id:rid(4),type,text,ts:Date.now(),r:0},a=(db.notifs[id]=db.notifs[id]||[]);a.push(n);if(a.length>100)a.shift();save();to(id,{t:'notif',n})}
function part(room,uid){const p=String(room).split(':');
 if(p[0]==='s'){const s=db.servers[p[1]];return s&&s.members.includes(uid)&&s.ch.some(c=>c.id===p[2])?s.members:null}
 if(p[0]==='d'&&p.length===3&&p.includes(uid)&&U(p[1])&&U(p[2])&&U(p[1]).friends.includes(p[2]))return[p[1],p[2]];return null}
function accept(u,o){for(const[a,b]of[[u,o],[o,u]]){a.inn=a.inn.filter(x=>x!==b.id);a.out=a.out.filter(x=>x!==b.id);if(!a.friends.includes(b.id))a.friends.push(b.id)}
 save();notify(o.id,'friend_accept',u.name+' accepted your friend request');to(u.id,{t:'ok',m:'You and '+o.name+' are now friends'});push(u.id);push(o.id)}
function login(ws,u,token){token=token||rid(24);db.sess[token]=u.id;save();ws.uid=u.id;if(!socks.has(u.id))socks.set(u.id,new Set());socks.get(u.id).add(ws);send(ws,{t:'auth',token,me:pub(u.id)});pushAll()}
const srv=http.createServer((q,r)=>fs.readFile(path.join(__dirname,'elmforge.html'),(e,d)=>{if(e){r.writeHead(404);return r.end('elmforge.html must be next to server.js')}r.writeHead(200,{'content-type':'text/html; charset=utf-8'});r.end(d)}));
const GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
function frame(s,op,p){const n=p.length;let h;if(n<126)h=Buffer.from([128|op,n]);else if(n<65536){h=Buffer.alloc(4);h[0]=128|op;h[1]=126;h.writeUInt16BE(n,2)}else{h=Buffer.alloc(10);h[0]=128|op;h[1]=127;h.writeBigUInt64BE(BigInt(n),2)}s.write(Buffer.concat([h,p]))}
// tiny built-in WebSocket implementation (no npm install needed)
srv.on('upgrade',(req,sock)=>{const key=req.headers['sec-websocket-key'];if(!key)return sock.destroy();
 sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+crypto.createHash('sha1').update(key+GUID).digest('base64')+'\r\n\r\n');
 const ws={readyState:1,h:{},send(d){if(ws.readyState===1)frame(sock,1,Buffer.from(d))},on(e,f){ws.h[e]=f}};let buf=Buffer.alloc(0);
 sock.on('data',d=>{buf=Buffer.concat([buf,d]);for(;;){if(buf.length<2)return;const op=buf[0]&15,mk=buf[1]&128;let len=buf[1]&127,off=2;
  if(len===126){if(buf.length<4)return;len=buf.readUInt16BE(2);off=4}else if(len===127){if(buf.length<10)return;len=Number(buf.readBigUInt64BE(2));off=10}
  if(len>1e6)return sock.destroy();const need=off+(mk?4:0)+len;if(buf.length<need)return;
  let p=Buffer.from(buf.slice(off+(mk?4:0),need));if(mk){const k=buf.slice(off,off+4);for(let i=0;i<p.length;i++)p[i]^=k[i&3]}
  buf=buf.slice(need);if(op===8)return sock.end();if(op===9)frame(sock,10,p);else if(op===1&&ws.h.message)ws.h.message(p.toString())}});
 const close=()=>{if(ws.readyState===1){ws.readyState=3;ws.h.close&&ws.h.close()}};sock.on('close',close);sock.on('error',close);wssConn(ws)});
const wssConn=ws=>{
 ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}
  const err=x=>send(ws,{t:'err',m:x});
  if(!ws.uid){
   if(m.t==='signup'){const email=String(m.email||'').trim().toLowerCase(),user=String(m.user||'').trim(),name=String(m.name||'').trim().slice(0,30),pass=String(m.pass||'');
    if(!/^\S+@\S+\.\S+$/.test(email))return err('Enter a valid email address.');
    if(name.length<2)return err('Display name is too short.');
    if(!/^[a-z0-9_.]{3,20}$/i.test(user))return err('Username: 3–20 letters, numbers, _ or .');
    if(pass.length<6)return err('Password must be at least 6 characters.');
    if(Object.values(db.users).some(x=>x.email===email||x.user.toLowerCase()===user.toLowerCase()))return err('That email or username is already registered.');
    const salt=rid(8),u={id:rid(6),email,user,name,salt,h:hp(pass,salt),friends:[],inn:[],out:[]};db.users[u.id]=u;return login(ws,u)}
   if(m.t==='login'){const id=String(m.id||'').trim().toLowerCase(),u=Object.values(db.users).find(x=>x.email===id||x.user.toLowerCase()===id);
    if(!u||hp(String(m.pass||''),u.salt)!==u.h)return err('Invalid login or password.');return login(ws,u)}
   if(m.t==='resume'){const id=db.sess[m.token];if(!id||!U(id))return send(ws,{t:'nosess'});return login(ws,U(id),m.token)}
   return}
  const u=U(ws.uid);if(!u)return;
  switch(m.t){
   case'friend':{const o=Object.values(db.users).find(x=>x.user.toLowerCase()===String(m.user||'').trim().toLowerCase());
    if(!o)return err('No user with that username.');if(o.id===u.id)return err('That’s you 😄');
    if(u.friends.includes(o.id))return err('You’re already friends.');if(u.out.includes(o.id))return err('Request already sent.');
    if(u.inn.includes(o.id))return accept(u,o);
    u.out.push(o.id);o.inn.push(u.id);save();notify(o.id,'friend_request',u.name+' sent you a friend request');send(ws,{t:'ok',m:'Friend request sent to @'+o.user});push(u.id);push(o.id);break}
   case'accept':{const o=U(m.id);if(o&&u.inn.includes(o.id))accept(u,o);break}
   case'decline':{const o=U(m.id);if(o){u.inn=u.inn.filter(x=>x!==o.id);o.out=o.out.filter(x=>x!==u.id);save();push(u.id);push(o.id)}break}
   case'mkserver':{const name=String(m.name||'').trim().slice(0,32);if(name.length<2)return err('Server name is too short.');
    const s={id:rid(4),name,owner:u.id,code:rid(4),members:[u.id],ch:[{id:'general',name:'general'}]};db.servers[s.id]=s;save();push(u.id);to(u.id,{t:'created',sid:s.id});break}
   case'join':{const s=Object.values(db.servers).find(x=>x.code===String(m.code||'').trim());if(!s)return err('That invite link is invalid.');
    if(!s.members.includes(u.id)){s.members.push(u.id);save();s.members.forEach(id=>id!==u.id&&notify(id,'server_join',u.name+' joined '+s.name))}
    s.members.forEach(id=>push(id));to(u.id,{t:'created',sid:s.id});break}
   case'hist':{if(part(m.room,u.id))send(ws,{t:'hist',room:m.room,msgs:(db.msgs[m.room]||[]).slice(-100)});break}
   case'msg':{const ps=part(m.room,u.id),text=String(m.text||'').trim().slice(0,1000),now=Date.now();if(!ps||!text||now-(ws.last||0)<200)return;ws.last=now;
    const msg={id:rid(4),from:u.id,name:u.name,text,ts:now},a=(db.msgs[m.room]=db.msgs[m.room]||[]);a.push(msg);if(a.length>300)a.shift();save();
    ps.forEach(id=>to(id,{t:'msg',room:m.room,m:msg}));
    if(m.room[0]==='d'){const o=ps.find(x=>x!==u.id);if(!isOn(o))notify(o,'dm',u.name+' sent you a message')}break}
   case'readn':{(db.notifs[u.id]||[]).forEach(n=>n.r=1);save();break}
   case'logout':{delete db.sess[m.token];save();break}}});
 ws.on('close',()=>{if(!ws.uid)return;const s=socks.get(ws.uid);if(s){s.delete(ws);if(!s.size)socks.delete(ws.uid)}pushAll()})};
srv.listen(PORT,()=>console.log('ElmForge is live → http://localhost:'+PORT));
