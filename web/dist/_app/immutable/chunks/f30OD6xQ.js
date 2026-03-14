const r=[...` 	
\r\f \v\uFEFF`];function o(l,u,f){var n=""+l;if(f){for(var t of Object.keys(f))if(f[t])n=n?n+" "+t:t;else if(n.length)for(var i=t.length,s=0;(s=n.indexOf(t,s))>=0;){var e=s+i;(s===0||r.includes(n[s-1]))&&(e===n.length||r.includes(n[e]))?n=(s===0?"":n.substring(0,s))+n.substring(e+1):s=e}}return n===""?null:n}function c(l,u){return l==null?null:String(l)}export{c as a,o as t};
