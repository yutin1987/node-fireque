Fireque
=======
是一個用於處理大量request的解決方案, 透過fireque可以將大量的request在background分散至多個worker同時處理, 以達到提高處理效率的目標, 並且可以成功接受request, 但尚未處理, 將會在最短時間內進行處理的效果.（Ex. Http code 202）

通常用於**大量的圖片需要resize**, **大量的抓取資料**, **大量的發送訊息**, **排程資料分析**.

## 特色
- 可同時多個Worker/Producer
- 可依據protectKey限制同類型的Job, 最大的Worker數
- 支援三個(high/med/low)等級的優先權(priority), 和最高等級的插單
- 支援namespace設定

## Install [![NPM version][npm-image]][npm-url]
```
npm install fireque
```

> 安裝 [Redis](http://redis.io/) 請參考 [Redis安裝](http://redis.io/download)

> Package使用[node_redis](https://github.com/mranney/node_redis)作為操作redis的library

## Usage

```
var Fireque = require('fireque');

var worker = new Fireque.Worker('addition');
worker.onPerform( function (job, callback) {
	job.data.ans = job.data.x + job.data.y;
	callback(false);
});

var job = new Fireque.Job('addition', {x: 1, y: 1});
job.enqueue();

producer = new Fireque.Producer('addition');

producer.onCompleted( function (jobs, callback) {
	var x = jobs[0].data.x;
	var y = jobs[0].data.y;
	var ans = jobs[0].data.ans;	
   	console.log(x + '+' + y + '=' + ans);
   	callback();
});
```

## Global Config

- FIREQUE_HOST

`Fireque.FIREQUE_HOST = '127.0.0.1'`

> Redis的address

- FIREQUE_PORT

`Fireque.FIREQUE_PORT = '6379'`

> Redis的port

- FIREQUE_NAMESPACE

`Fireque.FIREQUE_NAMESPACE = 'noame'`

> Fireque存放在Redis的namespace


Object
=======

## Job

`new Job(protocol, data, option)`
> 建立一個新的Job, 物件初始化並不會將Job放入Queue

`new Job(uuid, callback)`
> 從database取出已經存在的Job, 必須要有UUID

### 將Job放入Queue
`enqueue('protectKey', "high|med|low", callback())`
> 將Job放入Queue, 並指定protectKey和優先權

`enqueue('protectKey', callback())`
> 將Job放入Queue, 並指定protectKey而優先權使用預設med

`enqueue("high|med|low", callback())` 將job放住佇列
> 將Job放入Queue, 並指定優先權而protectKey會使用預設unrestricted.

`enqueue(true, callback())` 將job放住佇列
> 將Job強制放入Queue的最先順位, 將會立即處理此job, 不會指定protectKey和優先權. !!!請謹慎使用

`enqueue(callback())` 將job放住佇列
> 將Job放入Queue, 優先權(預設med)和protectKey(預設unrestricted)都使用預設

`dequeue(callback())`
> 將此Job從Queue中移除, 當job正在處理中(processing)會無法移除

`requeue( ... )`
> 將Job重新放入Queue, 參數請參考enqueue

`toCompleted()`
> 將此Job移至完成清單

`toFailed()`
> 將此Job移至失敗清單

## Work

`new Wrok(protocol, option)`
> 建立一個新的Worker, 物件初始化並不會立即接受Job

`onPerform(function(job, callback))`
> 設定處理Job的handler, 並開始接受委派的job

> callback(null); 完成, 但不處理

> callback(true); 完成, 並呼叫toCompleted()

> callback(false); 失敗, 並呼叫toFailed()

`offPerform(callback)`
> 移除處理Job的handler, 並停止接受委派的job, 正在處理中Job的handler會直至處理完為止.

## Producer

`onCompleted(function([job, ...]))`
> 取得已處理完的Job, 並從清單中移除

`onFailed(function([job, ...]))`
> 取得已失敗的Job, 並從清單中移除

`onTimeout(function([job, ...]))` 當job發生timeout
> 取得已超時的Job, 但不清楚處理中(processing)的清單

## Monitor

`new Monitor(protocol, workload, option)`

[npm-url]: https://npmjs.org/package/fireque
[npm-image]: https://badge.fury.io/js/fireque.png