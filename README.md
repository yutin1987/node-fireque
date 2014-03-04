Fireque
=======
是一個用於處理大量request的解決方案, 透過fireque可以將大量的request在background分散至多個worker同時處理, 以達到提高處理效率的目標, 並且可以成功接受request, 但尚未處理, 將會在最短時間內進行處理的效果.（Ex. Http code 202）

通常用於**大量的圖片需要resize**, **大量的抓取資料**, **大量的發送訊息**, **排程資料分析**.

## 特色
- 可同時多個Worker/Producer
- 可依據protectKey限制同類型的Job, 最大的Worker數
- 支援三個(high/med/low)等級的優先權(priority), 和最高等級的插單
- 可指定schedule, 並依指定的時間執行.
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

## Example

[Protect](https://github.com/yutin1987/node-fireque/blob/master/example/protect.js) ProtectKey保護, 同時間最多3個worker

[Schedule](https://github.com/yutin1987/node-fireque/blob/master/example/schedule.js) Schedule設定, 每5秒執行10個job

## Global Config

`Fireque.host = '127.0.0.1'`

> Redis的address

`Fireque.port = '6379'`

> Redis的port

`Fireque.databaseIndex = 0`

> 指定Redis的資料庫編號

`Fireque.namespace = 'noame'`

> Fireque存放在Redis的namespace


Object
=======

## Job

`new Job(protocol, data, option)`
> 建立一個新的Job, 物件初始化並不會將Job放入Queue

`new Job(uuid, callback, option)`
> 從database取出已經存在的Job, 必須要有UUID

### 將Job放入Queue

`enqueue(callback())`
> 將Job放入Queue, 優先權(預設med)和protectKey(預設unrestricted)都使用預設

**優先權(Priority)**

`enqueue("high|med|low", callback())`
> 將Job放入Queue, 並指定優先權

`enqueue("1|0|-1", callback())`
> 將Job放入Queue, 並指定優先權

`enqueue("high|med|low", {protectKey: 'key'}, callback())`
> 將Job放入Queue, 並指定優先權和protectKey保護

`enqueue("1|0|-1", {protectKey: 'key'}, callback())`
> 將Job放入Queue, 並指定優先權和protectKey保護

**排程(Schedule)**

`enqueueAt(new Date(), callback())`
> 將Job放入Queue, 並指定schedule的日期時間

`enqueueAt(Number, callback())`
> 將Job放入Queue, 並指定幾秒後執行

`enqueueAt(new Date(), {protectKey: 'key'}, callback())`
> 將Job放入Queue, 並指定schedule的日期時間和protectKey保護

`enqueueAt(Number, {protectKey: 'key'}, callback())`
> 將Job放入Queue, 並指定幾秒後執行和protectKey保護

**最高等級插單(Top)**

`enqueueTop(callback())`
> 將Job放入Queue, 當有Worker閒置時, 立即執行

### 將Job從Queue移除

`dequeue(callback())`
> 將此Job從Queue中移除, 當job正在處理中(processing)會無法移除

### 將Job重新放入Queue, 原本在Queue的Job將會移除

`requeue( ... )`
> 將Job重新放入Queue, 參數請參考enqueue

`toCompleted()`
> 將此Job移至完成清單

`toFailed()`
> 將此Job移至失敗清單

## Work

`new Wrok(protocol, option)`
> 建立一個新的Worker, 物件初始化並不會立即接受Job

```
option = {
  workload: 100,
  workinghour: 30 * 60,
  timeout: 60,
  priority: ['high','high','high','med','med','low'],
}
```
> workload - Work的最大工作量, 預設是100個job

> workinghour - Work的最長的工作時間, 預設是30min

> timeout - 當worker多久未回應代表已超時, 必須發出警告, 預設是1min

> priority - 執行優先權的順序, 預設每次執行3個high, 2個med 和 1個low. 當Keeper有啟動時, 此設定等同無效

`onPerform(function(job, callback))`
> 設定處理Job的handler, 並開始接受委派的job

> callback(null); 完成, 但不處理

> callback(false); 完成, 並呼叫toFailed()

> callback(true); 失敗, 並呼叫toCompleted()

> 執行過程中發生throw, 會自動送至toFailed()

`offPerform(callback())`
> 移除處理Job的handler, 並停止接受委派的job, 正在處理中Job的handler會直至處理完為止.

`onWorkOut(callback())`
> 當Worker已超過工作量or超過工作時間, Worker將停止運作, 並拋出workout handler.

## Producer

`new Producer(protocol, option)`
> 建立一個新的Producer, 用於取得已完成or已失敗的Job, 並對timeout的job發出警告

```
option = {
  max_wait: 30
  max_count: 10
}
```
> max_wait - 等待最久幾秒呼叫一次handler, 預設是30sec

> max_count - 當到達多少數量時立即呼叫一次handler, 預設是10筆job

`onCompleted(function([job, ...], callback()), option)`
> 取得已處理完的Job, 並從清單中移除

> 注意！必須要呼叫callback(), 才會繼續取得已完成的job.

`onFailed(function([job, ...], callback()), option)`
> 取得已失敗的Job, 並從清單中移除

> 注意！必須要呼叫callback(), 才會繼續取得已失敗的job.

`onTimeout(function([job, ...]), option)`
> 取得已超時的Job, 但不清除處理中(processing)的清單

> 注意！必須要呼叫callback(), 才會繼續檢查已超時的job.

## Keeper

`new Keeper(protocol, workload, option)`
> 建立一個新的Keeper, 當Queue需要支援protectKey或Schedule時, 就必須執行至少1個Keeper

> 亦可透過cli模式, 執行 fireque keeper

```
option = {
  workload: 5
  priority: ['high','high','high','med','med','low']
}
```
> workload - 每個protectKey最大的worker數量, 預設是5個worker

> priority - 執行優先權的順序, 預設每次執行3個high, 2個med 和 1個low.

`start(function(err, reply), interval)`
> 開始執行Keeper

`stop()`
> 停止執行Keeper

[npm-url]: https://npmjs.org/package/fireque
[npm-image]: https://badge.fury.io/js/fireque.png