fireque data schema
=======


data 存放於 [redis](redis.io), 使用到redis的Keys, Hashes, Lists, 詳細說明請參考如下：

## 名詞解釋

`namespace`
> 命名空間, 在同一台server裡可以透過namespace做專案切割, 當然也可以透過設定不同的database來做切割

`protocol`
> 協定, 通常不同的protocol會交給我不同的Worker處理, 每個Worker只負責一種protocol

`protectKey`
> Worker 保護鍵, 避免同類型的Job同時間太多Worker在處理

`uuid`
> Job 的唯一碼, 請參考 [RFC4122](http://www.ietf.org/rfc/rfc4122.txt) A Universally Unique IDentifier (UUID) URN Namespace

## Base
`fireque:{namespace}:{protocol}:queue = [LIST]`
> 存放待處理Job的UUID, 左放右取採先進先出的方式排列

> 由右放入Job的UUID, 將會產生立即插單處理的效果

`fireque:{namespace}:{protocol}:processing = [LIST]`
> 存放正在處理中Job的UUID, 由左放入 

`fireque:{namespace}:{protocol}:completed = [LIST]`
> 存放已完成Job的UUID,  左放右取採先進先出的方式排列

`fireque:{namespace}:{protocol}:failed = [LIST]`
> 存放已失敗Job的UUID,  左放右取採先進先出的方式排列. 此處所指的失敗是不可預知的失敗, 例如throw, 通常需要人工處理

```
fireque:{namespace}:job:{uuid} = HASH
	data: "JSON string"
	protocol: "string"
	protectKey: "string"
	priority: "high" or "med" or "low"
	worker: "work_name"
```
> 使用hash的方式存放Job的詳細資料, 方便用於一次取出全部資料. worker 只在開始處理時才寫入

> Job的詳細資料只存放 3天, 逾時將自動清除

`fireque:{namespace}:job:{uuid}:timeout = 1`
> 存放處理該Job所需的最久時間, 當此key不存在時, 代表Job的處理已超時 或 Worker發生了不可預期的錯誤

## Buffer

放入Queue的Job將會排入立即處理, 為了避免同類型的Job同時間造成過多的Worker都在處理, 因此依protectKey先存放於buffer, 透過monitor去管理每個protectKey的工作量(預設是 max 5 worker), 當有剩餘額度時才將Job的UUID從buffer移至Queue, 並且會依照優先權先行搬移較高優先權的Job.

`fireque:{namespace}:{protocol}:buffer:{protectKey}:high = [LIST]`
> 高優先權Job的UUID, 左放右取採先進先出的方式排列

`fireque:{namespace}:{protocol}:buffer:{protectKey}:med = [LIST]`
> 一般優先權Job的UUID, 左放右取採先進先出的方式排列

`fireque:{namespace}:{protocol}:buffer:{protectKey}:low = [LIST]`
> 低優先權Job的UUID, 左放右取採先進先出的方式排列

`fireque:{namespace}:{protocol}:workload:{protectKey} = INT`
> 存放目前相同protectKey的Job在Queue和Processing的數量

## Schedule

`fireque:{namespace}:{protocol}:schedule:{timestamp} = LIST`
> 存放已排程Job的UUID, 由左放入


