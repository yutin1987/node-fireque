fireque
=======
佇列

`fireque:{namespace}:{protocol}:queue = [LIST]`

`fireque:{namespace}:{protocol}:processing = [LIST]`

`fireque:{namespace}:{protocol}:completed = [LIST]`

`fireque:{namespace}:{protocol}:failed = [LIST]`


`fireque:{namespace}:{protocol}:buffer:{collapse}:high = [LIST]`

`fireque:{namespace}:{protocol}:buffer:{collapse}:med = [LIST]`

`fireque:{namespace}:{protocol}:buffer:{collapse}:low = [LIST]`

Data

```
fireque:{namespace}:{protocol}:workload:{collapse} = INT
```

```
fireque:{namespace}:job:{uuid} = HASH
	data: "string"
	protocol: "xyz"
	priority: "high", "med", "low"
	work: "work_name"
````
EXPIRE: 3 * 24 * 60 * 60

Config
=======

- FIREQUE_HOST
- FIREQUE_PORT
- FIREQUE_NAMESPACE



Object
=======


## Job

`new Job(protocol, data, option)`

```
option = {
	port: 6379
	host: 127.0.0.1
}
```

`enqueue(callback(), "high|med|low")` 將job放住佇列

`dequeue(callback())` 刪除指定的job

`requeue(callback() "high|med|low")` 重新將job放回佇列（最後端）

`completed()`

`failed()`

## Work

`new Wrok(protocol, option)`

```
option = {
	wait: 10
	workload: 100
	port: 6379
	host: 127.0.0.1
}
```

`exit()` 離開工作

`onExit()`

`perform(action(job, done()))` 接收並執行委派的job

如果done(report)
null: 完成, 但不處理
true: 完成, 並呼叫completed()
false: 失敗, 並呼叫failed()

## Producer

`onCompleted(fun([job]), max_count)` 當job執行完成

`onFailed(fun(job))` 當job發生錯誤

`onTimeout(fun(job))` 當job發生timeout

## Monitor
```
option = {
	workload = 5,
	priority: ["high", "high", "high", "med", "med", "low"]
}
```

