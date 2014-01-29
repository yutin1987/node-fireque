fireque
=======
佇列

`fireque:{namespace}:{protocol}:queue:high = [LIST]`

`fireque:{namespace}:{protocol}:queue:med = [LIST]`

`fireque:{namespace}:{protocol}:queue:low = [LIST]`

`fireque:{namespace}:{protocol}:processing = [LIST]`

`fireque:{namespace}:{protocol}:completed = [LIST]`

`fireque:{namespace}:{protocol}:failed = [LIST]`

Data

```
fireque:{namespace}:job:{uuid} = HASH
	data: "string"
	timeout: 30		// sec
	work: "work_name"		
````

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
  timeout: 30
}
```

`enqueue("heigh|med|low")` 將job放住佇列

`dequeue()` 刪除指定的job

`requeue` 重新將job放回佇列（最後端）

## Work

`perform` 接收並執行委派的job

## Producer

`onCompleted(fun([job]), max_count)` 當job執行完成

`onFailed(fun(job))` 當job發生錯誤

`onTimeout(fun(job))` 當job發生timeout


