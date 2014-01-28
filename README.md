fireque
=======
佇列

`fireque_{namespace}_queue_{protocol}_heigh = [LIST]`

`fireque_{namespace}_queue_{protocol}_med = [LIST]`

`fireque_{namespace}_queue_{protocol}_low = [LIST]`

`fireque_{namespace}_processing_{protocol} = [LIST]`

`fireque_{namespace}_completed_{protocol} = [LIST]`

`fireque_{namespace}_failed_{protocol} = [LIST]`

Data

```
fireque_job_{uuid} = HASH
	data: "string"
	timeout: 30		// sec
	work: "work_name"
````

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


