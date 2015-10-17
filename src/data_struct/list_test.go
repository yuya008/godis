package godis

import (
	"fmt"
	"testing"
)

func TestList(t *testing.T) {
	list := NewList()
	t.Logf("list len is %d", list.Len())
	for i := 0; i < 6; i++ {
		obj := CreateStringObject([]byte(fmt.Sprintf("%d", i)))
		list.Rput(obj)
	}
	t.Logf("list len is %d", list.Len())
	t.Log("Rput() end!")
	for {
		obj := list.Rpop()
		if obj == nil {
			break
		}
		t.Log(string(obj.GetBuffer()))
	}
	t.Log("Rpop() end!")
	t.Logf("list len is %d", list.Len())
	for i := 0; i < 10; i++ {
		obj := CreateStringObject([]byte(fmt.Sprintf("%d", i)))
		list.Lput(obj)
	}
	obj := list.Get(2)
	t.Log(string(obj.GetBuffer()))
	t.Log("Get(2) end!")

	list.Remove(CreateStringObject([]byte(fmt.Sprintf("%d", 4))))
	list.Remove(CreateStringObject([]byte(fmt.Sprintf("%d", 9))))
	list.Remove(CreateStringObject([]byte(fmt.Sprintf("%d", 0))))
	t.Log("Remove(4) Remove(9) Remove(0) end!")

	i1 := list.Index(CreateStringObject([]byte(fmt.Sprintf("%d", 2))))
	i2 := list.Index(CreateStringObject([]byte(fmt.Sprintf("%d", 7))))
	i3 := list.Index(CreateStringObject([]byte(fmt.Sprintf("%d", 0))))
	t.Logf("Index(2) %d", i1)
	t.Logf("Index(7) %d", i2)
	t.Logf("Index(0) %d", i3)
	t.Log("Index() end!")

	list.Insert(After, 0, CreateStringObject([]byte(fmt.Sprintf("%d", 200))))
	list.Insert(After, 6, CreateStringObject([]byte(fmt.Sprintf("%d", 300))))
	list.Insert(After, 3, CreateStringObject([]byte(fmt.Sprintf("%d", 400))))

	list.Insert(Before, 0, CreateStringObject([]byte(fmt.Sprintf("%d", 500))))
	list.Insert(Before, 6, CreateStringObject([]byte(fmt.Sprintf("%d", 600))))
	list.Insert(Before, 3, CreateStringObject([]byte(fmt.Sprintf("%d", 700))))
	// list.Clear()
	t.Log("Insert() end!")
	for {
		obj := list.Lpop()
		if obj == nil {
			break
		}
		t.Log(string(obj.GetBuffer()))
	}

	t.Logf("list len is %d", list.Len())
	t.Log("Lpop() end!")
}
