package main

import "sync"

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HSET":    hset,
	"HGET":    hget,
	"HGETALL": hgetall,
}

var SETs = map[string]string{}
var SETsMutex = sync.RWMutex{}

var HSETs = map[string]map[string]string{}
var HSETsMutex = sync.RWMutex{}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}

	return Value{typ: "string", str: args[0].bulk}
}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments"}
	}
	key := args[0].bulk
	value := args[1].bulk

	SETsMutex.Lock()
	SETs[key] = value
	SETsMutex.Unlock()

	return Value{typ: "string", str: "OK"}
}

func get(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments"}
	}

	key := args[0].bulk

	SETsMutex.RLock()
	value, ok := SETs[key]
	SETsMutex.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments"}
	}
	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMutex.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMutex.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	SETsMutex.RLock()
	value, ok := HSETs[hash][key]
	SETsMutex.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments"}
	}

	hash := args[0].bulk

	HSETsMutex.RLock()
	value, ok := HSETs[hash]
	HSETsMutex.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	values := []Value{}
	for k, v := range value {
		values = append(values, Value{typ: "bulk", bulk: k})
		values = append(values, Value{typ: "bulk", bulk: v})
	}

	return Value{typ: "array", array: values}
}
