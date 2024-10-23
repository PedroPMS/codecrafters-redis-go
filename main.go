package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("Listening on port :6379")

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	aof, err := NewAof("dataabse.aof")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer aof.Close()

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	aof.Read(func(value Value) {
		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]
		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			return
		}

		handler(args)
	})

	for {
		resp := NewResp(conn)

		val, err := resp.Read()
		if err != nil {
			fmt.Println("error reading from client: ", err.Error())
			return
		}
		if val.typ != "array" {
			fmt.Println("Invalid request, expected array")
			continue
		}
		if len(val.array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
		}

		command := strings.ToUpper(val.array[0].bulk)
		args := val.array[1:]

		writer := NewWriter(conn)
		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("Invalid command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		if command == "SET" || command == "HSET" {
			aof.Write(val)
		}

		writer.Write(handler(args))
	}
}
