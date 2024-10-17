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

	conn, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

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

		writer.Write(handler(args))
	}
}
