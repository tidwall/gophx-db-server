package main

import (
	"runtime"
	"strings"

	"github.com/tidwall/redcon"
)

var (
	txs  = map[redcon.Conn][][]string{}
	keys = map[string]string{}
	cmds = map[string]cmd{}
)

type cmd struct {
	nargs, sys int
	fn         func(conn redcon.Conn, args []string)
}

func main() {
	runtime.GOMAXPROCS(1)
	println("Starting server on port 8888")
	cmds = map[string]cmd{
		"get":    cmd{2, 0, cmdGET},
		"set":    cmd{3, 0, cmdSET},
		"del":    cmd{2, 0, cmdDEL},
		"quit":   cmd{1, 1, cmdQUIT},
		"begin":  cmd{1, 1, cmdBEGIN},
		"commit": cmd{1, 1, cmdCOMMIT},
	}
	println(redcon.ListenAndServe(":8888",
		func(conn redcon.Conn, cmd redcon.Command) {
			var args []string
			for _, arg := range cmd.Args {
				args = append(args, string(arg))
			}
			exec(conn, args)
		}, nil, func(c redcon.Conn, err error) { delete(txs, c) }).Error())

}
func exec(conn redcon.Conn, args []string) {
	cmdv := cmds[strings.ToLower(args[0])]
	if cmdv.sys == 0 && txs[conn] != nil {
		txs[conn] = append(txs[conn], args)
		conn.WriteString("QUEUED")
	} else if cmdv.fn == nil {
		conn.WriteError("ERR unknown command '" + args[0] + "'")
	} else if len(args) != cmdv.nargs {
		conn.WriteError("ERR invalid number of arguments")
	} else {
		cmdv.fn(conn, args)
	}
}
func cmdGET(conn redcon.Conn, args []string) {
	if val, ok := keys[args[1]]; ok {
		conn.WriteBulkString(val)
	} else {
		conn.WriteNull()
	}
}
func cmdSET(conn redcon.Conn, args []string) {
	keys[args[1]] = args[2]
	conn.WriteString("OK")
}
func cmdDEL(conn redcon.Conn, args []string) {
	delete(keys, args[1])
	conn.WriteString("OK")
}
func cmdQUIT(conn redcon.Conn, args []string) {
	conn.WriteString("OK")
	conn.Close()
}
func cmdBEGIN(conn redcon.Conn, args []string) {
	if txs[conn] != nil {
		conn.WriteError("ERR transaction in progress")
	} else {
		txs[conn] = [][]string{}
		conn.WriteString("OK")
	}
}
func cmdCOMMIT(conn redcon.Conn, args []string) {
	if txs[conn] == nil {
		conn.WriteError("ERR transaction not in progress")
	} else {
		tx := txs[conn]
		txs[conn] = nil
		conn.WriteArray(len(tx))
		for _, args := range tx {
			exec(conn, args)
		}
	}
}
