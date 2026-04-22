package redis

import (
	"fmt"
	"strings"
)

type Command struct {
	Name string
	Args []string
}

// ParseCommand parses a RESPElement into a Command. The element must be either
// a RESPArray (standard inline command) or a RESPBulkString (bare command with no args).
func ParseCommand(el RESPElement) (Command, error) {
	switch v := el.(type) {
	case RESPArray:
		if len(v.Elements) == 0 {
			return Command{}, fmt.Errorf("empty command array")
		}
		name, ok := v.Elements[0].(RESPBulkString)
		if !ok {
			return Command{}, fmt.Errorf("command name must be a bulk string, got %T", v.Elements[0])
		}
		args := make([]string, 0, len(v.Elements)-1)
		for i, el := range v.Elements[1:] {
			arg, ok := el.(RESPBulkString)
			if !ok {
				return Command{}, fmt.Errorf("argument %d must be a bulk string, got %T", i, el)
			}
			args = append(args, arg.Value)
		}
		return Command{Name: strings.ToUpper(name.Value), Args: args}, nil
	case RESPBulkString:
		return Command{Name: strings.ToUpper(v.Value)}, nil
	default:
		return Command{}, fmt.Errorf("cannot parse command from %T", el)
	}
}
