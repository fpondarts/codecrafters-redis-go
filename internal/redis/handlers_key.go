package redis

func (r *Redis) handleType(args []string) (Response, error) {
	if len(args) < 1 {
		return Response{Data: EncodeError("Err too few arguments for TYPE command")}, nil
	}

	typeStr := r.storage.Type(args[0])

	if typeStr == "" {
		return Response{Data: EncodeSimpleString("none")}, nil
	}

	return Response{Data: EncodeSimpleString(typeStr)}, nil
}
