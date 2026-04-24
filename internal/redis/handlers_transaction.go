package redis

import "log"

func (r *Redis) handleMulti(connID uint64) ([]byte, error) {
	r.queue[connID] = []Command{}
	log.Printf("MULTI connID=%d", connID)
	return EncodeSimpleString("OK"), nil
}

func (r *Redis) handleExec(connID uint64) (Response, error) {
	cmds := r.queue[connID]
	delete(r.queue, connID)
	log.Printf("EXEC connID=%d, %d commands", connID, len(cmds))

	responses := make([][]byte, len(cmds))
	for i, cmd := range cmds {
		resp, err := r.dispatch(connID, cmd)
		if err != nil {
			return Response{}, err
		}
		responses[i] = resp.Data
	}
	return wrap(EncodeResponses(responses), nil)
}

func (r *Redis) handleDiscard(connID uint64) ([]byte, error) {
	_, isTx := r.queue[connID]
	if !isTx {
		return EncodeError("Err DISCARD without MULTI"), nil
	}
	delete(r.queue, connID)

	return EncodeSimpleString("OK"), nil
}
