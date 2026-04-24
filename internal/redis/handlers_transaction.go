package redis

import "log"

func (r *Redis) handleMulti(connID uint64) ([]byte, error) {
	tx, isTx := r.transactions[connID]

	if !isTx {
		r.transactions[connID] = transaction{connID: connID, commands: []Command{}, watchedKeys: []string{}, multiCalled: true}
		return EncodeSimpleString("OK"), nil
	}

	if tx.multiCalled {
		return EncodeError("ERR MULTI calls can not be nested"), nil
	}

	tx.multiCalled = true
	r.transactions[connID] = tx

	log.Printf("MULTI connID=%d", connID)
	return EncodeSimpleString("OK"), nil
}

func (r *Redis) handleExec(connID uint64) (Response, error) {
	cmds := r.transactions[connID].commands
	delete(r.transactions, connID)
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
	_, isTx := r.transactions[connID]
	if !isTx {
		return EncodeError("ERR DISCARD without MULTI"), nil
	}
	delete(r.transactions, connID)

	return EncodeSimpleString("OK"), nil
}

func (r *Redis) handleWatch(connID uint64, args []string) ([]byte, error) {
	tx, isTx := r.transactions[connID]

	if !isTx {
		tx.commands = []Command{}
		tx.connID = connID
		tx.multiCalled = false
		tx.watchedKeys = args
		r.transactions[connID] = tx
		return EncodeSimpleString("OK"), nil
	}

	if tx.multiCalled {
		return EncodeError("ERR WATCH inside MULTI is not allowed"), nil
	}

	tx.watchedKeys = append(tx.watchedKeys, args...)
	r.transactions[connID] = tx
	return EncodeSimpleString("OK"), nil
}
