package core

// apt-get install libzmq3-dev

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/pebbe/zmq4"
)

type Data struct {
	// Assume your types for the following fields
	BlockWithHashOnly map[string]interface{}
	BlockWithFullTx   map[string]interface{}
	Receipts          types.Receipts
	Current           *big.Int
	Final             *big.Int
	Safe              *big.Int
	Codes             map[common.Address][]byte
	Balances          map[common.Address]string
	Traces            map[string]json.RawMessage
}

type ZmqSender struct {
	socket *zmq4.Socket
}

func NewZmqSender(endpoint string) *ZmqSender {
	sender := &ZmqSender{}
	for {
		socket, err := zmq4.NewSocket(zmq4.PUSH)
		if err != nil {
			fmt.Println("Failed to create ZMQ socket:", err)
			continue
		}
		err = socket.Connect(endpoint)
		if err != nil {
			fmt.Println("Failed to connect to ZMQ server:", err)
			socket.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		sender.socket = socket
		break
	}
	return sender
}

func (s *ZmqSender) Send(data []byte) error {
	for {
		_, err := s.socket.SendBytes(data, 0)
		if err != nil {
			fmt.Println("Failed to send data:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		return nil
	}
}

func (bc *BlockChain) QNCache(head *types.Block) {
	start := time.Now()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup

	send := func(reference string, f func() ([]byte, error)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := f()
			if err != nil {
				fmt.Printf("Failed to execute function %s: %v\n", reference, err)
				cancel()
				return
			}

			// nothing to do
			if len(v) == 0 {
				return
			}

			hexWithPrefix := hexutil.EncodeBig(head.Number())
			hexWithoutPrefix := strings.TrimPrefix(hexWithPrefix, "0x")

			prefix := hexWithoutPrefix + "_" + reference + "_"
			v = append([]byte(prefix), v...)

			err = bc.zmqSender.Send(v)
			if err != nil {
				fmt.Println("failed to send data to ZMQ:", err)
			}
		}()
	}

	send("current", func() ([]byte, error) {
		current := bc.CurrentBlock().Number
		data, err := json.MarshalIndent(current, "", "  ")
		return data, err
	})

	send("final", func() ([]byte, error) {
		final := bc.CurrentFinalBlock().Number
		data, err := json.MarshalIndent(final, "", "  ")
		return data, err
	})

	send("safe", func() ([]byte, error) {
		safe := bc.CurrentSafeBlock().Number
		data, err := json.MarshalIndent(safe, "", "  ")
		return data, err
	})

	send("blockByNumberHashOnly", func() ([]byte, error) {
		block := bc.getBlockByNumber(head, true, false)
		data, err := json.MarshalIndent(block, "", "  ")
		return data, err
	})

	send("blockByNumberFullTx", func() ([]byte, error) {
		block := bc.getBlockByNumber(head, true, true)
		data, err := json.MarshalIndent(block, "", "  ")
		return data, err
	})

	send("receipts", func() ([]byte, error) {
		receipts := rawdb.ReadRawReceipts(bc.db, head.Hash(), head.NumberU64())
		err := receipts.DeriveFields(bc.chainConfig, head.Hash(), head.NumberU64(), head.Time(), head.BaseFee(), head.Transactions())
		if err != nil {
			return nil, err
		}
		data, err := json.MarshalIndent(receipts, "", "  ")
		return data, err
	})

	send("balances", func() ([]byte, error) {
		balances := bc.getBalances(head)
		data, err := json.MarshalIndent(balances, "", "  ")
		return data, err
	})

	send("codes", func() ([]byte, error) {
		codes := bc.getCodes(head)
		data, err := json.MarshalIndent(codes, "", "  ")
		return data, err
	})

	send("traces", func() ([]byte, error) {
		traces, err := TracerBlockByNumber(head.NumberU64())
		if err != nil {
			return nil, err
		}
		data, err := json.MarshalIndent(traces, "", "  ")
		return data, err
	})

	// This goroutine will cancel the context after all the other goroutines have finished.
	go func() {
		wg.Wait()
		cancel()
	}()

	// Wait until either all function calls have completed, or there's an error.
	<-ctx.Done()

	elapsed := time.Since(start)
	fmt.Println("cache function took", elapsed)
}

func TracerBlockByNumber(blockNumber uint64) (map[string]json.RawMessage, error) {
	timeoutDuration, _ := time.ParseDuration("300s")
	ctx, cancel := context.WithTimeout(context.Background(), timeoutDuration)
	defer cancel()

	rawClient, err := rpc.DialContext(ctx, "http://127.0.0.1:8545")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Ethereum node: %v", err)
	}

	blockNum := hexutil.EncodeUint64(blockNumber)
	var responseWith, responseWithout json.RawMessage
	var errWith, errWithout error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		errWith = rawClient.CallContext(ctx, &responseWith, "debug_traceBlockByNumber", blockNum, map[string]interface{}{
			"tracer": "callTracer",
			"tracerConfig": map[string]interface{}{
				"withLog":     true,
				"onlyTopCall": false,
			},
		})
	}()

	go func() {
		defer wg.Done()
		errWithout = rawClient.CallContext(ctx, &responseWithout, "debug_traceBlockByNumber", blockNum, map[string]interface{}{
			"tracer": "callTracer",
			"tracerConfig": map[string]interface{}{
				"withLog":     false,
				"onlyTopCall": false,
			},
		})
	}()

	wg.Wait()

	if errWith != nil {
		return nil, fmt.Errorf("failed to trace block by number with logs: %v", errWith)
	}

	if errWithout != nil {
		return nil, fmt.Errorf("failed to trace block by number without logs: %v", errWithout)
	}

	result := make(map[string]json.RawMessage)
	result["with"] = responseWith
	result["without"] = responseWithout

	return result, nil
}

func (bc *BlockChain) getBalances(head *types.Block) map[common.Address]string {
	txs := head.Transactions()

	// Initialize the results map
	results := make(map[common.Address]string)

	// Create a channel to collect results from goroutines
	resultsCh := make(chan struct {
		address common.Address
		balance string
	})

	// Create a WaitGroup to ensure all goroutines finish
	var wg sync.WaitGroup

	// Semaphore to limit concurrent requests
	sem := make(chan struct{}, 20) // Adjust this number based on your client's capabilities

	for _, tx := range txs {
		wg.Add(1)
		go func(tx *types.Transaction) {
			defer wg.Done()

			sem <- struct{}{}        // Acquire a token
			defer func() { <-sem }() // Release the token when finished

			signer := types.LatestSignerForChainID(bc.chainConfig.ChainID)
			from, err := types.Sender(signer, tx)
			if err != nil {
				fmt.Println("Failed to get from address:", err)
				return
			}

			// If the transaction has a 'to' address
			if tx.To() != nil {
				to := tx.To()

				// Get the balance for 'from' and 'to' addresses
				fromBalance, toBalance := getBalance(from), getBalance(*to)

				// Send the 'from' address and its balance to the results channel
				resultsCh <- struct {
					address common.Address
					balance string
				}{from, fromBalance}

				// Send the 'to' address and its balance to the results channel
				resultsCh <- struct {
					address common.Address
					balance string
				}{*to, toBalance}
			} else {
				// This is a contract creation transaction,
				// so we only get the balance for the 'from' address
				fromBalance := getBalance(from)

				// Send the 'from' address and its balance to the results channel
				resultsCh <- struct {
					address common.Address
					balance string
				}{from, fromBalance}
			}
		}(tx)
	}

	// Start a goroutine to close the results channel after all other goroutines finish
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results from the channel
	for result := range resultsCh {
		results[result.address] = result.balance
	}

	return results
}

func getBalance(address common.Address) string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rawClient, err := rpc.DialContext(ctx, "http://127.0.0.1:8545")
	if err != nil {
		fmt.Println("Failed to connect to the Ethereum client: ", err)
		return ""
	}

	var hexBalance string
	err = rawClient.CallContext(ctx, &hexBalance, "eth_getBalance", address, "latest")
	if err != nil {
		fmt.Println("Failed to get balance for account", address.Hex(), err)
		return ""
	}
	return hexBalance
}

func (bc *BlockChain) getCodes(head *types.Block) map[common.Address][]byte {
	txs := head.Transactions()

	// Initialize the results map
	results := make(map[common.Address][]byte)

	// Create a channel to collect results from goroutines
	resultsCh := make(chan struct {
		addr  common.Address
		input []byte
	})

	// Create a WaitGroup to ensure all goroutines finish
	var wg sync.WaitGroup
	wg.Add(len(txs))

	for _, tx := range txs {
		// Start a goroutine for each transaction
		go func(tx *types.Transaction) {
			defer wg.Done()

			// Check if the transaction is a contract creation
			if tx.To() == nil {
				signer := types.NewEIP155Signer(bc.chainConfig.ChainID)
				from, _ := types.Sender(signer, tx)
				contractAddress := crypto.CreateAddress(from, tx.Nonce())
				// If it is, send the contract address and input data to the results channel
				resultsCh <- struct {
					addr  common.Address
					input []byte
				}{contractAddress, tx.Data()}
			}
		}(tx)
	}

	// Start a goroutine to close the results channel after all other goroutines finish
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect results from the channel
	for result := range resultsCh {
		results[result.addr] = result.input
	}

	return results
}

// getBlockByNumber
func (bc *BlockChain) getBlockByNumber(block *types.Block, inclTx bool, fullTx bool) map[string]interface{} {
	fields := RPCMarshalBlock(block, inclTx, fullTx, bc.chainConfig)
	if inclTx {
		fields["totalDifficulty"] = (*hexutil.Big)(bc.GetTd(block.Hash(), block.NumberU64()))
	}
	return fields
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash        *common.Hash      `json:"blockHash"`
	BlockNumber      *hexutil.Big      `json:"blockNumber"`
	From             common.Address    `json:"from"`
	Gas              hexutil.Uint64    `json:"gas"`
	GasPrice         *hexutil.Big      `json:"gasPrice"`
	GasFeeCap        *hexutil.Big      `json:"maxFeePerGas,omitempty"`
	GasTipCap        *hexutil.Big      `json:"maxPriorityFeePerGas,omitempty"`
	Hash             common.Hash       `json:"hash"`
	Input            hexutil.Bytes     `json:"input"`
	Nonce            hexutil.Uint64    `json:"nonce"`
	To               *common.Address   `json:"to"`
	TransactionIndex *hexutil.Uint64   `json:"transactionIndex"`
	Value            *hexutil.Big      `json:"value"`
	Type             hexutil.Uint64    `json:"type"`
	Accesses         *types.AccessList `json:"accessList,omitempty"`
	ChainID          *hexutil.Big      `json:"chainId,omitempty"`
	V                *hexutil.Big      `json:"v"`
	R                *hexutil.Big      `json:"r"`
	S                *hexutil.Big      `json:"s"`
}

// newRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCTransaction(tx *types.Transaction, blockHash common.Hash, blockNumber uint64, blockTime uint64, index uint64, baseFee *big.Int, config *params.ChainConfig) *RPCTransaction {
	signer := types.MakeSigner(config, new(big.Int).SetUint64(blockNumber), blockTime)
	from, _ := types.Sender(signer, tx)
	v, r, s := tx.RawSignatureValues()
	result := &RPCTransaction{
		Type:     hexutil.Uint64(tx.Type()),
		From:     from,
		Gas:      hexutil.Uint64(tx.Gas()),
		GasPrice: (*hexutil.Big)(tx.GasPrice()),
		Hash:     tx.Hash(),
		Input:    hexutil.Bytes(tx.Data()),
		Nonce:    hexutil.Uint64(tx.Nonce()),
		To:       tx.To(),
		Value:    (*hexutil.Big)(tx.Value()),
		V:        (*hexutil.Big)(v),
		R:        (*hexutil.Big)(r),
		S:        (*hexutil.Big)(s),
	}
	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	switch tx.Type() {
	case types.LegacyTxType:
		// if a legacy transaction has an EIP-155 chain id, include it explicitly
		if id := tx.ChainId(); id.Sign() != 0 {
			result.ChainID = (*hexutil.Big)(id)
		}
	case types.AccessListTxType:
		al := tx.AccessList()
		result.Accesses = &al
		result.ChainID = (*hexutil.Big)(tx.ChainId())
	case types.DynamicFeeTxType:
		al := tx.AccessList()
		result.Accesses = &al
		result.ChainID = (*hexutil.Big)(tx.ChainId())
		result.GasFeeCap = (*hexutil.Big)(tx.GasFeeCap())
		result.GasTipCap = (*hexutil.Big)(tx.GasTipCap())
		// if the transaction has been mined, compute the effective gas price
		if baseFee != nil && blockHash != (common.Hash{}) {
			// price = min(tip, gasFeeCap - baseFee) + baseFee
			price := math.BigMin(new(big.Int).Add(tx.GasTipCap(), baseFee), tx.GasFeeCap())
			result.GasPrice = (*hexutil.Big)(price)
		} else {
			result.GasPrice = (*hexutil.Big)(tx.GasFeeCap())
		}
	}
	return result
}

// newRPCTransactionFromBlockIndex returns a transaction that will serialize to the RPC representation.
func newRPCTransactionFromBlockIndex(b *types.Block, index uint64, config *params.ChainConfig) *RPCTransaction {
	txs := b.Transactions()
	if index >= uint64(len(txs)) {
		return nil
	}
	return newRPCTransaction(txs[index], b.Hash(), b.NumberU64(), b.Time(), index, b.BaseFee(), config)
}

// RPCMarshalHeader converts the given header to the RPC output .
func RPCMarshalHeader(head *types.Header) map[string]interface{} {
	result := map[string]interface{}{
		"number":           (*hexutil.Big)(head.Number),
		"hash":             head.Hash(),
		"parentHash":       head.ParentHash,
		"nonce":            head.Nonce,
		"mixHash":          head.MixDigest,
		"sha3Uncles":       head.UncleHash,
		"logsBloom":        head.Bloom,
		"stateRoot":        head.Root,
		"miner":            head.Coinbase,
		"difficulty":       (*hexutil.Big)(head.Difficulty),
		"extraData":        hexutil.Bytes(head.Extra),
		"gasLimit":         hexutil.Uint64(head.GasLimit),
		"gasUsed":          hexutil.Uint64(head.GasUsed),
		"timestamp":        hexutil.Uint64(head.Time),
		"transactionsRoot": head.TxHash,
		"receiptsRoot":     head.ReceiptHash,
	}

	if head.BaseFee != nil {
		result["baseFeePerGas"] = (*hexutil.Big)(head.BaseFee)
	}

	if head.WithdrawalsHash != nil {
		result["withdrawalsRoot"] = head.WithdrawalsHash
	}

	return result
}

// RPCMarshalBlock converts the given block to the RPC output which depends on fullTx. If inclTx is true transactions are
// returned. When fullTx is true the returned block contains full transaction details, otherwise it will only contain
// transaction hashes.
func RPCMarshalBlock(block *types.Block, inclTx bool, fullTx bool, config *params.ChainConfig) map[string]interface{} {
	fields := RPCMarshalHeader(block.Header())
	fields["size"] = hexutil.Uint64(block.Size())

	if inclTx {
		formatTx := func(idx int, tx *types.Transaction) interface{} {
			return tx.Hash()
		}
		if fullTx {
			formatTx = func(idx int, tx *types.Transaction) interface{} {
				return newRPCTransactionFromBlockIndex(block, uint64(idx), config)
			}
		}
		txs := block.Transactions()
		transactions := make([]interface{}, len(txs))
		for i, tx := range txs {
			transactions[i] = formatTx(i, tx)
		}
		fields["transactions"] = transactions
	}
	uncles := block.Uncles()
	uncleHashes := make([]common.Hash, len(uncles))
	for i, uncle := range uncles {
		uncleHashes[i] = uncle.Hash()
	}
	fields["uncles"] = uncleHashes
	if block.Header().WithdrawalsHash != nil {
		fields["withdrawals"] = block.Withdrawals()
	}
	return fields
}
