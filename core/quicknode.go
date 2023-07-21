package core

import (
	"encoding/json"
	"math/big"
	"os"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

type BlockData struct {
	Receipts        types.Receipts
	BlockWithHash   map[string]interface{}
	BlockWithFullTx map[string]interface{}
	Codes           interface{}
	Balances        interface{}
	Current         *types.Header
	Final           *types.Header
	Safe            *types.Header
	Trace           interface{}
}

func (bc *BlockChain) cache(head *types.Block, logs []*types.Log) {
	// Create a ZeroMQ PUB socket
	// publisher, _ := zmq4.NewSocket(zmq4.PUB)
	// defer publisher.Close()
	// publisher.Bind("tcp://*:5556")

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(8) // number of goroutines

	// Create channels for each goroutine to send its result
	blockWithHashOnlyCh := make(chan map[string]interface{})
	blockWithFullTxCh := make(chan map[string]interface{})
	codesCh := make(chan interface{})
	balancesCh := make(chan interface{})
	currentCh := make(chan *types.Header)
	finalCh := make(chan *types.Header)
	safeCh := make(chan *types.Header)
	traceCh := make(chan interface{})

	// Start goroutines
	go func() {
		defer wg.Done()
		blockWithHashOnly := bc.getBlockByNumber(head, true, false)
		blockWithHashOnlyCh <- blockWithHashOnly
	}()

	go func() {
		defer wg.Done()
		blockWithFullTx := bc.getBlockByNumber(head, true, true)
		blockWithFullTxCh <- blockWithFullTx
	}()

	go func() {
		defer wg.Done()
		codes := bc.getCodes(head)
		codesCh <- codes
	}()

	go func() {
		defer wg.Done()
		balances := bc.getBalances(head)
		balancesCh <- balances
	}()

	go func() {
		defer wg.Done()
		current := bc.CurrentBlock()
		currentCh <- current
	}()

	go func() {
		defer wg.Done()
		final := bc.CurrentFinalBlock()
		finalCh <- final
	}()

	go func() {
		defer wg.Done()
		safe := bc.CurrentSafeBlock()
		safeCh <- safe
	}()

	go func() {
		defer wg.Done()
		trace := traceBlockByNumber(head)
		traceCh <- trace
	}()

	// Wait for all goroutines to finish
	wg.Wait()

	// Collect the results from the channels
	blockData := BlockData{
		BlockWithHash:   <-blockWithHashOnlyCh,
		BlockWithFullTx: <-blockWithFullTxCh,
		Codes:           <-codesCh,
		Balances:        <-balancesCh,
		Current:         <-currentCh,
		Final:           <-finalCh,
		Safe:            <-safeCh,
		Trace:           <-traceCh,
	}

	// Marshal the data to JSON
	data, err := json.MarshalIndent(blockData, "", "  ")
	if err != nil {
		panic("shitttt MarshalIndent" + err.Error())
	}

	// Write to a file
	file, err := os.OpenFile("/tmp/data.json", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic("shitttt OpenFile" + err.Error())
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		panic("shitttt Write" + err.Error())
	}

	// Send the data over ZeroMQ
	//publisher.SendBytes(data, 0)
}

func traceBlockByNumber(head *types.Block) interface{} {
	return nil
}

func (bc *BlockChain) getBalances(head *types.Block) interface{} {
	return nil
}

func (bc *BlockChain) getCodes(head *types.Block) interface{} {
	// state, err := bc.StateAt(root)
	// if state == nil || err != nil {
	// 	return nil, err
	// }
	// code := state.GetCode(addr)
	// return code, nil
	return nil
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
