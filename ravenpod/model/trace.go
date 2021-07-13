package model

import (
    guuid "github.com/google/uuid"
    "os"
    "time"
)

type Trace struct {
    Id string `json:"id"`
    AccountId string `json:"account_id"`
    WebTxnId string `json:"web_txn_id"`
    RavenpodTxnId string `json:"ravenpod_txn_id"`
    BlockchainTxnId string `json:"blockchain_txn_id"`
    InvocationId string `json:"invocation_id"`
    Hostname string `json:"hostname"`
    Channel string `json:"channel"`
    IsTransactionStart bool `json:"is_transaction_start"`
    SequenceNumber int `json:"sequence_number"`
    NestLevel int `json:"nest_level"`
    ModuleName string `json:"module_name"`
    FunctionName string `json:"function_name"`
    Args string `json:"args"`
    ReturnedResult string `json:"returned_result"`
    EventType int `json:"event_type"`
    EventData string `json:"event_data"`
    EventTime string `json:"event_time"`
    CreatedAt string `json:"created_at"`
    UpdatedAt string `json:"updated_at"`
    TransientMap string `json:"transient_map"`
    Collection string `json:"collection"`
}

const (
	EVENT_TYPE_ENTRY = 1
	EVENT_TYPE_EXIT  = 2
)

func NewTraceRecord(accountId string,
    webTxnId string,
    ravenpodTxnId string,
    blockchainTxnId string,
    invocationId string,
    channel string,
    isTransactionStart bool,
    sequenceNumber int, 
    nestLevel int, 
    moduleName string, 
    functionName string, 
    args string, 
    transientMap string, 
    collection string, 
    returnedResult string, 
    eventType int, 
    eventData string) Trace {

    currentTime := time.Now()
    format := "2006-01-02 15:04:05.000"
    hostname, _ := os.Hostname()

	traceRecord := Trace{
        Id: guuid.New().String(),
        AccountId: accountId,
        WebTxnId: webTxnId,
        RavenpodTxnId: ravenpodTxnId,
        BlockchainTxnId: blockchainTxnId,
        InvocationId: invocationId,
        Hostname: hostname,
        Channel: channel,
        IsTransactionStart: isTransactionStart,
        SequenceNumber: sequenceNumber,
        NestLevel: nestLevel,
        ModuleName: moduleName,
        FunctionName: functionName,
        Args: args,
        ReturnedResult: returnedResult,
        EventType: eventType,
        EventData: eventData,
        EventTime: currentTime.Format(format),
        CreatedAt: currentTime.Format(format),
        UpdatedAt: "",
        TransientMap: transientMap,
        Collection: collection}

    return traceRecord

}
