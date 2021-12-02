package contractapi

import (
	"os"
	"encoding/json"	
	"log"
	"net/http"	
	"strconv"	
    guuid "github.com/google/uuid"
	"github.com/ravengit/ravenpod-cc-dc-go/config"
	"github.com/ravengit/ravenpod-cc-dc-go/model"
	"github.com/ravengit/ravenpod-cc-dc-go/datapublisher"
)

type DataCollector struct {
	AccessKey string
	SecretAccessKey string
	DataPipelineRegion string
	DataPipelineAccessKey string
	DataPipelineSecretAccessKey string
}

type DCOptions struct {
	Debug bool 
}

func NewDataCollector(accessKey string, secretAccessKey string, options DCOptions) *DataCollector {
	log.Println("[RAVENPOD] Initializing data collector")
	dc := DataCollector{AccessKey: accessKey, SecretAccessKey: secretAccessKey}
	getDataPipelineAccessKey(&dc)
	datapublisher.InitDataPublisher(dc.DataPipelineRegion, dc.DataPipelineAccessKey, dc.DataPipelineSecretAccessKey)

	log.Println("[RAVENPOD] Finished initializing data collector")
    return &dc
}
func (* DataCollector) RpBeforeHook(ctx TransactionContextInterface) {

	dataPublisher := datapublisher.GetDataPublisher()

	stub := ctx.GetStub()
	transientMap, err := stub.GetTransient()
	if err != nil {
		log.Println("[RAVENPOD] Error when accessing transient map.")
		return
	}

	hasRavenpodData := transientMap["rp_webTxnId"]
	if len(hasRavenpodData) > 0 {
		webTxnId := string( transientMap["rp_webTxnId"] )
		ravenpodTxnId := string( transientMap["rp_ravenpodTxnId"] )
		blockchainTxnId := stub.GetTxID()
		invocationId := guuid.New().String()
		accountId := string( transientMap["rp_accountId"] )
		channel := stub.GetChannelID()
		moduleName :=  os.Getenv("CORE_CHAINCODE_ID_NAME")	
		funcName, args := stub.GetFunctionAndParameters()
		sequenceNumber := 0;
		nestLevel := 0;
		log.Println("[RAVENPOD] Before txn hook triggered.", webTxnId, ravenpodTxnId, blockchainTxnId, accountId, channel, moduleName, funcName, args)
		argsInBytes, _ := json.Marshal(args) 
		entryEvent := model.NewTraceRecord(accountId, webTxnId, ravenpodTxnId, blockchainTxnId, invocationId, channel, false, sequenceNumber, nestLevel, moduleName, funcName, string(argsInBytes), "", "", "", model.EVENT_TYPE_ENTRY, "")
		dataPublisher.PushRecord(entryEvent, accountId)
		nestLevel++;
		sequenceNumber++;
		transientMap["rp_invocationId"] = []byte(invocationId)
		transientMap["rp_channel"] = []byte(channel)
		transientMap["rp_moduleName"] = []byte(moduleName)
		transientMap["rp_funcName"] = []byte(funcName)
		transientMap["rp_args"] = argsInBytes
		transientMap["rp_sequenceNumber"] = []byte(strconv.Itoa(sequenceNumber))
		transientMap["rp_nestLevel"] = []byte(strconv.Itoa(nestLevel))
	} else {
		log.Println("[RAVENPOD] Ravenpod context data not found. Did you enable Ravenpod data collector in the web app?")
		return
	}

}

func (* DataCollector) RpAfterHook(ctx TransactionContextInterface) {

	dataPublisher := datapublisher.GetDataPublisher()

	stub := ctx.GetStub()
	transientMap, err := stub.GetTransient()
	if err != nil {
		log.Println("[RAVENPOD] Error when accessing transient map.")
		return
	}

	hasRavenpodData := transientMap["rp_webTxnId"]
	if len(hasRavenpodData) > 0 {
		webTxnId := string(transientMap["rp_webTxnId"])
		ravenpodTxnId := string(transientMap["rp_ravenpodTxnId"])
		blockchainTxnId := stub.GetTxID()
		invocationId := string(transientMap["rp_invocationId"])
		accountId := string(transientMap["rp_accountId"])
		channel := string(transientMap["rp_channel"])
		moduleName := string(transientMap["rp_moduleName"])
		funcName := string(transientMap["rp_funcName"])
		args := string(transientMap["rp_args"])
		nestLevel, _ := strconv.Atoi( string(transientMap["rp_nestLevel"]) )
		sequenceNumber, _ := strconv.Atoi( string(transientMap["rp_sequenceNumber"]) )
		nestLevel--
		log.Println("[RAVENPOD] After txn hook triggered.", webTxnId, ravenpodTxnId, blockchainTxnId, accountId, channel, moduleName, funcName, args)
		exitEvent := model.NewTraceRecord(accountId, webTxnId, ravenpodTxnId, blockchainTxnId, invocationId, channel, false, sequenceNumber, nestLevel, moduleName, funcName, args, "", "", "", model.EVENT_TYPE_EXIT, "");
		dataPublisher.PushRecord(exitEvent, accountId)
	} else {
		log.Println("[RAVENPOD] Ravenpod context data not found. Did you enable Ravenpod data collector in the web app?")
		return
	}

}

func getDataPipelineAccessKey(dc * DataCollector) {

	req, _ := http.NewRequest("GET", config.API_GET_DATA_PIPELINE_ACCESS, nil)

	q := req.URL.Query()
	q.Add("accessKey", dc.AccessKey)
	q.Add("secretAccessKey", dc.SecretAccessKey)
	req.URL.RawQuery = q.Encode()

	log.Println("[RAVENPOD] Data pipeline access key request", req.URL.String())

    resp, err := http.Get(req.URL.String())
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()

    log.Println("[RAVENPOD] Data pipeline access key response status:", resp.Status)	
	
	var cResp model.DataPipelineAccessResponse
	if err := json.NewDecoder(resp.Body).Decode(&cResp); err != nil {
		log.Fatal("[RAVENPOD] Error when obtaining data pipeline access keys.")
		log.Fatal(err)
	}

	dc.DataPipelineRegion = cResp.Region
	dc.DataPipelineAccessKey = cResp.AccessKey
	dc.DataPipelineSecretAccessKey = cResp.SecretAccessKey

}
