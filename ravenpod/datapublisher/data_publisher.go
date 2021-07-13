package datapublisher

import (
	"sync"
	"log"
    "encoding/json"    
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"	
	"github.com/ravengit/fabric-contract-api-go/ravenpod/model"
)

const BATCH_SIZE = 1
const STREAM_NAME_TRACE = "trace"
const STREAM_NAME_KEY_TRACE = "keytrace"

type AWSKinesis struct {
	stream          string
	region          string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
}

type TraceRecordSet struct {
    items             []*kinesis.PutRecordsRequestEntry
	mutex             *sync.Mutex
}

type KeyTraceRecordSet struct {
    items             []*kinesis.PutRecordsRequestEntry
	mutex             *sync.Mutex
}

type DataPublisher struct {
	kinesisSession *kinesis.Kinesis
	trs TraceRecordSet
	ktrs KeyTraceRecordSet
}

var (
	doOnce sync.Once
	dataPublisher *DataPublisher // singleton
)

func InitDataPublisher(region string, dataPipelineAccessKey string, dataPipelineSecretAccessKey string) {
	doOnce.Do(func() {
		log.Println(region, dataPipelineAccessKey, dataPipelineSecretAccessKey)
		// connect to aws-kinesis
		sess := session.New(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewStaticCredentials(dataPipelineAccessKey, dataPipelineSecretAccessKey, ""),
		})
		dataPublisher = &DataPublisher{
			kinesisSession: kinesis.New(sess),
			trs: TraceRecordSet{mutex: &sync.Mutex{}},
			ktrs: KeyTraceRecordSet{mutex:  &sync.Mutex{}},
		}
    })
}

func GetDataPublisher() *DataPublisher {
    return dataPublisher
}

func (dp *DataPublisher) PushTraceRecord(record model.Trace) {
	dp.trs.mutex.Lock()
	defer dp.trs.mutex.Unlock()

    jsonBuffer, err := json.Marshal(&record)
    if err != nil {
        log.Println("Error marshalling trace record data", err)
        return
	}

	item := &kinesis.PutRecordsRequestEntry{
		Data:         jsonBuffer,
		PartitionKey: aws.String(record.AccountId),
	}

	dp.trs.items = append(dp.trs.items, item)

	log.Println("Pipeline items: ", dp.trs.items)

	if len(dp.trs.items) >= BATCH_SIZE {
		dp.flushRecordSet(STREAM_NAME_TRACE, &dp.trs.items)
	}

}

func (dp *DataPublisher) flushRecordSet(streamName string, items *[]*kinesis.PutRecordsRequestEntry) {
	_, err := dp.kinesisSession.PutRecords(&kinesis.PutRecordsInput{
		Records:    *items,
		StreamName: aws.String(streamName),
	})
	if err != nil {
		log.Fatal("Error pushing trace recordset to data pipeline", err)
	} else {
		log.Println("Trace recordset pushed to data pipeline successfully")
		*items = nil
	}
}

func (dp *DataPublisher) PushKeyTraceRecord(record model.Trace) {
}