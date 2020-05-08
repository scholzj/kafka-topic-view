package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"net/http"
	"strings"
	"time"
)

type TVPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	State     string `json:"State"`
	Leader    bool   `json:"Leader"`
}

type TVBroker struct {
	Partitions []TVPartition `json:"partitions"`
}

type TV struct {
	Brokers map[string]*TVBroker `json:"brokers"`
}

var (
	addr 		 	= "0.0.0.0:8080"
	bootstrapServer = ""
	//version  = ""
	//group    = ""
	//topics   = ""
	//assignor = ""
	//oldest   = true
	verbose  = false
	saramaVerbose  = false

	topicData = &TV{
		Brokers: map[string]*TVBroker{},
	}
)

func init() {
	flag.StringVar(&addr, "hostname", "0.0.0.0:8080", "The address where the HTTP Server runs")
	flag.StringVar(&bootstrapServer, "bootstrap-server", "", "Comma separate list of Kafka Brokers to connect to")
	//flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	//flag.StringVar(&version, "version", "2.5.0", "Kafka cluster version")
	//flag.StringVar(&topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")
	//flag.StringVar(&assignor, "assignor", "range", "Consumer group Partition assignment strategy (range, roundrobin, sticky)")
	//flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&verbose, "verbose", false, "Turn on verbose logging")
	flag.BoolVar(&saramaVerbose, "sarama-verbose", false, "Turn on verbose logging int he Sarama Kafka client")
	flag.Parse()

	if len(bootstrapServer) == 0 {
		panic("No bootstrap servers specified. Please use the option --bootstrap-server to specify at least one Kafka broker!")
	}
}

func main() {
	adminClient := createAdminClient()
	go pollTopicMetadata(adminClient)

	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/api/topics", getTopics)

	log.Println("Creating HTTP server listening on: ", addr)
	err := http.ListenAndServe(addr, nil)

	if err != nil {
		log.Fatal(err)
	}
}

func getTopics(w http.ResponseWriter, r *http.Request)	{
	if r.Method != "GET" {
		w.WriteHeader(501)
		return
	}

	jsonData, err := json.MarshalIndent(topicData, "", "    ")
	if err != nil {
		log.Fatal(err)
	}

	if verbose	{
		log.Println("Got API request - responding: ", string(jsonData))
	}

	w.Write(jsonData)
}

func createAdminClient() sarama.ClusterAdmin	{
	kafkaVersion, err := sarama.ParseKafkaVersion("2.5.0")
	if err != nil	{
		log.Fatal(err)
	}

	config := sarama.NewConfig()
	config.Version = kafkaVersion

	brokerList := strings.Split(bootstrapServer, ",")
	log.Printf("Creating ClusterAdmin client for Brokers: %s", strings.Join(brokerList, ", "))
	client, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		log.Fatal(err)
	}

	return client
}

func pollTopicMetadata(adminClient sarama.ClusterAdmin) 	{
	for {
		newData := TV{
			Brokers: map[string]*TVBroker{},
		}

		log.Println("Refreshing cluster metadata")
		brokers, _, err := adminClient.DescribeCluster()
		if err != nil {
			log.Println("Failed to get cluster metadata")
			log.Fatal(err)
		}

		for _, broker := range brokers {
			//log.Println("Found broker: ", fmt.Sprint(broker.ID()))
			newData.Brokers[fmt.Sprint(broker.ID())] = &TVBroker{
				Partitions: []TVPartition{},
			}
		}

		log.Println("Refreshing Topic metadata")
		topics, err := adminClient.DescribeTopics([]string{})
		if err != nil {
			log.Println("Failed to get Topic metadata")
			log.Fatal(err)
		}

		for _, topic := range topics {
			//log.Println("Found Topic: ", topic.Name)
			partitions := topic.Partitions

			for _, partition:= range partitions	{
				//log.Println("Found Partition: ", partition.ID)
				replicas := partition.Replicas
				for _, replica:= range replicas	{
					tvPartition := TVPartition{
						Topic:     topic.Name,
						Partition: partition.ID,
						State:     "green",
						Leader:    false,
					}

					if replica == partition.Leader	{
						tvPartition.Leader = true
					}

					newData.Brokers[fmt.Sprint(replica)].Partitions = append(newData.Brokers[fmt.Sprint(replica)].Partitions, tvPartition)
				}
			}
		}

		if verbose	{
			log.Printf("Prepared new data %v\n", newData)

			for _, broker := range newData.Brokers {
				log.Printf("\tBroker: %v\n", broker)
			}
		}

		topicData = &newData

		time.Sleep(1 * time.Minute)
	}
}

