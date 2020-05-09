package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type TVPartition struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	State     string `json:"state"`
	Leader    bool   `json:"leader"`
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
	config.Metadata.RefreshFrequency = 1 * time.Minute
	
	brokerList := strings.Split(bootstrapServer, ",")
	log.Printf("Creating ClusterAdmin client for Brokers: %s", strings.Join(brokerList, ", "))
	client, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		log.Fatal(err)
	}

	return client
}

func findMinIsr(adminClient sarama.ClusterAdmin, brokers []*sarama.Broker)	(minIsr, minTransactionIsr int)	{
	for _, broker := range brokers {
		// Find default min.insync.replicas
		isrConfigs, err := adminClient.DescribeConfig(sarama.ConfigResource{
			Type:        4,
			Name:        fmt.Sprint(broker.ID()),
			ConfigNames: []string{"min.insync.replicas", "transaction.state.log.min.isr"},
		})
		if err != nil {
			log.Println("Failed to get broker configuration")
			log.Fatal(err)
		}

		log.Printf("Broker configuration %v", isrConfigs)

		for _, config := range isrConfigs	{
			if config.Name == "min.insync.replicas"	{
				minIsr, err = strconv.Atoi(config.Value)
				if err != nil {
					log.Println("Failed to convert min.insync.replicas to int")
					log.Fatal(err)
				}
			}

			if config.Name == "transaction.state.log.min.isr"	{
				minTransactionIsr, err = strconv.Atoi(config.Value)
				if err != nil {
					log.Println("Failed to convert transaction.state.log.min.isr to int")
					log.Fatal(err)
				}
			}

			if minIsr != 0 && minTransactionIsr != 0	{
				log.Printf("Found out that min.insync.replicas=%d and transaction.state.log.min.isr=%d", minIsr, minTransactionIsr)
				return
			}
		}
	}

	if minIsr == 0	{
		minIsr = 1
	}

	if minTransactionIsr == 0	{
		minTransactionIsr = 2
	}

	log.Printf("Found out that min.insync.replicas=%d and transaction.state.log.min.isr=%d", minIsr, minTransactionIsr)
	return
}

func getPerTopicMinIsr(name string, topicDetails map[string]sarama.TopicDetail, minIsr int, minTransactionIsr int)	int	{
	topic := topicDetails[name]

	if topic.ConfigEntries != nil	{
		minIsrConfig := topic.ConfigEntries["min.insync.replicas"]
		if minIsrConfig != nil	{
			minIsr, err := strconv.Atoi(*minIsrConfig)
			if err != nil {
				log.Println("Failed to convert min.insync.replicas to int")
				log.Fatal(err)
			}

			return minIsr
		}
	}

	if name == "__transaction_state"	{
		return minTransactionIsr
	} else {
		return minIsr
	}
}

func pollTopicMetadata(adminClient sarama.ClusterAdmin) 	{
	for {
		newData := TV{
			Brokers: map[string]*TVBroker{},
		}

		// Get list o brokers
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

		minIsr, minTransactionIsr := findMinIsr(adminClient, brokers)

		// List topicMetadata
		topics, err := adminClient.ListTopics()
		if err != nil {
			log.Println("Failed to list topicMetadata")
			log.Fatal(err)
		}

		log.Printf("List topicMetadata %v", topics)
		
		log.Println("Refreshing Topic metadata")
		topicMetadata, err := adminClient.DescribeTopics([]string{})
		if err != nil {
			log.Println("Failed to get Topic metadata")
			log.Fatal(err)
		}

		for _, topic := range topicMetadata {
			//log.Println("Found Topic: ", topic.Name)
			partitions := topic.Partitions
			actualMinIsr := getPerTopicMinIsr(topic.Name, topics, minIsr, minTransactionIsr)

			for _, partition:= range partitions	{
				//log.Println("Found Partition: ", partition.ID)
				replicaCount := len(partition.Replicas)
				isrCount := len(partition.Isr)
				offlineCount := len(partition.OfflineReplicas)

				var state string

				if isrCount == replicaCount && isrCount >= actualMinIsr	{
					state = "online"
				} else if isrCount > actualMinIsr	{
					state = "in-sync"
				} else if isrCount == actualMinIsr	{
					state = "at-min-isr"
				} else if isrCount < actualMinIsr	{
					state = "under-min-isr"
				} else if replicaCount >= offlineCount	{
					state = "offline"
				} else {
					state = "unknown"
				}

				replicas := partition.Replicas
				for _, replica:= range replicas	{
					tvPartition := TVPartition{
						Topic:     topic.Name,
						Partition: partition.ID,
						State:     state,
						Leader:    false,
					}

					if replica == partition.Leader	{
						tvPartition.Leader = true
					}

					if newData.Brokers[fmt.Sprint(replica)] != nil	{
						newData.Brokers[fmt.Sprint(replica)].Partitions = append(newData.Brokers[fmt.Sprint(replica)].Partitions, tvPartition)
					} else {
						newData.Brokers[fmt.Sprint(replica)] = &TVBroker{
							Partitions: []TVPartition{tvPartition},
						}
					}
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

