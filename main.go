package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
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
	listenAddr      		= "0.0.0.0:8080"
	bootstrapServer 		= ""
	kafkaVersion    		= "2.5.0"
	tlsCaFile				= ""
	tlsCertFile				= ""
	tlsKeyFile				= ""
	tlsInsecureSkipVerify 	= false
	fetchInterval			= 1 * time.Minute
	verbose  				= false
	saramaVerbose  			= false

	topicData = &TV{
		Brokers: map[string]*TVBroker{},
	}
)

func init() {
	flag.StringVar(&listenAddr, "listen-addr", "0.0.0.0:8080", "The address where the HTTP Server runs")
	flag.StringVar(&bootstrapServer, "bootstrap-server", "", "Comma separate list of Kafka Brokers to connect to")
	flag.DurationVar(&fetchInterval, "fetch-interval", 1 * time.Minute, "The interval at which to fetch the new topic data")
	flag.StringVar(&kafkaVersion, "kafka-version", "2.5.0", "Version of the Kafka cluster")
	flag.StringVar(&tlsCaFile, "tls-ca-file", "", "File with the CA certificate for server authentication")
	flag.StringVar(&tlsCertFile, "tls-cert-file", "", "File with the client certificate for client authentication")
	flag.StringVar(&tlsKeyFile, "tls-key-file", "", "File with the key for client authentication")
	flag.BoolVar(&tlsInsecureSkipVerify, "tls-insecure-skip-verify", false, "Skip TLS verification " +
		"when connecting to the Kafka brokers. Use at your own risk!")
	flag.BoolVar(&verbose, "verbose", false, "Turn on verbose logging")
	flag.BoolVar(&saramaVerbose, "sarama-verbose", false, "Turn on verbose logging int he Sarama Kafka client")
	flag.Parse()

	if len(bootstrapServer) == 0 {
		panic("No bootstrap servers specified. Please use the option --bootstrap-server to specify at least one Kafka broker!")
	}
}

func main() {
	adminClient := createAdminClient()
	go pollTopicForTopicData(adminClient)

	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/api/topics", getTopics)

	log.Println("Creating HTTP server listening on: ", listenAddr)
	err := http.ListenAndServe(listenAddr, nil)

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
	kafkaVersion, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil	{
		log.Fatal(err)
	}

	config := sarama.NewConfig()
	config.ClientID = "KafkaTopicView"
	config.Version = kafkaVersion
	config.Metadata.RefreshFrequency = 1 * time.Minute

	config.Admin.Retry.Backoff = 1 * time.Second
	config.Admin.Retry.Max = 5
	config.Admin.Timeout = 10 * time.Second

	if len(tlsCaFile) > 0 || tlsInsecureSkipVerify == true {
		log.Print("Enabling TLS in Kafka Admin client")

		caCertPool := x509.NewCertPool()

		if len(tlsCaFile) > 0 {
			tlsCa, err := ioutil.ReadFile(tlsCaFile)
			if err != nil {
				log.Print("Failed to load the TLS CA certificate file")
				log.Fatal(err)
			}

			caCertPool.AppendCertsFromPEM(tlsCa)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: tlsInsecureSkipVerify,
			RootCAs:            caCertPool,
		}

		if len(tlsCertFile) > 0 && len(tlsKeyFile) > 0	{
			tlsClient, err := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
			if err != nil {
				log.Print("Failed to load the TLS client certificate and key file")
				log.Fatal(err)
			}

			config.Net.TLS.Config.Certificates = []tls.Certificate{tlsClient}
		}
	}

	brokerList := strings.Split(bootstrapServer, ",")
	log.Printf("Creating ClusterAdmin client for Brokers: %s", strings.Join(brokerList, ", "))
	client, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		log.Fatal(err)
	}

	return client
}

// Finds the min.insync.replicas configuration in this Kafka cluster
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

	//log.Printf("Found out that min.insync.replicas=%d and transaction.state.log.min.isr=%d", minIsr, minTransactionIsr)
	return
}

// Finds out if given topic has its own configuration of min.insync.replicas and if not uses the cluster wide
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

func pollTopicForTopicData(adminClient sarama.ClusterAdmin) 	{
	for {
		log.Println("Refreshing topic data")

		newData := TV{
			Brokers: map[string]*TVBroker{},
		}

		// Get list o brokers
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

		// Finding the min.insync.replicas configuration of thsi Kafka cluster
		minIsr, minTransactionIsr := findMinIsr(adminClient, brokers)

		// List topicMetadata
		topics, err := adminClient.ListTopics()
		if err != nil {
			log.Println("Failed to list topicMetadata")
			log.Fatal(err)
		}

		// Describing topics to get ISR info
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

		topicData = &newData

		log.Println("Finished refreshing topic data. Next refresh will happen in", fetchInterval)

		time.Sleep(fetchInterval)
	}
}

