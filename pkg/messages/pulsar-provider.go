/*
Copyright 2020 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package messages

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/pulsar/pulsar-client-go/pulsar"
	"k8s.io/klog"
)

type pulsarProvider struct {
	messageProviderDefinition *ProviderDefinition
	client                    pulsar.Client
	producer                  map[string]pulsar.Producer
	consumer                  map[string]pulsar.Consumer
}

//Initialize Pulsar client and creater a new map container for the consumers
func (provider *pulsarProvider) initialize(mpd *ProviderDefinition) error {
	provider.messageProviderDefinition = mpd

	clientOptions := pulsar.ClientOptions{
		URL:                     mpd.URL,
		OperationTimeoutSeconds: mpd.Timeout,
	}

	client, err := pulsar.NewClient(clientOptions)

	if err != nil {
		return err
	}

	provider.client = client
	provider.producer = make(map[string]pulsar.Producer)
	provider.consumer = make(map[string]pulsar.Consumer)

	return nil
}

func (provider *pulsarProvider) Subscribe(node *EventNode) error {
	if klog.V(6) {
		urlAndTopic := fmt.Sprintf("%s:%s", provider.messageProviderDefinition.URL, node.Topic)
		klog.Infof("Subscribing to Pulsar provider on %s", urlAndTopic)
	}

	consumerOptions := pulsar.ConsumerOptions{
		SubscriptionName: node.Name,
		Topic:            node.Topic,
	}

	client := provider.client

	consumer, err := client.Subscribe(consumerOptions)

	if err != nil {
		log.Fatalf("Could not establish subscription: %v", err)
	}

	provider.consumer[node.Name] = consumer

	return nil
}

// Send an event to some eventSource.
func (provider *pulsarProvider) Send(node *EventNode, payload []byte, header interface{}) error {
	producer, ok := provider.producer[node.Name]

	client := provider.client
	var err error
	if !ok {
		producerOptions := pulsar.ProducerOptions{
			Topic: node.Topic,
		}

		producer, err = client.CreateProducer(producerOptions)

		if err != nil {
			return err
		}

		provider.producer[node.Name] = producer
	}

	klog.Infof("pulsarProvider: Sending %s", string(payload))

	producerMsg := pulsar.ProducerMessage{
		Payload: payload,
	}

	//Publishes a message to the producer's topic
	if err := producer.Send(context.Background(), producerMsg); err != nil {
		return err
	}

	//Flush all the messages buffered in the client
	if err := producer.Flush(); err != nil {
		return err
	}

	return nil
}

func (provider *pulsarProvider) Receive(node *EventNode) ([]byte, error) {
	consumer, ok := provider.consumer[node.Name]

	if !ok {
		klog.Errorf("no subscription for eventSource '%s'. It should be defined and Subscribed to.", node.Name)
	}
	if klog.V(6) {
		klog.Infof("pulsarPro: Looking for data from source %s and provider %s", node.Name, node.ProviderRef)
	}
	// timeout := provider.messageProviderDefinition.Timeout * time.Second
	timeout := provider.messageProviderDefinition.Timeout
	if klog.V(6) {
		klog.Infof("pulsarProvider.Receive timeout: %v", timeout)
	}
	msg, err := consumer.Receive(context.Background())

	if err != nil {
		return nil, err
	}

	return msg.Payload(), nil
}

// ListenAndServe listens for new eventDefinition on some eventSource and calls the ReceiverFunc on the message payload.
func (provider *pulsarProvider) ListenAndServe(node *EventNode, receiver ReceiverFunc) {
	urlAndTopic := fmt.Sprintf("%s:%s", provider.messageProviderDefinition.URL, node.Topic)
	if klog.V(5) {
		klog.Infof("pulsarProvider: Starting to listen for PULSAR eventDefinition from %s", urlAndTopic)
	}

	msgChannel := make(chan pulsar.ConsumerMessage)
	consumer, ok := provider.consumer[node.Name]

	if !ok {
		klog.Errorf("unable to set up listener for PULSAR eventDefinition for %s", urlAndTopic)
	}

	defer consumer.Close()

	for msg := range msgChannel {
		if klog.V(8) {
			klog.Infof("Received message on %s: %s\n", urlAndTopic, msg.Payload())
		}
		receiver(msg.Payload())
	}

	delete(provider.consumer, node.Name)
}

func newPULSARProvider(mpd *ProviderDefinition) (*pulsarProvider, error) {
	provider := new(pulsarProvider)
	if err := provider.initialize(mpd); err != nil {
		return nil, err
	}

	return provider, nil
}
