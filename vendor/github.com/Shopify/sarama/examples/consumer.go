package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
)

func main() {
	sarama.Logger = log.Default()
	// 创建消费者对象
	config := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion("0.11.0.1")
	config.Version = version
	config.Net.SASL.Enable = true
	config.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
	config.Net.SASL.GSSAPI.ServiceName = "kafka"
	config.Net.SASL.GSSAPI.KerberosConfigPath = "C:\\krb5.conf"
	config.Net.SASL.GSSAPI.Realm = "BOTECH.COM"
	config.Net.SASL.GSSAPI.Username = "qdgakk"
	config.Net.SASL.GSSAPI.KeyTabPath = "C:\\user.keytab"
	config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
	consumer, err := sarama.NewConsumer([]string{"172.16.1.102:21007"}, config)
	if err != nil {
		panic(err)
	}

	// 程序运行结束时，调用Close关闭消费者对象
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	// 创建消费者对象管理的分区消费者对象
	partitionConsumer, err := consumer.ConsumePartition("hb-human-recognition", 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	// 程序运行结束时，调用Close关闭分区消费者对象
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Println(err)
		}
	}()

	//用系统中断信号作为结束程序的信号
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
		select {
		// 消费者从分区中拿出数据消费
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			consumed++
		// 收到中断信号结束程序
		case <-signals:
			break ConsumerLoop
		}
	}
}