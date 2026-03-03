package proxycmder

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kafkapublisher "github.com/papercomputeco/tapes/pkg/publisher/kafka"
)

var _ = Describe("proxyCommander publisher config", func() {
	Describe("validatePublisherConfig", func() {
		It("accepts missing kafka config and disables publishing", func() {
			cmder := &proxyCommander{}

			Expect(cmder.validatePublisherConfig()).To(Succeed())
		})

		It("rejects kafka topic without brokers", func() {
			cmder := &proxyCommander{
				kafkaTopic: "tapes.nodes.v1",
			}

			Expect(cmder.validatePublisherConfig()).To(MatchError(
				"kafka brokers are required when kafka topic is set",
			))
		})

		It("rejects kafka brokers without topic", func() {
			cmder := &proxyCommander{
				kafkaBrokers: "localhost:9092",
			}

			Expect(cmder.validatePublisherConfig()).To(MatchError(
				"kafka topic is required when kafka brokers are set",
			))
		})
	})

	Describe("newPublisher", func() {
		It("returns nil publisher when kafka config is missing", func() {
			cmder := &proxyCommander{}

			pub, err := cmder.newPublisher()
			Expect(err).NotTo(HaveOccurred())
			Expect(pub).To(BeNil())
		})

		It("creates a kafka publisher when minimum kafka config is provided", func() {
			cmder := &proxyCommander{
				kafkaBrokers: "localhost:9092",
				kafkaTopic:   "tapes.nodes.v1",
			}

			pub, err := cmder.newPublisher()
			Expect(err).NotTo(HaveOccurred())
			Expect(pub).NotTo(BeNil())
			_, ok := pub.(*kafkapublisher.Publisher)
			Expect(ok).To(BeTrue())
		})
	})

	Describe("splitKafkaBrokers", func() {
		It("trims and drops empty values", func() {
			brokers := splitKafkaBrokers(" localhost:9092, ,host2:9092 ,")
			Expect(brokers).To(Equal([]string{"localhost:9092", "host2:9092"}))
		})
	})
})
