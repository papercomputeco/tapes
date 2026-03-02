package proxycmder

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/papercomputeco/tapes/pkg/publisher"
)

var _ = Describe("proxyCommander publisher config", func() {
	Describe("validatePublisherConfig", func() {
		It("accepts nop publisher without kafka config", func() {
			cmder := &proxyCommander{publisherType: publisherTypeNop}

			Expect(cmder.validatePublisherConfig()).To(Succeed())
		})

		It("rejects kafka publisher without brokers", func() {
			cmder := &proxyCommander{
				publisherType: publisherTypeKafka,
				kafkaTopic:    "tapes.nodes.v1",
			}

			Expect(cmder.validatePublisherConfig()).To(MatchError(
				"kafka brokers are required when --publisher=kafka",
			))
		})

		It("rejects kafka publisher without topic", func() {
			cmder := &proxyCommander{
				publisherType: publisherTypeKafka,
				kafkaBrokers:  "localhost:9092",
			}

			Expect(cmder.validatePublisherConfig()).To(MatchError(
				"kafka topic is required when --publisher=kafka",
			))
		})

		It("rejects unknown publisher type", func() {
			cmder := &proxyCommander{publisherType: "unknown"}

			Expect(cmder.validatePublisherConfig()).To(MatchError(
				ContainSubstring("unsupported publisher type"),
			))
		})
	})

	Describe("newPublisher", func() {
		It("creates a nop publisher", func() {
			cmder := &proxyCommander{publisherType: publisherTypeNop}

			pub, err := cmder.newPublisher()
			Expect(err).NotTo(HaveOccurred())
			Expect(pub).NotTo(BeNil())
			_, ok := pub.(*publisher.NopPublisher)
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
