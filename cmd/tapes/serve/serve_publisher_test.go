package servecmder

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	kafkapublisher "github.com/papercomputeco/tapes/pkg/publisher/kafka"
)

var _ = Describe("serve publisher config", func() {
	Describe("flags", func() {
		It("exposes the same kafka publisher flags as the proxy subcommand", func() {
			cmd := NewServeCmd()

			Expect(cmd.Flags().Lookup("kafka-brokers")).NotTo(BeNil())
			Expect(cmd.Flags().Lookup("kafka-client-id")).NotTo(BeNil())
			Expect(cmd.Flags().Lookup("kafka-topic")).NotTo(BeNil())
		})

		It("binds kafka flag values before validating publisher config", func() {
			cmd := NewServeCmd()
			cmd.SilenceUsage = true
			cmd.Flags().Bool("debug", false, "")
			cmd.SetArgs([]string{"--kafka-brokers", "localhost:9092"})

			Expect(cmd.Execute()).To(MatchError("kafka topic is required when kafka brokers are set"))
		})
	})

	Describe("validatePublisherConfig", func() {
		It("accepts missing kafka config and disables publishing", func() {
			cmder := &ServeCommander{}

			Expect(cmder.validatePublisherConfig()).To(Succeed())
		})

		It("rejects kafka topic without brokers", func() {
			cmder := &ServeCommander{
				kafkaTopic: "tapes.nodes.v1",
			}

			Expect(cmder.validatePublisherConfig()).To(MatchError(
				"kafka brokers are required when kafka topic is set",
			))
		})

		It("rejects kafka brokers without topic", func() {
			cmder := &ServeCommander{
				kafkaBrokers: "localhost:9092",
			}

			Expect(cmder.validatePublisherConfig()).To(MatchError(
				"kafka topic is required when kafka brokers are set",
			))
		})
	})

	Describe("newPublisher", func() {
		It("returns nil publisher when kafka config is missing", func() {
			cmder := &ServeCommander{}

			pub, err := cmder.newPublisher()
			Expect(err).NotTo(HaveOccurred())
			Expect(pub).To(BeNil())
		})

		It("creates a kafka publisher when minimum kafka config is provided", func() {
			cmder := &ServeCommander{
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
