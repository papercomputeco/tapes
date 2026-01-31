package sqlitevec_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"

	"github.com/papercomputeco/tapes/pkg/vector"
	"github.com/papercomputeco/tapes/pkg/vector/sqlitevec"
)

var _ = Describe("SQLiteVecDriver", func() {
	var logger *zap.Logger

	BeforeEach(func() {
		logger = zap.NewNop()
	})

	Describe("NewSQLiteVecDriver", func() {
		It("should return an error when DBPath is empty", func() {
			_, err := sqlitevec.NewSQLiteVecDriver(sqlitevec.Config{DBPath: ""}, logger)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("database path is required"))
		})

		It("should create a driver with an in-memory database", func() {
			driver, err := sqlitevec.NewSQLiteVecDriver(sqlitevec.Config{
				DBPath:     ":memory:",
				Dimensions: 4,
			}, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(driver).NotTo(BeNil())
			Expect(driver.Close()).To(Succeed())
		})

		It("should error when dimension not specified", func() {
			_, err := sqlitevec.NewSQLiteVecDriver(sqlitevec.Config{
				DBPath: ":memory:",
			}, logger)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Interface compliance", func() {
		It("should implement vector.Driver interface", func() {
			var _ vector.Driver = (*sqlitevec.SQLiteVecDriver)(nil)
		})
	})

	Describe("Add", func() {
		var driver *sqlitevec.SQLiteVecDriver

		BeforeEach(func() {
			var err error
			driver, err = sqlitevec.NewSQLiteVecDriver(sqlitevec.Config{
				DBPath:     ":memory:",
				Dimensions: 4,
			}, logger)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(driver.Close()).To(Succeed())
		})

		It("should do nothing when given empty docs", func() {
			err := driver.Add(context.Background(), []vector.Document{})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should add a single document", func() {
			docs := []vector.Document{
				{
					ID:        "doc-1",
					Hash:      "hash-1",
					Embedding: []float32{0.1, 0.2, 0.3, 0.4},
				},
			}

			err := driver.Add(context.Background(), docs)
			Expect(err).NotTo(HaveOccurred())

			// Verify it was stored
			retrieved, err := driver.Get(context.Background(), []string{"doc-1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved).To(HaveLen(1))
			Expect(retrieved[0].ID).To(Equal("doc-1"))
			Expect(retrieved[0].Hash).To(Equal("hash-1"))
		})

		It("should add multiple documents", func() {
			docs := []vector.Document{
				{ID: "doc-1", Hash: "hash-1", Embedding: []float32{0.1, 0.1, 0.1, 0.1}},
				{ID: "doc-2", Hash: "hash-2", Embedding: []float32{0.2, 0.2, 0.2, 0.2}},
				{ID: "doc-3", Hash: "hash-3", Embedding: []float32{0.3, 0.3, 0.3, 0.3}},
			}

			err := driver.Add(context.Background(), docs)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := driver.Get(context.Background(), []string{"doc-1", "doc-2", "doc-3"})
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved).To(HaveLen(3))
		})

		It("should update an existing document", func() {
			docs := []vector.Document{
				{ID: "doc-1", Hash: "hash-1", Embedding: []float32{0.1, 0.1, 0.1, 0.1}},
			}
			err := driver.Add(context.Background(), docs)
			Expect(err).NotTo(HaveOccurred())

			// Update with new hash and embedding
			updatedDocs := []vector.Document{
				{ID: "doc-1", Hash: "hash-1-updated", Embedding: []float32{0.9, 0.9, 0.9, 0.9}},
			}
			err = driver.Add(context.Background(), updatedDocs)
			Expect(err).NotTo(HaveOccurred())

			retrieved, err := driver.Get(context.Background(), []string{"doc-1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(retrieved).To(HaveLen(1))
			Expect(retrieved[0].Hash).To(Equal("hash-1-updated"))
		})
	})

	Describe("Query", func() {
		var driver *sqlitevec.SQLiteVecDriver

		BeforeEach(func() {
			var err error
			driver, err = sqlitevec.NewSQLiteVecDriver(sqlitevec.Config{
				DBPath:     ":memory:",
				Dimensions: 4,
			}, logger)
			Expect(err).NotTo(HaveOccurred())

			// Insert test data
			docs := []vector.Document{
				{ID: "doc-1", Hash: "hash-1", Embedding: []float32{0.1, 0.1, 0.1, 0.1}},
				{ID: "doc-2", Hash: "hash-2", Embedding: []float32{0.2, 0.2, 0.2, 0.2}},
				{ID: "doc-3", Hash: "hash-3", Embedding: []float32{0.3, 0.3, 0.3, 0.3}},
				{ID: "doc-4", Hash: "hash-4", Embedding: []float32{0.4, 0.4, 0.4, 0.4}},
				{ID: "doc-5", Hash: "hash-5", Embedding: []float32{0.5, 0.5, 0.5, 0.5}},
			}
			err = driver.Add(context.Background(), docs)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(driver.Close()).To(Succeed())
		})

		It("should return the closest documents", func() {
			queryVec := []float32{0.3, 0.3, 0.3, 0.3}
			results, err := driver.Query(context.Background(), queryVec, 3)
			Expect(err).NotTo(HaveOccurred())
			Expect(results).To(HaveLen(3))

			// The closest document to [0.3, 0.3, 0.3, 0.3] should be doc-3
			Expect(results[0].ID).To(Equal("doc-3"))
			Expect(results[0].Hash).To(Equal("hash-3"))
		})

		It("should respect topK limit", func() {
			queryVec := []float32{0.3, 0.3, 0.3, 0.3}
			results, err := driver.Query(context.Background(), queryVec, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(results).To(HaveLen(2))
		})

		It("should default topK to 10 when zero or negative", func() {
			queryVec := []float32{0.3, 0.3, 0.3, 0.3}
			results, err := driver.Query(context.Background(), queryVec, 0)
			Expect(err).NotTo(HaveOccurred())
			// We only have 5 documents, so we should get 5 back
			Expect(results).To(HaveLen(5))
		})

		It("should return similarity scores in descending order", func() {
			queryVec := []float32{0.3, 0.3, 0.3, 0.3}
			results, err := driver.Query(context.Background(), queryVec, 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(results).To(HaveLen(5))

			// Scores should be in descending order (closest first = highest score)
			for i := 1; i < len(results); i++ {
				Expect(results[i-1].Score).To(BeNumerically(">=", results[i].Score))
			}
		})
	})

	Describe("Get", func() {
		var driver *sqlitevec.SQLiteVecDriver

		BeforeEach(func() {
			var err error
			driver, err = sqlitevec.NewSQLiteVecDriver(sqlitevec.Config{
				DBPath:     ":memory:",
				Dimensions: 4,
			}, logger)
			Expect(err).NotTo(HaveOccurred())

			docs := []vector.Document{
				{ID: "doc-1", Hash: "hash-1", Embedding: []float32{0.1, 0.2, 0.3, 0.4}},
				{ID: "doc-2", Hash: "hash-2", Embedding: []float32{0.5, 0.6, 0.7, 0.8}},
			}
			err = driver.Add(context.Background(), docs)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(driver.Close()).To(Succeed())
		})

		It("should return nil for empty IDs", func() {
			docs, err := driver.Get(context.Background(), []string{})
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(BeNil())
		})

		It("should retrieve documents by IDs", func() {
			docs, err := driver.Get(context.Background(), []string{"doc-1", "doc-2"})
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(2))
		})

		It("should return embeddings with retrieved documents", func() {
			docs, err := driver.Get(context.Background(), []string{"doc-1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))
			Expect(docs[0].Embedding).To(HaveLen(4))
			Expect(docs[0].Embedding[0]).To(BeNumerically("~", 0.1, 0.001))
			Expect(docs[0].Embedding[1]).To(BeNumerically("~", 0.2, 0.001))
			Expect(docs[0].Embedding[2]).To(BeNumerically("~", 0.3, 0.001))
			Expect(docs[0].Embedding[3]).To(BeNumerically("~", 0.4, 0.001))
		})

		It("should skip non-existent IDs", func() {
			docs, err := driver.Get(context.Background(), []string{"doc-1", "nonexistent"})
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))
			Expect(docs[0].ID).To(Equal("doc-1"))
		})
	})

	Describe("Delete", func() {
		var driver *sqlitevec.SQLiteVecDriver

		BeforeEach(func() {
			var err error
			driver, err = sqlitevec.NewSQLiteVecDriver(sqlitevec.Config{
				DBPath:     ":memory:",
				Dimensions: 4,
			}, logger)
			Expect(err).NotTo(HaveOccurred())

			docs := []vector.Document{
				{ID: "doc-1", Hash: "hash-1", Embedding: []float32{0.1, 0.1, 0.1, 0.1}},
				{ID: "doc-2", Hash: "hash-2", Embedding: []float32{0.2, 0.2, 0.2, 0.2}},
				{ID: "doc-3", Hash: "hash-3", Embedding: []float32{0.3, 0.3, 0.3, 0.3}},
			}
			err = driver.Add(context.Background(), docs)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			Expect(driver.Close()).To(Succeed())
		})

		It("should do nothing when given empty IDs", func() {
			err := driver.Delete(context.Background(), []string{})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should delete a single document", func() {
			err := driver.Delete(context.Background(), []string{"doc-1"})
			Expect(err).NotTo(HaveOccurred())

			docs, err := driver.Get(context.Background(), []string{"doc-1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(BeEmpty())

			// Other documents should still exist
			docs, err = driver.Get(context.Background(), []string{"doc-2", "doc-3"})
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(2))
		})

		It("should delete multiple documents", func() {
			err := driver.Delete(context.Background(), []string{"doc-1", "doc-2"})
			Expect(err).NotTo(HaveOccurred())

			docs, err := driver.Get(context.Background(), []string{"doc-1", "doc-2", "doc-3"})
			Expect(err).NotTo(HaveOccurred())
			Expect(docs).To(HaveLen(1))
			Expect(docs[0].ID).To(Equal("doc-3"))
		})

		It("should not error when deleting non-existent IDs", func() {
			err := driver.Delete(context.Background(), []string{"nonexistent"})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should remove documents from query results after deletion", func() {
			err := driver.Delete(context.Background(), []string{"doc-3"})
			Expect(err).NotTo(HaveOccurred())

			queryVec := []float32{0.3, 0.3, 0.3, 0.3}
			results, err := driver.Query(context.Background(), queryVec, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(results).To(HaveLen(2))

			for _, result := range results {
				Expect(result.ID).NotTo(Equal("doc-3"))
			}
		})
	})

	Describe("Close", func() {
		It("should close the database connection", func() {
			driver, err := sqlitevec.NewSQLiteVecDriver(sqlitevec.Config{
				DBPath:     ":memory:",
				Dimensions: 4,
			}, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(driver.Close()).To(Succeed())
		})
	})
})
