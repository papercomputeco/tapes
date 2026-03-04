package utils

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("truncate", func() {
	It("returns the string unchanged when within the limit", func() {
		Expect(Truncate("short", 10)).To(Equal("short"))
	})

	It("returns the string unchanged when exactly at the limit", func() {
		Expect(Truncate("12345", 5)).To(Equal("12345"))
	})

	It("truncates with ellipsis when over the limit", func() {
		result := Truncate("this is a long string", 10)
		Expect(result).To(Equal("this is a ..."))
	})
})

var _ = Describe("ExtractTextFromContent", func() {
	It("returns empty with an empty slice", func() {
		emptySlice := []map[string]any{}
		result := ExtractTextFromContent(emptySlice)
		Expect(result).To(Equal(""))
	})

	It("returns empty with an irrelevant slice", func() {
		functionCall := map[string]string {"name": "fetch", "arguments": "{\"url\": \"https://allrecipes.com/top-5-italian\"}"}
		irrelevantSlice := []map[string]any{
			{"type": "image_url", "image_url": "data:image/png;ibVOR..."},
			{"type": "function", "function": functionCall},
		}
		result := ExtractTextFromContent(irrelevantSlice)
		Expect(result).To(Equal(""))
	})

	It("returns the expected content with matching content blocks", func() {
		msg1 := "I need a recipe for chicken carbonara"
		msg2 := "<system-message>: User has an egg allergy, ensure recipes have documented substitutions."
		contentBlocks := []map[string]any{
			{"type": "text", "text": msg1},
			{"type": "text", "text": msg2},
		}
		result := ExtractTextFromContent(contentBlocks)
		Expect(result).To(ContainSubstring(msg1))
		Expect(result).To(ContainSubstring(msg2))
	})

	It("returns the expected content with mixed content blocks", func() {
		imgContent := "data:image/png;ibVOR..."
		textContent := "What's wrong with this picture"
		mixedBlocks := []map[string]any{
			{"type": "text", "text": textContent},
			{"type": "image_url", "image_url": imgContent},
		}
		result := ExtractTextFromContent(mixedBlocks)
		Expect(result).To(ContainSubstring(textContent))
		Expect(result).ToNot(ContainSubstring(imgContent))
	})
})
