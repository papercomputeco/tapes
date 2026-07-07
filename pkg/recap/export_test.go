package recap

// ElideMiddle exposes the transcript elision helper to the external test
// package (same idiom as pkg/storage/postgres/export_test.go) — its
// small-maxChars floor guard is unreachable through Generate, whose cap is a
// package constant.
var ElideMiddle = elideMiddle
