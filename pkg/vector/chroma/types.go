package chroma

// chromaCollection represents a Chroma collection response.
type chromaCollection struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// chromaAddRequest is the request body for adding documents.
type chromaAddRequest struct {
	IDs        []string         `json:"ids"`
	Embeddings [][]float32      `json:"embeddings"`
	Metadatas  []map[string]any `json:"metadatas,omitempty"`
	Documents  []string         `json:"documents,omitempty"`
}

// chromaQueryRequest is the request body for querying.
type chromaQueryRequest struct {
	QueryEmbeddings [][]float32 `json:"query_embeddings"`
	NResults        int         `json:"n_results"`
	Include         []string    `json:"include"`
}

// chromaQueryResponse is the response from a query.
type chromaQueryResponse struct {
	IDs        [][]string         `json:"ids"`
	Distances  [][]float32        `json:"distances"`
	Metadatas  [][]map[string]any `json:"metadatas"`
	Embeddings [][][]float32      `json:"embeddings"`
}

// chromaGetRequest is the request body for getting documents.
type chromaGetRequest struct {
	IDs     []string `json:"ids"`
	Include []string `json:"include"`
}

// chromaGetResponse is the response from getting documents.
type chromaGetResponse struct {
	IDs        []string         `json:"ids"`
	Metadatas  []map[string]any `json:"metadatas"`
	Embeddings [][]float32      `json:"embeddings"`
}

// chromaDeleteRequest is the request body for deleting documents.
type chromaDeleteRequest struct {
	IDs []string `json:"ids"`
}
