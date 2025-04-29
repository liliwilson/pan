package panapi

// Not sure if we need these

type Pversion int
type Ppath string
type Flag struct {
	Ephemeral  bool
	Sequential bool
}

type Pan interface {
	// Client facing ZK API from paper

	// Type of flags
	Create(path Ppath, data string, flags Flag)

	Delete(path Ppath, version Pversion)

	// Watches block (for now........)
	Exists(path Ppath, watch bool) bool

	GetData(path Ppath, watch bool) string

	SetData(path Ppath, data string, version Pversion)

	GetChildren(path Ppath, watch bool) []Ppath

	Sync(path Ppath)
}
