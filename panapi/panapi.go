package panapi


// Not sure if we need these

type Pversion int
type Ppath string

type Pan interface {
	// Client facing ZK API from paper

	// Type of flags
	Create(path Ppath, data string, flags int)

	Delete(path Ppath, version Pversion)

	// Watches could be set by reading from a client given channel? Sus bc client can close channel
	Exists(path Ppath, watch chan int) (bool)

	GetData(path Ppath, watch chan int) (string)

	SetData(path Ppath, data string, version Pversion)

	GetChildren(path Ppath, watch chan int) []Ppath

	Sync(path Ppath)
}
