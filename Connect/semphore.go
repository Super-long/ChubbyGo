package Connect

type Empty interface {}
type Semaphore chan Empty

// acquire n resources
func (s Semaphore) P(n int) {
	e := new(Empty)
	for i := 0; i < n; i++ {
		s <- e
	}
}

// release n resources
func (s Semaphore) V(n int) {
	if n < 0 {	// 防止一手client_handler.go connectAll中的小问题
		return
	}
	for i := 0; i < n; i++ {
		<-s
	}
}