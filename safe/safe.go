package safe

import "sync"

//#################
//Thread Safe Types
//#################

/*************
 Safe Boolean
*************/

type SafeBool struct {
	v  bool
	mu *sync.Mutex
}

//New false
func NewBool() *SafeBool {
	return &SafeBool{v: false, mu: &sync.Mutex{}}
}

func (b *SafeBool) SetFalse() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.v = false
}

func (b *SafeBool) SetTrue() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.v = true
}

func (b *SafeBool) Val() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v
}
