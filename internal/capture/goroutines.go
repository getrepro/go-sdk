package capture

import (
	"strings"
	"sync/atomic"
)

type packageSet struct{ names []string }

var applicationPackages atomic.Pointer[packageSet]

func RegisterPackage(name string) {
	for {
		old := applicationPackages.Load()
		next := &packageSet{}
		if old != nil {
			for _, existing := range old.names {
				if existing == name {
					return
				}
			}
			next.names = append(next.names, old.names...)
		}
		next.names = append(next.names, name)
		if applicationPackages.CompareAndSwap(old, next) {
			return
		}
	}
}

type goroutineLease struct {
	scope    *Scope
	released atomic.Bool
}

// Called on the creating user goroutine, before switching to the scheduler
// stack. Allocation and race-instrumented atomics must never run on g0.
func (f *Frame) CloneForGoroutine(caller string) interface{} {
	if f == nil {
		return nil
	}
	copy := *f
	copy.lease = nil
	if f.Scope != nil {
		if packages := applicationPackages.Load(); packages != nil {
			for _, name := range packages.names {
				if strings.HasPrefix(caller, name+".") {
					f.Scope.children.Add(1)
					copy.lease = &goroutineLease{scope: f.Scope}
					break
				}
			}
		}
	}
	return &copy
}

// Done is invoked on the exiting user goroutine, before runtime.goexit1 tears
// down its race context. Driver-owned long-lived goroutines do not have leases.
func (f *Frame) Done() {
	if f == nil {
		return
	}
	lease := f.lease
	if lease == nil || !lease.released.CompareAndSwap(false, true) {
		return
	}
	s := lease.scope
	s.children.Add(-1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed && s.request != nil && s.pending == 0 && s.children.Load() == 0 {
		_ = s.finishLocked()
	}
}
