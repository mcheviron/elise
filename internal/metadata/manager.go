package metadata

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/fsnotify/fsnotify"
)

// Manager watches the metadata log file and provides a cached Cluster view.
type Manager struct {
	path    string
	watcher *fsnotify.Watcher

	stopCh    chan struct{}
	stoppedCh chan struct{}

	cluster  atomic.Pointer[Cluster]
	reloadMu sync.Mutex
}

// NewManager constructs a Manager that watches the provided metadata log path.
func NewManager(path string) (*Manager, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("metadata: create watcher: %w", err)
	}

	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); err != nil {
		watcher.Close()
		return nil, fmt.Errorf("metadata: stat directory %q: %w", dir, err)
	}

	if err := watcher.Add(dir); err != nil {
		watcher.Close()
		return nil, fmt.Errorf("metadata: watch directory %q: %w", dir, err)
	}

	initial, err := Load(path)
	if err != nil {
		watcher.Close()
		return nil, fmt.Errorf("metadata: initial load: %w", err)
	}

	mgr := &Manager{
		path:      filepath.Clean(path),
		watcher:   watcher,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
	mgr.cluster.Store(initial)

	go mgr.run()

	return mgr, nil
}

// Close stops the watcher and frees associated resources.
func (m *Manager) Close() error {
	select {
	case <-m.stopCh:
		// already closed
	default:
		close(m.stopCh)
	}
	<-m.stoppedCh
	return nil
}

// Cluster returns the most recent metadata snapshot.
func (m *Manager) Cluster() *Cluster {
	if m == nil {
		return nil
	}
	if cluster := m.cluster.Load(); cluster != nil {
		return cluster
	}
	return nil
}

func (m *Manager) run() {
	defer close(m.stoppedCh)
	defer m.watcher.Close()

	for {
		select {
		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}
			if m.shouldReload(event) {
				m.reload()
			}
		case err, ok := <-m.watcher.Errors:
			if !ok {
				return
			}
			log.Printf("metadata: watcher error: %v", err)
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) shouldReload(event fsnotify.Event) bool {
	if event.Name == "" {
		return false
	}
	base := filepath.Base(m.path)
	eventBase := filepath.Base(event.Name)
	if base != eventBase {
		return false
	}

	if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Remove|fsnotify.Rename) != 0 {
		return true
	}
	return false
}

func (m *Manager) reload() {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()

	cluster, err := Load(m.path)
	if err != nil {
		log.Printf("metadata: reload failed: %v", err)
		return
	}
	m.cluster.Store(cluster)
}
