package agentui

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/grasskode/bramha/internal/journal"
)

// DefaultLiveAfter is the mtime-freshness threshold for tagging a
// worker log as live in the logs view. Picked to bracket one
// orchestrator tick: anything written this recently is almost
// certainly still being appended to.
const DefaultLiveAfter = 30 * time.Second

// DefaultTailBytes is the maximum trailing slice of a log file read
// into the logs view. Tail starts at file end minus this many bytes;
// a partial leading line is trimmed.
const DefaultTailBytes int64 = 8 * 1024

// LogEntry is one worker-invocation log file the logs view knows about,
// enriched with the task id resolved from the journal for that worker.
type LogEntry struct {
	Pool     string    // pipeline name (subdir under workers/)
	WorkerID string    // file basename without .log
	TaskID   string    // most-recent task_id seen in the matching journal file; "" if unknown
	Path     string    // absolute path of the .log file
	ModTime  time.Time // file mtime
	Size     int64
	Live     bool // true when ModTime is within liveAfter of now
}

// DiscoverLogs enumerates <state>/workers/<pool>/<id>.log files and
// pairs each with the latest task_id from <state>/journal/<pool>/<id>.jsonl
// (best effort: a missing or unreadable journal file just leaves
// TaskID empty). Sorted: live first, then by ModTime descending so
// the freshest activity is always at the top.
func DiscoverLogs(stateDir string, liveAfter time.Duration) []LogEntry {
	if liveAfter <= 0 {
		liveAfter = DefaultLiveAfter
	}
	root := filepath.Join(stateDir, "workers")
	pools, err := os.ReadDir(root)
	if err != nil {
		return nil
	}
	now := time.Now()
	var out []LogEntry
	for _, pe := range pools {
		if !pe.IsDir() {
			continue
		}
		pool := pe.Name()
		files, err := os.ReadDir(filepath.Join(root, pool))
		if err != nil {
			continue
		}
		for _, fe := range files {
			name := fe.Name()
			if fe.IsDir() || !strings.HasSuffix(name, ".log") {
				continue
			}
			workerID := strings.TrimSuffix(name, ".log")
			path := filepath.Join(root, pool, name)
			info, err := fe.Info()
			if err != nil {
				continue
			}
			out = append(out, LogEntry{
				Pool:     pool,
				WorkerID: workerID,
				TaskID:   latestTaskID(stateDir, pool, workerID),
				Path:     path,
				ModTime:  info.ModTime(),
				Size:     info.Size(),
				Live:     now.Sub(info.ModTime()) < liveAfter,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Live != out[j].Live {
			return out[i].Live
		}
		return out[i].ModTime.After(out[j].ModTime)
	})
	return out
}

// latestTaskID returns the task_id from the most-recent journal event
// for this worker, or "" if the journal file is missing/empty/malformed.
// The journal is one event per line; we scan top-to-bottom keeping the
// last decoded TaskID since every event for a single worker invocation
// carries the same task_id once it claims one.
func latestTaskID(stateDir, pool, workerID string) string {
	path := journal.FileFor(stateDir, pool, workerID)
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var last string
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// Cheap field probe — avoid pulling the full journal package's
		// JSON decoder for every line.
		const key = `"task_id":"`
		i := strings.Index(string(line), key)
		if i < 0 {
			continue
		}
		rest := string(line[i+len(key):])
		end := strings.IndexByte(rest, '"')
		if end < 0 {
			continue
		}
		last = rest[:end]
	}
	return last
}

// TailFile reads up to maxBytes from the end of path and returns the
// content as a string. When the file is larger than maxBytes, the
// leading partial line is trimmed so the result starts on a line
// boundary. Missing file is not an error — returns "".
func TailFile(path string, maxBytes int64) (string, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultTailBytes
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	start := int64(0)
	if info.Size() > maxBytes {
		start = info.Size() - maxBytes
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return "", err
	}
	buf, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	if start > 0 {
		if nl := bytes.IndexByte(buf, '\n'); nl >= 0 && nl < len(buf)-1 {
			buf = buf[nl+1:]
		}
	}
	return string(buf), nil
}
