package main

import (
	"fmt"
	"html/template"
	"os"
	"sort"
	"strings"
	"time"

	tzfs "github.com/temporalio/temporal-zfs/pkg/fs"
	"github.com/temporalio/temporal-zfs/pkg/store"
)

// ReportData is the top-level data structure for the HTML report template.
type ReportData struct {
	GeneratedAt  string
	TotalWFs     int
	TotalFiles   int
	TotalSnaps   int
	TotalBytes   string
	TotalRetries int
	Workflows    []ReportWorkflow
}

// ReportWorkflow describes one workflow's filesystem state for the report.
type ReportWorkflow struct {
	TopicName  string
	TopicSlug  string
	Files      []ReportFile
	Snapshots  []ReportSnapshot
	FileCount  int
	TotalBytes int64
	Retries    int
	Status     string // "completed", "failed"
}

// ReportFile represents a file in a workflow's filesystem.
type ReportFile struct {
	Path    string
	Size    int64
	Content string
}

// ReportSnapshot represents a snapshot.
type ReportSnapshot struct {
	Name  string
	Files []string
}

func generateHTMLReport(ds *DemoStore, outputPath string) error {
	manifest, err := ds.LoadManifest()
	if err != nil {
		return fmt.Errorf("load manifest: %w", err)
	}

	var data ReportData
	data.GeneratedAt = time.Now().Format(time.RFC3339)

	for _, entry := range manifest {
		s := store.NewPrefixedStore(ds.Base(), entry.PartitionID)
		f, err := tzfs.Open(s)
		if err != nil {
			continue // skip broken partitions
		}

		status := "completed"
		if entry.Failed {
			status = "failed"
		}
		wf := ReportWorkflow{
			TopicName: entry.TopicName,
			TopicSlug: entry.TopicSlug,
			Retries:   entry.Retries,
			Status:    status,
		}

		// Collect files.
		wf.Files = collectFiles(f, "/research/"+entry.TopicSlug)
		wf.FileCount = len(wf.Files)
		for _, file := range wf.Files {
			wf.TotalBytes += file.Size
		}

		// Collect snapshots.
		snapshots, err := f.ListSnapshots()
		if err == nil {
			for _, snap := range snapshots {
				rs := ReportSnapshot{Name: snap.Name}
				snapFS, err := f.OpenSnapshot(snap.Name)
				if err == nil {
					rs.Files = collectFilePaths(snapFS, "/research/"+entry.TopicSlug)
					_ = snapFS.Close()
				}
				wf.Snapshots = append(wf.Snapshots, rs)
			}
		}

		data.Workflows = append(data.Workflows, wf)
		data.TotalFiles += wf.FileCount
		data.TotalSnaps += len(wf.Snapshots)
		data.TotalRetries += wf.Retries
		data.TotalBytes = humanBytes(int64(totalBytesAll(data.Workflows)))

		_ = f.Close()
	}

	data.TotalWFs = len(data.Workflows)
	data.TotalBytes = humanBytes(int64(totalBytesAll(data.Workflows)))

	// Sort by topic name.
	sort.Slice(data.Workflows, func(i, j int) bool {
		return data.Workflows[i].TopicName < data.Workflows[j].TopicName
	})

	return writeHTMLReport(data, outputPath)
}

func totalBytesAll(wfs []ReportWorkflow) int64 {
	var total int64
	for _, wf := range wfs {
		total += wf.TotalBytes
	}
	return total
}

func collectFiles(f *tzfs.FS, dir string) []ReportFile {
	var files []ReportFile
	entries, err := f.ReadDir(dir)
	if err != nil {
		return files
	}
	for _, e := range entries {
		path := dir + "/" + e.Name
		if e.Type == tzfs.InodeTypeDir {
			files = append(files, collectFiles(f, path)...)
		} else {
			data, err := f.ReadFile(path)
			if err != nil {
				continue
			}
			content := string(data)
			if len(content) > 2000 {
				content = content[:2000] + "\n... (truncated)"
			}
			files = append(files, ReportFile{
				Path:    path,
				Size:    int64(len(data)),
				Content: content,
			})
		}
	}
	return files
}

func collectFilePaths(f *tzfs.FS, dir string) []string {
	var paths []string
	entries, err := f.ReadDir(dir)
	if err != nil {
		return paths
	}
	for _, e := range entries {
		path := dir + "/" + e.Name
		if e.Type == tzfs.InodeTypeDir {
			paths = append(paths, collectFilePaths(f, path)...)
		} else {
			paths = append(paths, path)
		}
	}
	return paths
}

func writeHTMLReport(data ReportData, outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer f.Close()
	return reportTemplate.Execute(f, data)
}

var reportTemplate = template.Must(template.New("report").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>TemporalFS Demo Report</title>
<style>
  :root { --bg: #0d1117; --card: #161b22; --border: #30363d; --text: #c9d1d9;
    --accent: #58a6ff; --green: #3fb950; --yellow: #d29922; --red: #f85149; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif; padding: 20px; }
  h1 { color: var(--accent); margin-bottom: 8px; font-size: 1.8em; }
  h2 { color: var(--accent); margin: 24px 0 12px; font-size: 1.3em; }
  .subtitle { color: #8b949e; margin-bottom: 24px; }
  .stats { display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }
  .stat-card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px 24px; min-width: 150px; }
  .stat-value { font-size: 2em; font-weight: 700; color: var(--accent); }
  .stat-label { color: #8b949e; font-size: 0.85em; margin-top: 4px; }
  table { width: 100%; border-collapse: collapse; background: var(--card); border-radius: 8px; overflow: hidden; margin-bottom: 24px; }
  th { background: #1c2128; text-align: left; padding: 10px 16px; font-size: 0.85em; color: #8b949e; border-bottom: 1px solid var(--border); }
  td { padding: 10px 16px; border-bottom: 1px solid var(--border); }
  tr:hover { background: #1c2128; }
  details { background: var(--card); border: 1px solid var(--border); border-radius: 8px; margin-bottom: 8px; }
  summary { padding: 12px 16px; cursor: pointer; font-weight: 600; }
  summary:hover { background: #1c2128; }
  .detail-body { padding: 0 16px 16px; }
  .snapshot-section { margin-top: 12px; }
  .snapshot-name { color: var(--green); font-weight: 600; }
  .file-list { list-style: none; padding-left: 16px; }
  .file-list li { padding: 2px 0; font-family: monospace; font-size: 0.9em; }
  .file-content { background: var(--bg); border: 1px solid var(--border); border-radius: 4px; padding: 12px; margin-top: 8px; white-space: pre-wrap; font-family: monospace; font-size: 0.8em; max-height: 300px; overflow-y: auto; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75em; font-weight: 600; }
  .badge-green { background: rgba(63,185,80,0.2); color: var(--green); }
  .badge-yellow { background: rgba(210,153,34,0.2); color: var(--yellow); }
  .badge-red { background: rgba(248,81,73,0.2); color: var(--red); }
  .badge-blue { background: rgba(88,166,255,0.2); color: var(--accent); }
  footer { margin-top: 40px; text-align: center; color: #484f58; font-size: 0.85em; }
</style>
</head>
<body>
<h1>TemporalFS Research Agent Demo</h1>
<p class="subtitle">Generated {{.GeneratedAt}}</p>

<div class="stats">
  <div class="stat-card"><div class="stat-value">{{.TotalWFs}}</div><div class="stat-label">Workflows</div></div>
  <div class="stat-card"><div class="stat-value">{{.TotalFiles}}</div><div class="stat-label">Files Created</div></div>
  <div class="stat-card"><div class="stat-value">{{.TotalSnaps}}</div><div class="stat-label">Snapshots</div></div>
  <div class="stat-card"><div class="stat-value">{{.TotalRetries}}</div><div class="stat-label">Retries Survived</div></div>
  <div class="stat-card"><div class="stat-value">{{.TotalBytes}}</div><div class="stat-label">Data Written</div></div>
</div>

<h2>Workflow Summary</h2>
<table>
  <thead><tr><th>Topic</th><th>Files</th><th>Size</th><th>Snapshots</th><th>Retries</th><th>Status</th></tr></thead>
  <tbody>
  {{range .Workflows}}
  <tr>
    <td>{{.TopicName}}</td>
    <td>{{.FileCount}}</td>
    <td>{{.TotalBytes}} B</td>
    <td><span class="badge badge-green">{{len .Snapshots}} snapshots</span></td>
    <td>{{if .Retries}}<span class="badge badge-yellow">{{.Retries}} retries</span>{{else}}<span class="badge badge-green">0</span>{{end}}</td>
    <td>{{if eq .Status "failed"}}<span class="badge badge-red">failed</span>{{else}}<span class="badge badge-green">completed</span>{{end}}</td>
  </tr>
  {{end}}
  </tbody>
</table>

<h2>Filesystem Explorer</h2>
{{range .Workflows}}
<details>
  <summary>{{.TopicName}} <span class="badge badge-blue">{{.FileCount}} files</span></summary>
  <div class="detail-body">
    {{range .Snapshots}}
    <div class="snapshot-section">
      <div class="snapshot-name">📸 {{.Name}}</div>
      <ul class="file-list">
        {{range .Files}}<li>📄 {{.}}</li>{{end}}
      </ul>
    </div>
    {{end}}
    <h3 style="margin-top:16px;color:#c9d1d9;">Files (Final State)</h3>
    {{range .Files}}
    <details style="margin:4px 0;border:none;background:transparent;">
      <summary style="padding:4px 0;font-family:monospace;font-size:0.9em;">📄 {{.Path}} ({{.Size}} B)</summary>
      <div class="file-content">{{.Content}}</div>
    </details>
    {{end}}
  </div>
</details>
{{end}}

<footer>Powered by TemporalFS — Durable Filesystem for AI Agent Workflows</footer>
</body>
</html>`))

// browseWorkflow prints the directory tree of a specific workflow's filesystem.
func browseWorkflow(ds *DemoStore, topicSlug string) {
	manifest, err := ds.LoadManifest()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load manifest: %v\n", err)
		os.Exit(1)
	}

	var entry *ManifestEntry
	for i := range manifest {
		if manifest[i].TopicSlug == topicSlug {
			entry = &manifest[i]
			break
		}
	}
	if entry == nil {
		fmt.Fprintf(os.Stderr, "Topic %q not found. Available topics:\n", topicSlug)
		for _, m := range manifest {
			fmt.Fprintf(os.Stderr, "  %s\n", m.TopicSlug)
		}
		os.Exit(1)
	}

	s := store.NewPrefixedStore(ds.Base(), entry.PartitionID)
	f, err := tzfs.Open(s)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open filesystem: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()

	fmt.Printf("%s%s=== %s ===%s\n\n", colorBold, colorCyan, entry.TopicName, colorReset)

	// Print directory tree.
	printTree(f, "/", "")

	// Print snapshots.
	snapshots, err := f.ListSnapshots()
	if err == nil && len(snapshots) > 0 {
		fmt.Printf("\n%sSnapshots:%s\n", colorBold, colorReset)
		for _, snap := range snapshots {
			fmt.Printf("  %s📸 %s%s\n", colorGreen, snap.Name, colorReset)
		}
	}
}

func printTree(f *tzfs.FS, dir string, indent string) {
	entries, err := f.ReadDir(dir)
	if err != nil {
		return
	}

	for i, e := range entries {
		isLast := i == len(entries)-1
		connector := "├── "
		if isLast {
			connector = "└── "
		}

		if e.Type == tzfs.InodeTypeDir {
			fmt.Printf("%s%s%s📁 %s%s\n", indent, connector, colorYellow, e.Name, colorReset)
			childIndent := indent + "│   "
			if isLast {
				childIndent = indent + "    "
			}
			subdir := dir
			if !strings.HasSuffix(dir, "/") {
				subdir += "/"
			}
			printTree(f, subdir+e.Name, childIndent)
		} else {
			info, _ := f.Stat(dir + "/" + e.Name)
			size := ""
			if info != nil {
				size = fmt.Sprintf(" (%s)", humanBytes(int64(info.Size)))
			}
			fmt.Printf("%s%s📄 %s%s%s\n", indent, connector, e.Name, colorDim+size, colorReset)
		}
	}
}
