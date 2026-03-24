package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	version = "0.1.0"
)

func main() {
	// Subcommands
	shellCmd := flag.NewFlagSet("shell", flag.ExitOnError)
	shellHost := shellCmd.String("host", "localhost:8086", "Server host:port")
	shellDatabase := shellCmd.String("database", "", "Database to use")
	shellFormat := shellCmd.String("format", "table", "Output format (table, json, csv)")
	shellPrecision := shellCmd.String("precision", "ns", "Timestamp precision (ns, us, ms, s)")

	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverBind := serverCmd.String("bind", ":8086", "Bind address")
	serverData := serverCmd.String("data", "./data", "Data directory")
	serverConfig := serverCmd.String("config", "", "Config file path")

	importCmd := flag.NewFlagSet("import", flag.ExitOnError)
	importHost := importCmd.String("host", "localhost:8086", "Server host:port")
	importFile := importCmd.String("file", "", "File to import (required)")
	importDB := importCmd.String("db", "", "Database name (required)")
	importMeasurement := importCmd.String("measurement", "", "Measurement name (for CSV)")
	importFormat := importCmd.String("format", "auto", "Input format (auto, line, json, csv)")
	importBatch := importCmd.Int("batch", 5000, "Batch size for writes")
	importTags := importCmd.String("tags", "", "Tag columns for CSV (comma-separated)")

	exportCmd := flag.NewFlagSet("export", flag.ExitOnError)
	exportHost := exportCmd.String("host", "localhost:8086", "Server host:port")
	exportDB := exportCmd.String("db", "", "Database name (required)")
	exportMeasurement := exportCmd.String("measurement", "", "Measurement to export (optional)")
	exportStart := exportCmd.String("start", "", "Start time (RFC3339 or Unix timestamp)")
	exportEnd := exportCmd.String("end", "", "End time (RFC3339 or Unix timestamp)")
	exportFormat := exportCmd.String("format", "line", "Output format (line, json, csv)")
	exportOutput := exportCmd.String("output", "-", "Output file (- for stdout)")

	clusterCmd := flag.NewFlagSet("cluster", flag.ExitOnError)

	// Version flag
	versionFlag := flag.Bool("version", false, "Print version")
	flag.BoolVar(versionFlag, "v", false, "Print version (shorthand)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "epoch - Time series database\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  epoch <command> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  shell     Start interactive query shell\n")
		fmt.Fprintf(os.Stderr, "  server    Start the database server\n")
		fmt.Fprintf(os.Stderr, "  import    Import data from file\n")
		fmt.Fprintf(os.Stderr, "  export    Export data to file\n")
		fmt.Fprintf(os.Stderr, "  cluster   Cluster management commands\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *versionFlag {
		fmt.Printf("epoch version %s\n", version)
		return
	}

	if len(os.Args) < 2 {
		// Default to shell mode
		shell := NewShell(ShellConfig{
			Host:      "localhost:8086",
			Format:    "table",
			Precision: "ns",
		})
		shell.Run()
		return
	}

	switch os.Args[1] {
	case "shell":
		shellCmd.Parse(os.Args[2:])
		shell := NewShell(ShellConfig{
			Host:      *shellHost,
			Database:  *shellDatabase,
			Format:    *shellFormat,
			Precision: *shellPrecision,
		})
		shell.Run()

	case "server":
		serverCmd.Parse(os.Args[2:])
		if err := runServer(*serverBind, *serverData, *serverConfig); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "import":
		importCmd.Parse(os.Args[2:])
		if *importFile == "" || *importDB == "" {
			fmt.Fprintln(os.Stderr, "Error: --file and --db are required")
			importCmd.Usage()
			os.Exit(1)
		}
		cfg := ImportConfig{
			Host:        *importHost,
			File:        *importFile,
			Database:    *importDB,
			Measurement: *importMeasurement,
			Format:      *importFormat,
			BatchSize:   *importBatch,
			TagColumns:  *importTags,
		}
		if err := runImport(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "export":
		exportCmd.Parse(os.Args[2:])
		if *exportDB == "" {
			fmt.Fprintln(os.Stderr, "Error: --db is required")
			exportCmd.Usage()
			os.Exit(1)
		}
		cfg := ExportConfig{
			Host:        *exportHost,
			Database:    *exportDB,
			Measurement: *exportMeasurement,
			StartTime:   *exportStart,
			EndTime:     *exportEnd,
			Format:      *exportFormat,
			Output:      *exportOutput,
		}
		if err := runExport(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "cluster":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: epoch cluster <subcommand>")
			fmt.Fprintln(os.Stderr, "\nSubcommands:")
			fmt.Fprintln(os.Stderr, "  status      Show cluster status")
			fmt.Fprintln(os.Stderr, "  add-node    Add a node to the cluster")
			fmt.Fprintln(os.Stderr, "  remove-node Remove a node from the cluster")
			fmt.Fprintln(os.Stderr, "  rebalance   Rebalance data across nodes")
			os.Exit(1)
		}
		clusterCmd.Parse(os.Args[3:])
		runCluster(os.Args[2], clusterCmd.Args())

	case "help", "-h", "--help":
		flag.Usage()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		flag.Usage()
		os.Exit(1)
	}
}

func runServer(bind, dataDir, configPath string) error {
	fmt.Printf("Starting epoch server on %s (data: %s)\n", bind, dataDir)
	if configPath != "" {
		fmt.Printf("Using config: %s\n", configPath)
	}
	// TODO: implement server startup
	// For now, just print and exit
	fmt.Println("Server mode not yet fully implemented. Use the HTTP server package directly.")
	return nil
}
