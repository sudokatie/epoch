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

	// Version flag
	versionFlag := flag.Bool("version", false, "Print version")
	flag.BoolVar(versionFlag, "v", false, "Print version (shorthand)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "epoch - Time series database\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  epoch <command> [options]\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  shell    Start interactive query shell\n")
		fmt.Fprintf(os.Stderr, "  server   Start the database server\n")
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
		if err := runServer(*serverBind, *serverData); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	case "help", "-h", "--help":
		flag.Usage()

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		flag.Usage()
		os.Exit(1)
	}
}

func runServer(bind, dataDir string) error {
	fmt.Printf("Starting epoch server on %s (data: %s)\n", bind, dataDir)
	// TODO: implement server startup
	// For now, just print and exit
	fmt.Println("Server mode not yet implemented. Use the HTTP server package directly.")
	return nil
}
