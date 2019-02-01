package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/cheggaaa/pb"
	"zf-analysis/zoneparse"
)

var (
	loadDone  = make(chan bool)
	inputChan = make(chan string)
	work      sync.WaitGroup
	zones     []ZoneInfo

	directory = flag.String("directory", "", "directory with zone files")
	verbose   = flag.Bool("verbose", false, "enable verbose logging")
	pbar      = flag.Bool("progress", false, "enable progress bar")
	parallel  = flag.Uint("parallel", 2, "number of zones to process in parallel")
)

type ZoneInfo struct {
	SOA   string
	Count uint
}

func v(format string, v ...interface{}) {
	if *verbose {
		log.Printf(format, v...)
	}
}

func checkFlags() {
	flag.Parse()
	if len(*directory) == 0 {
		log.Printf("must pass directory (e.g. /data/domains/2019/02/01/)")
		goto FlagError
	}
	if *parallel < 1 {
		log.Printf("parallel must be positive")
		goto FlagError
	}
	return

FlagError:
	flag.PrintDefaults()
	os.Exit(1)
}

func loadFilesToProcess(files []string) {
	for _, file := range files {
		work.Add(1)
		inputChan <- file
	}
	loadDone <- true
}

func worker(bar *pb.ProgressBar) {
	for {
		file, more := <-inputChan
		if more {
			if *pbar {
				bar.Increment()
			} else {
				log.Printf("Processing zone %s", file)
			}
			makeDomainsFile(file)
			work.Done()
		} else {
			// done
			return
		}
	}
}

func makeDomainsFile(zonefile string) {
	stream, err := os.Open(zonefile)
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	gz, err := gzip.NewReader(stream)
	if err != nil {
		log.Fatal(err)
	}
	defer gz.Close()

	var record zoneparse.Record
	scanner := zoneparse.NewScanner(gz)

	stuff := make(map[string]struct{})

	var zone ZoneInfo
	for {
		err := scanner.Next(&record)
		if err != nil {
			break
		}

		v("a '%s' Record for domain/subdomain '%s'\n",
			record.Type,
			record.DomainName,
		)
		if fmt.Sprintf("%s", record.Type) == "SOA" {
			zone.SOA = record.DomainName
		}
		stuff[strings.TrimRight(record.DomainName, ".")] = struct{}{}
	}
	zone.Count = uint(len(stuff))
	zones = append(zones, zone)
	outputFile, err := os.Create(strings.TrimSuffix(zonefile, ".gz") + "_domains.gz")
	if err != nil {
		log.Fatal(err)
	}

	gzw := gzip.NewWriter(outputFile)
	defer gzw.Close()

	for elem := range stuff {
		_, _ = gzw.Write([]byte(elem + "\n"))
	}
	stuff = nil
	// Yes, forcing gc locks program, but worth the time delay for memory save.
	// some zone file can be quite large.
	runtime.GC()
}

func main() {
	checkFlags()

	matches, err := filepath.Glob(*directory + "*.txt.gz")
	if err != nil {
		log.Fatal(err)
	}
	bar := pb.New(len(matches))
	if *pbar {
		bar.Start()
	}
	go loadFilesToProcess(matches)
	v("starting %d parallel processing", *parallel)
	for i := uint(0); i < *parallel; i++ {
		go worker(bar)
	}
	<-loadDone
	work.Wait()

	for _, zone := range zones {
		log.Printf("SOA: %20s\tNum.Domains: %d", zone.SOA, zone.Count)
	}
}
