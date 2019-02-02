package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/cheggaaa/pb"
	"zf-analysis/zoneparse"
	"zf-analysis/zoneparse/comparse"
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
	// Special case com.zone file
	if strings.Contains(zonefile, "com.zone.gz") {
		soa, count := comparse.Parse(zonefile)
		zones = append(zones, ZoneInfo{
			SOA:   soa,
			Count: count,
		})
		return
	}

	stream, err := os.Open(zonefile)
	if err != nil {
		log.Printf("ERR: %s not found; skipping", zonefile)
		return
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
			if err == io.EOF {
				break
			}
			//log.Println(err)
			continue
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

func writeStatsFile() {
	f, err := os.Create(*directory + "stats")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	for _, zone := range zones {
		f.WriteString(fmt.Sprintf("SOA: %20s\tNum.Domains: %d\n", zone.SOA, zone.Count))
	}
	f.Sync()
}

func main() {
	checkFlags()

	matches, err := filepath.Glob(*directory + "*.txt.gz")
	if err != nil {
		log.Fatal(err)
	}

	// add com and org
	matches = append(matches, []string{*directory + "com.zone.gz", *directory + "org.zone.gz"}...)

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

	writeStatsFile()

}
