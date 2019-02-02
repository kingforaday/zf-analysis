package comparse

import (
	"bufio"
	"compress/gzip"
	"log"
	"os"
	"sort"
	"strings"
)

func sortFunc(domains *map[string]struct{}) (sd *[]string) {
	// sort domains
	sortedDomains := make([]string, len(*domains))
	i := 0
	for domain := range *domains {
		sortedDomains[i] = domain
		i++
	}
	sort.Strings(sortedDomains)
	return &sortedDomains
}

func writeResults(gzw *gzip.Writer, domains *map[string]struct{}) {
	sortedDomains := sortFunc(domains)
	for _, k := range *sortedDomains {
		gzw.Write([]byte(k + ".com\n"))
	}
}

func Parse(filepath string) (soa string, count uint) {
	stream, err := os.Open(filepath)
	if err != nil {
		log.Printf("ERR: %s not found; skipping", filepath)
		return "---", uint(0)
	}
	defer stream.Close()

	gz, err := gzip.NewReader(stream)
	if err != nil {
		log.Fatal(err)
	}
	defer gz.Close()

	outputFile, err := os.Create(strings.TrimSuffix(filepath, ".gz") + "_domains.gz")
	if err != nil {
		log.Fatal(err)
	}

	gzw := gzip.NewWriter(outputFile)
	defer gzw.Close()

	domains := make(map[string]struct{})
	len_domains := 0

	scanner := bufio.NewScanner(gz)
	line_count := 0

	for scanner.Scan() {
		if line_count > 50000000 { // 50M
			// sort & store
			writeResults(gzw, &domains)
			len_domains = len_domains + len(domains)

			// clear map
			// compiler optimizes as of Go 1.11+
			for k := range domains {
				delete(domains, k)
			}
			//reset
			line_count = 0
		}
		tokens := strings.Split(scanner.Text(), " ")
		if len(tokens) > 2 && len(tokens[0]) > 0 && (strings.ToLower(tokens[1]) == "ns" || strings.ToLower(tokens[1]) == "a") {
			domains[strings.ToLower(tokens[0])] = struct{}{}
		}
		line_count++
	}
	// sort & store final
	writeResults(gzw, &domains)
	len_domains = len_domains + len(domains)
	return "com.", uint(len_domains)
}
