package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/dominant-strategies/go-quai/common"
	"github.com/dominant-strategies/go-quai/core/rawdb"
	"github.com/dominant-strategies/go-quai/ethdb"
	"github.com/dominant-strategies/go-quai/ethdb/leveldb"
)

const (
	epoch1Start = 1
	epoch1End   = 600000
	epoch2Start = 600001
	epoch2End   = 1200000
)

type MinerCount struct {
	Address string
	Zone    string
	Count   int
}

type BlockStats struct {
	TotalChecked uint64
	EmptyHashes  uint64
	NilBlocks    uint64
	Successful   uint64
}

type GlobalStats struct {
	TotalChecked   uint64
	TotalErrors    uint64
	TotalSuccesses uint64
}

func (gs *GlobalStats) AddChecked(n uint64) {
	atomic.AddUint64(&gs.TotalChecked, n)
}

func (gs *GlobalStats) AddErrors(n uint64) {
	atomic.AddUint64(&gs.TotalErrors, n)
}

func (gs *GlobalStats) AddSuccesses(n uint64) {
	atomic.AddUint64(&gs.TotalSuccesses, n)
}

// DatabaseReader wraps leveldb.Database to implement ethdb.Reader
type DatabaseReader struct {
	db *leveldb.Database
}

// Has retrieves if a key is present in the key-value data store.
func (dr *DatabaseReader) Has(key []byte) (bool, error) {
	return dr.db.Has(key)
}

// Get retrieves the given key if it's present in the key-value data store.
func (dr *DatabaseReader) Get(key []byte) ([]byte, error) {
	return dr.db.Get(key)
}

// Ancient returns an error as we don't support ancient data
func (dr *DatabaseReader) Ancient(kind string, number uint64) ([]byte, error) {
	return nil, fmt.Errorf("ancient not supported")
}

// Ancients returns the length of the ancient data
func (dr *DatabaseReader) Ancients() (uint64, error) {
	return 0, nil
}

// AncientRange returns an error as we don't support ancient data
func (dr *DatabaseReader) AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error) {
	return nil, fmt.Errorf("ancient range not supported")
}

// HasAncient returns if ancient data exists
func (dr *DatabaseReader) HasAncient(kind string, number uint64) (bool, error) {
	return false, nil
}

// AncientSize returns the size of the ancient data
func (dr *DatabaseReader) AncientSize(kind string) (uint64, error) {
	return 0, nil
}

// Tail returns the oldest available ancient number, or nil if no ancients are available
func (dr *DatabaseReader) Tail() (uint64, error) {
	return 0, nil
}

func openDatabase(path string) (ethdb.Reader, error) {
	log.Printf("Attempting to open database at: %s", path)
	db, err := leveldb.New(path, 0, 0, "", false)
	if err != nil {
		log.Printf("Error opening database: %v", err)
		return nil, fmt.Errorf("failed to open database: %v", err)
	}
	log.Printf("Successfully opened database at: %s", path)

	// Try to read some keys to understand the database structure
	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	count := 0
	log.Printf("Sampling first 10 keys in database:")
	for iter.Next() && count < 10 {
		key := iter.Key()
		log.Printf("Found key: %x (length: %d)", key, len(key))
		count++
	}

	if err := iter.Error(); err != nil {
		log.Printf("Error iterating database: %v", err)
	}

	return &DatabaseReader{db: db}, nil
}

func (dr *DatabaseReader) Close() error {
	return dr.db.Close()
}

func findZones(rootDir string) ([]string, error) {
	var zones []string

	// List all zone directories in the root
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// For each zone directory, look for quai/chaindata
		zonePath := filepath.Join(rootDir, entry.Name(), "quai", "chaindata")
		if _, err := os.Stat(zonePath); err == nil {
			// Check if directory has .ldb files
			files, err := os.ReadDir(zonePath)
			if err != nil {
				continue
			}

			hasLDB := false
			for _, file := range files {
				if filepath.Ext(file.Name()) == ".ldb" {
					hasLDB = true
					break
				}
			}

			if hasLDB {
				zones = append(zones, zonePath)
				log.Printf("Found zone chaindata: %s", zonePath)
			}
		}
	}

	return zones, nil
}

func processZoneEpoch(db ethdb.Reader, zoneName string, start, end uint64, results chan<- MinerCount, globalStats *GlobalStats) error {
	minerCounts := make(map[string]int)
	stats := BlockStats{}

	log.Printf("[%s] Starting block processing from %d to %d", zoneName, start, end)

	for i := start; i <= end; i++ {
		atomic.AddUint64(&stats.TotalChecked, 1)
		globalStats.AddChecked(1)

		hash := rawdb.ReadCanonicalHash(db, i)
		if hash == (common.Hash{}) {
			atomic.AddUint64(&stats.EmptyHashes, 1)
			globalStats.AddErrors(1)
			if i%10000 == 0 {
				log.Printf("[%s] Block %d: Empty hash encountered", zoneName, i)
			}
			continue
		}

		block := rawdb.ReadBlock(db, hash, i)
		if block == nil {
			atomic.AddUint64(&stats.NilBlocks, 1)
			globalStats.AddErrors(1)
			if i%10000 == 0 {
				log.Printf("[%s] Block %d: Nil block encountered", zoneName, i)
			}
			continue
		}

		atomic.AddUint64(&stats.Successful, 1)
		globalStats.AddSuccesses(1)

		coinbase := block.Coinbase().Hex()
		minerCounts[coinbase]++

		if i%1000 == 0 {
			log.Printf("[%s] Block %d Details:", zoneName, i)
			log.Printf("  Hash: %s", hash.Hex())
			log.Printf("  Miner: %s", coinbase)
			log.Printf("  Timestamp: %v", block.Time())
			log.Printf("  Number: %v", block.Number())
			log.Printf("  Parent Hash: %v", block.ParentHash().Hex())
			log.Printf("  Root: %v", block.Root().Hex())
			log.Printf("  GasLimit: %v", block.GasLimit())
			log.Printf("  GasUsed: %v", block.GasUsed())
			log.Printf("  Transaction Count: %d", len(block.Transactions()))

			if len(block.Transactions()) > 0 {
				log.Printf("  First Tx Hash: %s", block.Transactions()[0].Hash().Hex())
			}

			log.Printf("  Size: %d bytes", block.Size())
			log.Printf("  Difficulty: %v", block.Difficulty())

			// Add current statistics
			log.Printf("  Current Stats - Checked: %d, Successful: %d, Errors: %d",
				atomic.LoadUint64(&stats.TotalChecked),
				atomic.LoadUint64(&stats.Successful),
				atomic.LoadUint64(&stats.EmptyHashes)+atomic.LoadUint64(&stats.NilBlocks))
		}
	}

	log.Printf("\n[%s] Zone Processing Complete:", zoneName)
	log.Printf("Total Blocks Checked: %d", atomic.LoadUint64(&stats.TotalChecked))
	log.Printf("Successfully Processed: %d", atomic.LoadUint64(&stats.Successful))
	log.Printf("Empty Hash Errors: %d", atomic.LoadUint64(&stats.EmptyHashes))
	log.Printf("Nil Block Errors: %d", atomic.LoadUint64(&stats.NilBlocks))
	if stats.TotalChecked > 0 {
		successRate := float64(stats.Successful) / float64(stats.TotalChecked) * 100
		log.Printf("Success Rate: %.2f%%", successRate)
	}

	return nil
}

func writeResults(results []MinerCount, outputFile string) error {
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"Zone", "Miner Address", "Blocks Mined"}); err != nil {
		return fmt.Errorf("failed to write CSV header: %v", err)
	}

	// Write data
	for _, result := range results {
		if err := writer.Write([]string{result.Zone, result.Address, fmt.Sprintf("%d", result.Count)}); err != nil {
			return fmt.Errorf("failed to write CSV row: %v", err)
		}
	}

	return nil
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: ./blocks-mined <root-directory>")
	}
	rootDir := os.Args[1]

	zones, err := findZones(rootDir)
	if err != nil {
		log.Fatalf("Error finding zones: %v", err)
	}

	log.Printf("Found %d zones", len(zones))
	for _, zone := range zones {
		log.Printf("Zone found: %s", zone)
	}

	resultsEpoch1 := make(chan MinerCount, 1000)
	resultsEpoch2 := make(chan MinerCount, 1000)

	var wg sync.WaitGroup
	globalStats := &GlobalStats{}

	for _, zonePath := range zones {
		zoneName := filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(zonePath))))

		db, err := openDatabase(zonePath)
		if err != nil {
			log.Printf("Error opening database for zone %s: %v", zoneName, err)
			continue
		}

		wg.Add(2)

		go func(db ethdb.Reader, zone string) {
			defer wg.Done()
			if err := processZoneEpoch(db, zone, epoch1Start, epoch1End, resultsEpoch1, globalStats); err != nil {
				log.Printf("Error processing epoch 1 for zone %s: %v", zone, err)
			}
		}(db, zoneName)

		go func(db ethdb.Reader, zone string) {
			defer wg.Done()
			if err := processZoneEpoch(db, zone, epoch2Start, epoch2End, resultsEpoch2, globalStats); err != nil {
				log.Printf("Error processing epoch 2 for zone %s: %v", zone, err)
			}
		}(db, zoneName)

		if closer, ok := db.(interface{ Close() error }); ok {
			defer closer.Close()
		}
	}

	go func() {
		wg.Wait()
		close(resultsEpoch1)
		close(resultsEpoch2)
	}()

	var epoch1Results []MinerCount
	var epoch2Results []MinerCount

	for result := range resultsEpoch1 {
		epoch1Results = append(epoch1Results, result)
	}

	for result := range resultsEpoch2 {
		epoch2Results = append(epoch2Results, result)
	}

	log.Printf("\nGLOBAL STATISTICS:")
	log.Printf("Total Blocks Checked: %d", atomic.LoadUint64(&globalStats.TotalChecked))
	log.Printf("Successfully Processed: %d", atomic.LoadUint64(&globalStats.TotalSuccesses))
	log.Printf("Total Errors: %d", atomic.LoadUint64(&globalStats.TotalErrors))
	if globalStats.TotalChecked > 0 {
		successRate := float64(globalStats.TotalSuccesses) / float64(globalStats.TotalChecked) * 100
		log.Printf("Overall Success Rate: %.2f%%", successRate)
	}

	if err := writeResults(epoch1Results, "epoch1_miners.csv"); err != nil {
		log.Printf("Error writing epoch 1 results: %v", err)
	}

	if err := writeResults(epoch2Results, "epoch2_miners.csv"); err != nil {
		log.Printf("Error writing epoch 2 results: %v", err)
	}

	fmt.Println("Analysis complete. Results written to epoch1_miners.csv and epoch2_miners.csv")
}
