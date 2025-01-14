package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
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

func processZoneEpoch(db ethdb.Reader, zoneName string, start, end uint64, results chan<- MinerCount) error {
	minerCounts := make(map[string]int)
	processedBlocks := 0
	emptyHashes := 0
	nilBlocks := 0

	log.Printf("[%s] Starting block processing from %d to %d", zoneName, start, end)

	for i := start; i <= end; i++ {
		hash := rawdb.ReadCanonicalHash(db, i)
		if hash == (common.Hash{}) {
			emptyHashes++
			if i%10000 == 0 {
				log.Printf("[%s] Block %d: Empty hash encountered", zoneName, i)
			}
			// block is empty, stop here
			break
		}

		// Modify the block processing part in processZoneEpoch:
		block := rawdb.ReadBlock(db, hash, i)
		if block == nil {
			nilBlocks++
			if i%10000 == 0 {
				log.Printf("[%s] Block %d: Nil block encountered", zoneName, i)
			}
			continue
		}

		// Add detailed block information
		coinbase := block.Coinbase().Hex()
		minerCounts[coinbase]++
		processedBlocks++

		// Print block details
		if i%1000 == 0 { // Changed from 10000 to show more blocks
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

			// Print first transaction hash if exists
			if len(block.Transactions()) > 0 {
				log.Printf("  First Tx Hash: %s", block.Transactions()[0].Hash().Hex())
			}

			log.Printf("  Size: %d bytes", block.Size())
			log.Printf("  Difficulty: %v", block.Difficulty())
		}
	}

	log.Printf("[%s] Zone Processing Complete:", zoneName)
	log.Printf("[%s] Total blocks processed: %d", zoneName, processedBlocks)
	log.Printf("[%s] Total empty hashes encountered: %d", zoneName, emptyHashes)
	log.Printf("[%s] Total nil blocks encountered: %d", zoneName, nilBlocks)
	log.Printf("[%s] Unique miners found: %d", zoneName, len(minerCounts))

	// Print top 5 miners by blocks mined
	type minerStat struct {
		address string
		count   int
	}
	var minerStats []minerStat
	for addr, count := range minerCounts {
		minerStats = append(minerStats, minerStat{addr, count})
	}
	sort.Slice(minerStats, func(i, j int) bool {
		return minerStats[i].count > minerStats[j].count
	})

	log.Printf("[%s] Top 5 miners:", zoneName)
	for i := 0; i < len(minerStats) && i < 5; i++ {
		log.Printf("[%s] %d. Address: %s, Blocks: %d",
			zoneName, i+1, minerStats[i].address, minerStats[i].count)
	}

	// Send results through channel
	for addr, count := range minerCounts {
		results <- MinerCount{
			Address: addr,
			Zone:    zoneName,
			Count:   count,
		}
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

	// Find all zone directories
	zones, err := findZones(rootDir)
	if err != nil {
		log.Fatalf("Error finding zones: %v", err)
	}

	log.Printf("Found %d zones", len(zones))
	for _, zone := range zones {
		log.Printf("Zone found: %s", zone)
	}

	// Channel for collecting results from all goroutines
	resultsEpoch1 := make(chan MinerCount, 1000)
	resultsEpoch2 := make(chan MinerCount, 1000)

	var wg sync.WaitGroup

	// Process each zone
	for _, zonePath := range zones {
		// Extract zone name from path (the directory name above 'quai')
		zoneName := filepath.Base(filepath.Dir(filepath.Dir(filepath.Dir(zonePath))))

		// Open the database for this zone
		db, err := openDatabase(zonePath)
		if err != nil {
			log.Printf("Error opening database for zone %s: %v", zoneName, err)
			continue
		}

		wg.Add(2)

		// Process epoch 1
		go func(db ethdb.Reader, zone string) {
			defer wg.Done()
			if err := processZoneEpoch(db, zone, epoch1Start, epoch1End, resultsEpoch1); err != nil {
				log.Printf("Error processing epoch 1 for zone %s: %v", zone, err)
			}
		}(db, zoneName)

		// Process epoch 2
		go func(db ethdb.Reader, zone string) {
			defer wg.Done()
			if err := processZoneEpoch(db, zone, epoch2Start, epoch2End, resultsEpoch2); err != nil {
				log.Printf("Error processing epoch 2 for zone %s: %v", zone, err)
			}
		}(db, zoneName)

		if closer, ok := db.(interface{ Close() error }); ok {
			defer closer.Close()
		}
	}

	// Wait for all processing to complete in a separate goroutine
	go func() {
		wg.Wait()
		close(resultsEpoch1)
		close(resultsEpoch2)
	}()

	// Collect results
	var epoch1Results []MinerCount
	var epoch2Results []MinerCount

	// Collect epoch 1 results
	for result := range resultsEpoch1 {
		epoch1Results = append(epoch1Results, result)
	}

	// Collect epoch 2 results
	for result := range resultsEpoch2 {
		epoch2Results = append(epoch2Results, result)
	}

	// Write results to files
	if err := writeResults(epoch1Results, "epoch1_miners.csv"); err != nil {
		log.Printf("Error writing epoch 1 results: %v", err)
	}

	if err := writeResults(epoch2Results, "epoch2_miners.csv"); err != nil {
		log.Printf("Error writing epoch 2 results: %v", err)
	}

	fmt.Println("Analysis complete. Results written to epoch1_miners.csv and epoch2_miners.csv")
}
