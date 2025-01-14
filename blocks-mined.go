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
	Address    string
	Zone       string
	Count      int
	EpochStart uint64
	EpochEnd   uint64
}

type BlockStats struct {
	TotalChecked uint64
	EmptyHashes  uint64
	NilBlocks    uint64
	Successful   uint64
	OutOfRange   uint64
}

type GlobalStats struct {
	TotalChecked   uint64
	TotalErrors    uint64
	TotalSuccesses uint64
	OutOfRange     uint64
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

func (gs *GlobalStats) AddOutOfRange(n uint64) {
	atomic.AddUint64(&gs.OutOfRange, n)
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

	// List all zone directories in the root backup directory
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
				log.Printf("Found zone chaindata: %s (Zone: %s)", zonePath, entry.Name())
			}
		}
	}

	return zones, nil
}

func processZoneEpoch(db ethdb.Reader, zoneName string, start, end uint64, results chan<- MinerCount, globalStats *GlobalStats) error {
	minerCounts := make(map[string]int)
	stats := BlockStats{}

	log.Printf("[%s] Processing epoch from block %d to %d", zoneName, start, end)

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
			break
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

		// Verify block number is in correct range
		blockNum := block.Number().Uint64()
		if blockNum < start || blockNum > end {
			atomic.AddUint64(&stats.OutOfRange, 1)
			globalStats.AddOutOfRange(1)
			if i%10000 == 0 {
				log.Printf("[%s] Block %d outside epoch range %d-%d", zoneName, blockNum, start, end)
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
			log.Printf("  Block Number: %v", blockNum)
			log.Printf("  Timestamp: %v", block.Time())
			log.Printf("  Parent Hash: %v", block.ParentHash().Hex())
			log.Printf("  Transaction Count: %d", len(block.Transactions()))

			// Print current stats
			log.Printf("  Processing Statistics:")
			log.Printf("    Checked: %d", atomic.LoadUint64(&stats.TotalChecked))
			log.Printf("    Successful: %d", atomic.LoadUint64(&stats.Successful))
			log.Printf("    Errors: %d", atomic.LoadUint64(&stats.EmptyHashes)+atomic.LoadUint64(&stats.NilBlocks))
			log.Printf("    Out of Range: %d", atomic.LoadUint64(&stats.OutOfRange))
		}
	}

	// Print final statistics for this epoch
	log.Printf("\n[%s] Epoch %d-%d Processing Complete:", zoneName, start, end)
	log.Printf("  Total Blocks Checked: %d", atomic.LoadUint64(&stats.TotalChecked))
	log.Printf("  Successfully Processed: %d", atomic.LoadUint64(&stats.Successful))
	log.Printf("  Empty Hash Errors: %d", atomic.LoadUint64(&stats.EmptyHashes))
	log.Printf("  Nil Block Errors: %d", atomic.LoadUint64(&stats.NilBlocks))
	log.Printf("  Out of Range Blocks: %d", atomic.LoadUint64(&stats.OutOfRange))

	// Send results with epoch information
	for addr, count := range minerCounts {
		results <- MinerCount{
			Address:    addr,
			Zone:       zoneName,
			Count:      count,
			EpochStart: start,
			EpochEnd:   end,
		}
	}

	return nil
}

func writeResults(results []MinerCount, outputFile string) error {
	// Sort results by epoch and count
	sort.Slice(results, func(i, j int) bool {
		if results[i].EpochStart != results[j].EpochStart {
			return results[i].EpochStart < results[j].EpochStart
		}
		return results[i].Count > results[j].Count
	})

	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"Zone", "Miner Address", "Blocks Mined", "Epoch Range"}); err != nil {
		return fmt.Errorf("failed to write CSV header: %v", err)
	}

	// Write data
	for _, result := range results {
		epochRange := fmt.Sprintf("%d-%d", result.EpochStart, result.EpochEnd)
		if err := writer.Write([]string{
			result.Zone,
			result.Address,
			fmt.Sprintf("%d", result.Count),
			epochRange,
		}); err != nil {
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
	globalStats := &GlobalStats{}

	// Process each zone
	for _, zonePath := range zones {
		// Extract zone name from path (the directory name above 'quai')
		zoneName := filepath.Base(filepath.Dir(filepath.Dir(zonePath))) // This gets the zone directory name (prime, region-0, zone-1-1, etc.)
		log.Printf("Processing zone: %s at path: %s", zoneName, zonePath)

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
			log.Printf("[%s] Starting Epoch 1 (%d-%d)", zone, epoch1Start, epoch1End)
			if err := processZoneEpoch(db, zone, epoch1Start, epoch1End, resultsEpoch1, globalStats); err != nil {
				log.Printf("Error processing epoch 1 for zone %s: %v", zone, err)
			}
		}(db, zoneName)

		// Process epoch 2
		go func(db ethdb.Reader, zone string) {
			defer wg.Done()
			log.Printf("[%s] Starting Epoch 2 (%d-%d)", zone, epoch2Start, epoch2End)
			if err := processZoneEpoch(db, zone, epoch2Start, epoch2End, resultsEpoch2, globalStats); err != nil {
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

	log.Println("Collecting results from all zones...")

	// Collect epoch 1 results
	for result := range resultsEpoch1 {
		epoch1Results = append(epoch1Results, result)
	}
	log.Printf("Collected %d miner records for Epoch 1 (%d-%d)",
		len(epoch1Results), epoch1Start, epoch1End)

	// Collect epoch 2 results
	for result := range resultsEpoch2 {
		epoch2Results = append(epoch2Results, result)
	}
	log.Printf("Collected %d miner records for Epoch 2 (%d-%d)",
		len(epoch2Results), epoch2Start, epoch2End)

	// Print final global statistics
	log.Printf("\nFINAL GLOBAL STATISTICS:")
	log.Printf("Total Blocks Checked: %d", atomic.LoadUint64(&globalStats.TotalChecked))
	log.Printf("Successfully Processed: %d", atomic.LoadUint64(&globalStats.TotalSuccesses))
	log.Printf("Total Errors: %d", atomic.LoadUint64(&globalStats.TotalErrors))
	log.Printf("Out of Range Blocks: %d", atomic.LoadUint64(&globalStats.OutOfRange))

	if globalStats.TotalChecked > 0 {
		successRate := float64(globalStats.TotalSuccesses) / float64(globalStats.TotalChecked) * 100
		log.Printf("Overall Success Rate: %.2f%%", successRate)
	}

	// Write results to files
	log.Println("Writing results to CSV files...")

	if err := writeResults(epoch1Results, "epoch1_miners.csv"); err != nil {
		log.Printf("Error writing epoch 1 results: %v", err)
	} else {
		log.Printf("Successfully wrote epoch1_miners.csv with %d records", len(epoch1Results))
	}

	if err := writeResults(epoch2Results, "epoch2_miners.csv"); err != nil {
		log.Printf("Error writing epoch 2 results: %v", err)
	} else {
		log.Printf("Successfully wrote epoch2_miners.csv with %d records", len(epoch2Results))
	}

	log.Println("Analysis complete!")
}
