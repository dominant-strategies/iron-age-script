# Quai Iron Age Block Mined Script

A tool to analyze blocks mined across different zones in the Quai Network in Iron Age. 

## Prerequisites

- Go 1.22 or later
- Quai Network chain data

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd blocks-mined
```

2. Ensure you have go-quai in your local environment:
```bash
# Your go-quai should be at ~/code/go-quai
```

3. Install dependencies:
```bash
go mod tidy
```

## Usage

1. Build the binary:
```bash
go build -o blocks-mined
```

2. Run the analysis:
```bash
./blocks-mined /path/to/quai/backup/root
```

The script will:
- Process blocks from epoch 1 (blocks 1-600000)
- Process blocks from epoch 2 (blocks 600001-1200000)
- Generate two CSV files with mining statistics:
  - `epoch1_miners.csv`
  - `epoch2_miners.csv`

## Output Format

The CSV files contain the following columns:
- Zone: The Quai zone name
- Miner Address: The ethereum address that mined the block
- Blocks Mined: Number of blocks mined by that address in that zone

## Directory Structure

```
.
├── main.go           # Main script
├── go.mod           # Go module file
├── go.sum           # Go module checksum
├── README.md        # This file
└── .gitignore       # Git ignore file
```
