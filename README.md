# sample-parquet-generator

This little project serves as a simple parquet generator for customizable number (from a bunch to millions) of datapoints (time,temperature) as a reference for ingesting in other projects.

Install dependencies using:

```bash
bun install
```

## Usage

Run with default number of rows (10,000):

```bash
bun run index.ts
```

Specify custom number of rows:

```bash
bun run index.ts --rows 1000000
# or using short alias
bun run index.ts -r 1000000
```

### CLI Options

- `--rows, -r`: Number of rows to generate (default: 10,000)
- `--help`: Show help

## Output

Generates a `temps_gzip.parquet` file with GZIP compression containing:
- `ts`: Timestamp in microseconds, starting from 1 month ago
- `temp`: Temperature values (double) between 20 and 55 degrees

## Performance

The script provides performance metrics:
- Progress percentage during generation
- Total time elapsed
- Rows written per second
