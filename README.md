# Reddit History Scraper

Fetch Reddit comments from the PullPush API and parse ban bet data into structured CSV format.

## Usage

```bash
sh get_reddit_comments.sh -o OUTPUT.csv [OPTIONS]
```

### Required Flags

- `-o, --output FILE` - Output CSV filename

### Optional Flags

- `-u, --user USER` - Reddit username (default: VisualMod)
- `-s, --subreddit SUB` - Filter by subreddit (default: wallstreetbets)
- `-p, --pattern PATTERN` - Filter by text pattern (default: "ban bet")
- `-n, --number NUM` - Maximum number of comments to retrieve
- `-h, --help` - Display help message

### Output Format

The script parses ban bet comments and outputs a CSV with the following columns:

- `created_utc` - Comment timestamp
- `ticker` - Stock symbol
- `from_price` - Starting price
- `to_price` - Target price
- `price_change_percent` - Percentage change from starting to target price
- `period_number` - Duration number (e.g., "2" from "within 2 weeks")
- `period_timeframe` - Duration unit (e.g., "weeks", "days", "hours")
- `bet_status` - "won" or "lost" (created bets are excluded)
- `user` - Reddit username who made the bet
- `win_percentage` - Win ratio as integer percentage
- `win_count` - User's total wins
- `loss_count` - User's total losses

## Requirements

- bash
- jq
- curl
- awk
- sed

All utilities must work on both macOS and Linux.

## License

MIT License - see LICENSE file for details
