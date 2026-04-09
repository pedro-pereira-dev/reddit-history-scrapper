#!/bin/sh

################################################################################
# PAGINATION STRATEGY & NOTES
################################################################################
# This script retrieves the COMPLETE comment history for a Reddit user from the
# PullPush (Pushshift-compatible) API. Key design decisions:
#
# 1. BATCH SIZE: Set to 500 (the documented maximum per the Pushshift API).
#    - The API will cap or reject requests for size > 500.
#    - Smaller batch sizes (e.g., 25) would require more API calls and slower.
#
# 2. PAGING BACKWARD IN TIME (DESCENDING):
#    - First batch: Fetch most recent comments (sort=desc, no 'before' param).
#    - Subsequent batches: Use 'before' param set to (min_created_utc - 1)
#      from the previous batch. This ensures backward iteration without gaps.
#    - The API's 'before' parameter means "fetch comments created BEFORE this timestamp".
#    - Subtracting 1 excludes the boundary comment we've already seen.
#
# 3. WHY THIS WORKS:
#    - By using before=<last_min_timestamp - 1>, we exclude the boundary item.
#    - This prevents duplicates and gaps when paginating through history.
#    - The API naturally returns results in descending order (newest first).
#
# 4. LIMIT (-n flag):
#    - When -n is provided, the script stops as soon as the specified number
#      of comments is retrieved, even in the middle of a batch.
#    - This ensures accurate counting for partial extracts.
#
# 5. TERMINATION:
#    - Stops when batch_count < requested size (indicates end of history).
#    - Or when limit (-n) is reached.
#    - Or on API errors.
#
# REFERENCES:
# - Pushshift API Docs: https://github.com/pushshift/api#searching-comments
# - PullPush: https://pullpush.io/
#
################################################################################

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Spinner frames
SPINNER=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")
SPINNER_PID=""

start_spinner() {
    local message="$1"
    local i=0
    while true; do
        printf "\r%b%s %s%b" "$BLUE" "${SPINNER[$((i % 10))]}" "$message" "$NC" >&2
        sleep 0.1
        i=$((i + 1))
    done &
    SPINNER_PID=$!
}

stop_spinner() {
    if [ -n "$SPINNER_PID" ]; then
        kill $SPINNER_PID 2>/dev/null
        wait $SPINNER_PID 2>/dev/null
        printf "\r%80s\r" "" >&2
        SPINNER_PID=""
    fi
}

cleanup() {
    stop_spinner
    printf "\r%80s\r" "" >&2
    exit 130
}

trap cleanup SIGINT SIGTERM

print_help() {
    cat <<EOF
Usage: sh get_reddit_comments.sh [OPTIONS]

Retrieve all comments from a Reddit user via PullPush API and save to CSV.

REQUIRED OPTIONS:
  -o, --output FILE     Output CSV filename

OPTIONS:
  -u, --user USER       Reddit username (default: VisualMod)
  -s, --subreddit SUB   Filter comments by subreddit (default: wallstreetbets)
  -p, --pattern PATTERN Substring pattern to match in comment body (default: "ban bet")
  -n, --number NUM      Maximum number of comments to retrieve (default: all)
  -h, --help            Display this help message

EXAMPLES:
  sh get_reddit_comments.sh -o output.csv
  sh get_reddit_comments.sh -u spez -o spez.csv --subreddit AskReddit
  sh get_reddit_comments.sh -o bans.csv -n 5000

EOF
}

check_dependencies() {
    local missing=0
    
    if ! command -v jq >/dev/null 2>&1; then
        printf "%bERROR: jq is required but not installed%b\n" "$RED" "$NC" >&2
        missing=1
    fi
    
    if ! command -v curl >/dev/null 2>&1; then
        printf "%bERROR: curl is required but not installed%b\n" "$RED" "$NC" >&2
        missing=1
    fi
    
    if [ $missing -eq 1 ]; then
        printf "\nPlease install jq and curl before running this script.\n" >&2
        exit 1
    fi
}

parse_arguments() {
    # Set defaults
    USER="VisualMod"
    SUBREDDIT="wallstreetbets"
    PATTERN="ban bet"
    
    while [ $# -gt 0 ]; do
        case "$1" in
            -u | --user)
                shift
                if [ -z "$1" ]; then
                    printf "%bERROR: --user requires an argument%b\n" "$RED" "$NC" >&2
                    print_help
                    exit 1
                fi
                USER="$1"
                ;;
            -o | --output)
                shift
                if [ -z "$1" ]; then
                    printf "%bERROR: --output requires an argument%b\n" "$RED" "$NC" >&2
                    print_help
                    exit 1
                fi
                OUTPUT_FILE="$1"
                ;;
            -s | --subreddit)
                shift
                if [ -z "$1" ]; then
                    printf "%bERROR: --subreddit requires an argument%b\n" "$RED" "$NC" >&2
                    print_help
                    exit 1
                fi
                SUBREDDIT="$1"
                ;;
            -p | --pattern)
                shift
                if [ -z "$1" ]; then
                    printf "%bERROR: --pattern requires an argument%b\n" "$RED" "$NC" >&2
                    print_help
                    exit 1
                fi
                PATTERN="$1"
                ;;
            -n | --number)
                shift
                if [ -z "$1" ]; then
                    printf "%bERROR: --number requires an argument%b\n" "$RED" "$NC" >&2
                    print_help
                    exit 1
                fi
                LIMIT="$1"
                ;;
            -h | --help)
                print_help
                exit 0
                ;;
            *)
                printf "%bERROR: Unknown option: %s%b\n" "$RED" "$1" "$NC" >&2
                print_help
                exit 1
                ;;
        esac
        shift
    done
    
    if [ -z "$OUTPUT_FILE" ]; then
        printf "%bERROR: --output is required%b\n" "$RED" "$NC" >&2
        print_help
        exit 1
    fi
}

csv_escape_field() {
    local field="$1"
    field=$(printf '%s\n' "$field" | jq -Rs '.' | sed 's/^"//;s/"$//')
    printf '"%s"' "$field"
}

json_to_csv_row() {
    local json_obj="$1"
    local fields="$2"
    local first=1
    
    printf '%s\n' "$fields" | while IFS= read -r field; do
        local value
        value=$(printf '%s\n' "$json_obj" | jq -r ".\"$field\" // \"\"" 2>/dev/null)
        
        if [ $first -eq 1 ]; then
            csv_escape_field "$value"
            first=0
        else
            printf ','
            csv_escape_field "$value"
        fi
    done
}

format_decimal() {
    local value="$1"
    if [ -z "$value" ]; then
        printf ''
    else
        awk -v val="$value" 'BEGIN { printf "%.2f", val }'
    fi
}

format_integer() {
    local value="$1"
    if [ -z "$value" ]; then
        printf ''
    else
        printf '%.0f' "$value"
    fi
}

extract_fields() {
    local json_obj="$1"
    printf '%s\n' "$json_obj" | jq -r 'keys[]' 2>/dev/null | sort
}

parse_ban_bet() {
    local body="$1"
    local created_utc="$2"
    
    # Check if this is a "Ban Bet Won" or "Ban Bet Lost"
    if printf '%s\n' "$body" | grep -q "^#Ban Bet Won"; then
        parse_ban_bet_won "$body" "$created_utc"
    elif printf '%s\n' "$body" | grep -q "^#Ban Bet Lost"; then
        parse_ban_bet_lost "$body" "$created_utc"
    fi
    # Ignore "Ban Bet Created" messages
}

parse_ban_bet_won() {
    local body="$1"
    local created_utc="$2"
    
    # Extract user: /u/USERNAME
    local user
    user=$(printf '%s\n' "$body" | sed -n 's/.*\/u\/\([A-Za-z0-9_\-]*\).*/\1/p' | head -1)
    
    # Extract ticker: uppercase word before "would go"
    local ticker
    ticker=$(printf '%s\n' "$body" | sed -n 's/.*that \([A-Z][A-Z0-9]*\) would go.*/\1/p' | head -1)
    
    # Extract from_price: the price "when it was X"
    local from_price
    from_price=$(printf '%s\n' "$body" | sed -n 's/.*when it was \([0-9.]*\).*/\1/p')
    
    # Extract to_price: the target price before "within"
    local to_price
    to_price=$(printf '%s\n' "$body" | sed -n 's/.*would go to \([0-9.]*\).*/\1/p')
    
     # Extract period: "within N days/weeks/hours/etc" (may be bold like **2 weeks**)
     local period_number period_timeframe
     period_number=$(printf '%s\n' "$body" | sed -n 's/.*within \*\*\([0-9]*\) \([a-z]*\)\*\*.*/\1/p')
     period_timeframe=$(printf '%s\n' "$body" | sed -n 's/.*within \*\*\([0-9]*\) \([a-z]*\)\*\*.*/\2/p')
     
     # If bold extraction failed, try without bold
     if [ -z "$period_number" ]; then
         period_number=$(printf '%s\n' "$body" | sed -n 's/.*within \([0-9]*\) \([a-z]*\).*/\1/p')
         period_timeframe=$(printf '%s\n' "$body" | sed -n 's/.*within \([0-9]*\) \([a-z]*\).*/\2/p')
     fi
    
    # Extract win and loss counts: "record is now X wins and Y losses"
    local win_count loss_count
    win_count=$(printf '%s\n' "$body" | sed -n 's/.*record is now \([0-9]*\) wins.*/\1/p')
    loss_count=$(printf '%s\n' "$body" | sed -n 's/.*and \([0-9]*\) losses.*/\1/p')
    
    # Calculate win_percentage (exactly 2 decimals)
    local win_percentage
    if [ -n "$win_count" ] && [ -n "$loss_count" ] && [ $((win_count + loss_count)) -gt 0 ]; then
        win_percentage=$(awk "BEGIN {printf \"%.2f\", ($win_count * 100 / ($win_count + $loss_count))}")
    else
        win_percentage="0.00"
    fi
    
    # Calculate price_change_percent (exactly 2 decimals)
    local price_change_percent
    if [ -n "$from_price" ] && [ -n "$to_price" ]; then
        price_change_percent=$(awk "BEGIN {printf \"%.2f\", (($to_price - $from_price) / $from_price) * 100}")
    else
        price_change_percent=""
    fi
    
    # Format values for CSV output
    from_price=$(format_decimal "$from_price")
    to_price=$(format_decimal "$to_price")
    period_number=$(format_integer "$period_number")
    win_count=$(format_integer "$win_count")
    loss_count=$(format_integer "$loss_count")
    
    # Output CSV row
    output_csv_row "$created_utc" "won" "$user" "$ticker" "$from_price" "$to_price" "$period_number" "$period_timeframe" "$win_count" "$loss_count" "$win_percentage" "$price_change_percent"
}

parse_ban_bet_lost() {
    local body="$1"
    local created_utc="$2"
    
    # Extract user: /u/USERNAME
    local user
    user=$(printf '%s\n' "$body" | sed -n 's/.*\/u\/\([A-Za-z0-9_\-]*\).*/\1/p' | head -1)
    
    # Extract ticker: uppercase word before "would go"
    local ticker
    ticker=$(printf '%s\n' "$body" | sed -n 's/.*that \([A-Z][A-Z0-9]*\) would go.*/\1/p' | head -1)
    
    # Extract from_price: the price "when it was X"
    local from_price
    from_price=$(printf '%s\n' "$body" | sed -n 's/.*when it was \([0-9.]*\).*/\1/p')
    
    # Extract to_price: the target price before "within"
    local to_price
    to_price=$(printf '%s\n' "$body" | sed -n 's/.*would go to \([0-9.]*\).*/\1/p')
    
    # Extract period: "within N days/weeks/hours/etc" (may be bold like **2 weeks**)
    local period_number period_timeframe
    period_number=$(printf '%s\n' "$body" | sed -n 's/.*within \*\*\([0-9]*\) \([a-z]*\)\*\*.*/\1/p')
    period_timeframe=$(printf '%s\n' "$body" | sed -n 's/.*within \*\*\([0-9]*\) \([a-z]*\)\*\*.*/\2/p')
    
    # If bold extraction failed, try without bold
    if [ -z "$period_number" ]; then
        period_number=$(printf '%s\n' "$body" | sed -n 's/.*within \([0-9]*\) \([a-z]*\).*/\1/p')
        period_timeframe=$(printf '%s\n' "$body" | sed -n 's/.*within \([0-9]*\) \([a-z]*\).*/\2/p')
    fi
    
    # Extract win and loss counts: "record is now X wins and Y losses"
    local win_count loss_count
    win_count=$(printf '%s\n' "$body" | sed -n 's/.*record is now \([0-9]*\) wins.*/\1/p')
    loss_count=$(printf '%s\n' "$body" | sed -n 's/.*and \([0-9]*\) losses.*/\1/p')
    
    # Calculate win_percentage (exactly 2 decimals)
    local win_percentage
    if [ -n "$win_count" ] && [ -n "$loss_count" ] && [ $((win_count + loss_count)) -gt 0 ]; then
        win_percentage=$(awk "BEGIN {printf \"%.2f\", ($win_count * 100 / ($win_count + $loss_count))}")
    else
        win_percentage="0.00"
    fi
    
    # Calculate price_change_percent (exactly 2 decimals)
    local price_change_percent
    if [ -n "$from_price" ] && [ -n "$to_price" ]; then
        price_change_percent=$(awk "BEGIN {printf \"%.2f\", (($to_price - $from_price) / $from_price) * 100}")
    else
        price_change_percent=""
    fi
    
    # Format values for CSV output
    from_price=$(format_decimal "$from_price")
    to_price=$(format_decimal "$to_price")
    period_number=$(format_integer "$period_number")
    win_count=$(format_integer "$win_count")
    loss_count=$(format_integer "$loss_count")
    
    # Output CSV row
    output_csv_row "$created_utc" "lost" "$user" "$ticker" "$from_price" "$to_price" "$period_number" "$period_timeframe" "$win_count" "$loss_count" "$win_percentage" "$price_change_percent"
}

output_csv_row() {
    local created_utc="$1"
    local bet_status="$2"
    local user="$3"
    local ticker="$4"
    local from_price="$5"
    local to_price="$6"
    local period_number="$7"
    local period_timeframe="$8"
    local win_count="$9"
    local loss_count="${10}"
    local win_percentage="${11}"
    local price_change_percent="${12}"
    
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$created_utc" "$ticker" "$from_price" "$to_price" "$price_change_percent" \
        "$period_number" "$period_timeframe" "$bet_status" "$user" "$win_percentage" "$win_count" "$loss_count"
}

make_api_request() {
    local url="$1"
    local retry_count=0
    local backoff=60
    
    while true; do
        sleep 2
        
        local response
        local http_code
        local tmpfile
        
        start_spinner "Fetching data..."
        
        tmpfile=$(mktemp)
        http_code=$(curl -s -w "%{http_code}" -o "$tmpfile" "$url" 2>&1)
        response=$(cat "$tmpfile")
        rm -f "$tmpfile"
        
        stop_spinner
        
        case "$http_code" in
            200)
                printf '%s\n' "$response"
                return 0
                ;;
            429)
                printf "%b[WARN]%b Rate limited. Backing off %ds...\n" "$YELLOW" "$NC" "$backoff" >&2
                sleep "$backoff"
                retry_count=$((retry_count + 1))
                if [ $retry_count -ge 3 ]; then
                    printf "%b[ERROR]%b Too many rate limit retries\n" "$RED" "$NC" >&2
                    return 1
                fi
                backoff=$((backoff * 2))
                ;;
            *)
                printf "%b[ERROR]%b HTTP error %s\n" "$RED" "$NC" "$http_code" >&2
                return 1
                ;;
        esac
    done
}

main() {
     check_dependencies
     parse_arguments "$@"
     
     local api_base="https://api.pullpush.io/reddit/search/comment/"
     # PAGINATION: Set batch_size to 500 (API maximum per Pushshift/PullPush docs).
     # Requesting larger sizes will be capped or rejected by the API.
     local batch_size=500
     local total_comments=0
     local total_lines_written=0
     local header_written=0
     local before=""
     local last_min_created_utc=""
     local start_time
     local end_time
     local elapsed
     
     # Fixed CSV column order for ban bet parsing
     local csv_columns="created_utc,ticker,from_price,to_price,price_change_percent,period_number,period_timeframe,bet_status,user,win_percentage,win_count,loss_count"
     
     start_time=$(date +%s)
     
     # Build start message
     local start_msg="[START] user=$USER, out=$OUTPUT_FILE"
     if [ -n "$SUBREDDIT" ]; then
         start_msg="$start_msg, sub=$SUBREDDIT"
     fi
     if [ -n "$PATTERN" ]; then
         start_msg="$start_msg, pat=$PATTERN"
     fi
     if [ -n "$LIMIT" ]; then
         start_msg="$start_msg, lim=$LIMIT"
     fi
     printf "%b%s%b\n" "$GREEN" "$start_msg" "$NC" >&2
     
     > "$OUTPUT_FILE"
     
     # PAGINATION LOOP: Iterate backward through user's comment history using 'before' parameter.
     # Each iteration fetches up to 500 comments. The 'before' parameter is set to the minimum
     # created_utc from the previous batch to avoid gaps and duplicates.
     while true; do
         local current_batch_size=$batch_size
         
         # LIMIT HANDLING: If -n flag is set, ensure we don't request more than needed.
         if [ -n "$LIMIT" ]; then
             local remaining=$((LIMIT - total_comments))
             if [ "$remaining" -le 0 ]; then
                 printf "%b[INFO]%b Reached limit of %d comments\n" "$BLUE" "$NC" "$LIMIT" >&2
                 break
             fi
             if [ "$remaining" -lt "$batch_size" ]; then
                 current_batch_size=$remaining
             fi
         fi
         
         # Build API URL with pagination parameters.
         # PAGINATION LOGIC:
         # - First batch: sort=desc, no before param (fetch most recent)
         # - Subsequent batches: sort=desc with before=<timestamp_less_than_last>
         #   This ensures we walk backward through time without gaps or duplicates
         local url="${api_base}?author=${USER}&size=${current_batch_size}&sort=desc&sort_type=created_utc"
         if [ -n "$SUBREDDIT" ]; then
             url="${url}&subreddit=${SUBREDDIT}"
         fi
         if [ -n "$PATTERN" ]; then
             url="${url}&q=$(printf '%s\n' "$PATTERN" | jq -sRr @uri)"
         fi
         
         # PAGINATION: On subsequent batches, use before parameter with last_min_timestamp - 1
         # This excludes the boundary comment and continues fetching older comments
         if [ -n "$before" ]; then
             # Subtract 1 from the timestamp to exclude the boundary item
             # Use awk for proper floating-point arithmetic
             before_value=$(printf '%s\n' "$before" | awk '{printf "%.0f", $1 - 1}')
             url="${url}&before=${before_value}"
         fi
         
         printf "%b[DEBUG]%b Fetching from: %s\n" "$BLUE" "$NC" "$url" >&2
         
         local response
         response=$(make_api_request "$url")
         if [ $? -ne 0 ]; then
             printf "%b[ERROR]%b Failed to fetch from API\n" "$RED" "$NC" >&2
             break
         fi
         
         local data
         data=$(printf '%s\n' "$response" | jq '.data' 2>/dev/null)
         if [ -z "$data" ] || [ "$data" = "null" ]; then
             printf "%b[INFO]%b No more data available from API\n" "$BLUE" "$NC" >&2
             break
         fi
         
         local batch_count
         batch_count=$(printf '%s\n' "$data" | jq 'length' 2>/dev/null)
         if [ "$batch_count" -eq 0 ]; then
             printf "%b[INFO]%b End of user comment history reached\n" "$BLUE" "$NC" >&2
             break
         fi
         
         # Write header if not yet written
         if [ $header_written -eq 0 ]; then
             start_spinner "Saving header..."
             printf '%s\n' "$csv_columns" >> "$OUTPUT_FILE"
             stop_spinner
             header_written=1
             total_lines_written=$((total_lines_written + 1))
         fi
         
         # PROCESSING: Write comments from this batch
         start_spinner "Processing and writing batch..."
         
         # Get the minimum created_utc in this batch for next iteration's 'before' param.
         last_min_created_utc=$(printf '%s\n' "$data" | jq -r '.[-1].created_utc // ""' 2>/dev/null)
         
         # Process each comment in the batch.
         printf '%s\n' "$data" | jq -c '.[]' 2>/dev/null | while IFS= read -r comment_json; do
             local created_utc_epoch
             local created_utc_iso
             local body
             
             created_utc_epoch=$(printf '%s\n' "$comment_json" | jq -r '.created_utc // ""' 2>/dev/null)
             body=$(printf '%s\n' "$comment_json" | jq -r '.body // ""' 2>/dev/null)
             
             # Convert Unix timestamp to ISO 8601 format
             if [ -n "$created_utc_epoch" ] && [ "$created_utc_epoch" != "null" ]; then
                 created_utc_iso=$(date -u -f "%s" "$created_utc_epoch" "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null)
                 if [ -z "$created_utc_iso" ]; then
                     # Fallback for systems without -f flag in date
                     created_utc_iso=$(date -u -j -f "%s" "$created_utc_epoch" "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null)
                 fi
             else
                 created_utc_iso=""
             fi
             
             # Parse ban bet data (won/lost only, created bets are ignored)
             parse_ban_bet "$body" "$created_utc_iso" >> "$OUTPUT_FILE"
             total_lines_written=$((total_lines_written + 1))
         done
         stop_spinner
         
         total_comments=$((total_comments + batch_count))
         
         # Print debug samples from this batch
         printf "%b[DEBUG]%b Batch complete: %d comments processed. Samples:\n" "$BLUE" "$NC" "$batch_count" >&2
         printf '%s\n' "$data" | jq -c '.[] | {id: .id, created_utc: .created_utc, body: .body}' 2>/dev/null | head -3 | while IFS= read -r sample_json; do
             local sample_id
             local sample_body
             
             sample_id=$(printf '%s\n' "$sample_json" | jq -r '.id // "N/A"' 2>/dev/null)
             sample_body=$(printf '%s\n' "$sample_json" | jq -r '.body // ""' 2>/dev/null)
             
             # Truncate body to 80 chars and escape newlines
             sample_body=$(printf '%s\n' "$sample_body" | tr '\n' ' ' | cut -c 1-80)
             printf "%b[DEBUG]%b  id=%s created_utc=%s body=%s\n" "$BLUE" "$NC" "$sample_id" "$(printf '%s\n' "$sample_json" | jq -r '.created_utc')" "$sample_body" >&2
         done
         
         # Overall progress
         printf "%b[PROGRESS]%b Total comments from API: %d, Lines written: %d%b\n" "$BLUE" "$NC" "$total_comments" "$total_lines_written" "$NC" >&2
         
         # LIMIT CHECK: Stop if we've reached the requested number of comments.
         if [ -n "$LIMIT" ] && [ "$total_comments" -ge "$LIMIT" ]; then
             printf "%b[INFO]%b Reached limit of %d comments\n" "$BLUE" "$NC" "$LIMIT" >&2
             break
         fi
         
         # PAGINATION TERMINATION: Stop if we get zero results (true end of history).
         # Note: The API may return fewer results than requested due to internal rate limits.
         # We only stop when we get 0 results, not when we get less than requested.
         if [ "$batch_count" -eq 0 ]; then
             printf "%b[INFO]%b No results in this batch; end of user comment history reached\n" "$BLUE" "$NC" >&2
             break
         fi
         
         # PAGINATION: Update 'before' to the minimum created_utc from this batch for next iteration.
         # On next iteration, we'll use before=<this_value - 1> to exclude the boundary and continue.
         before="$last_min_created_utc"
         if [ -z "$before" ]; then
             printf "%b[WARN]%b Could not extract minimum timestamp for pagination\n" "$YELLOW" "$NC" >&2
             break
         fi
     done
     
     end_time=$(date +%s)
     elapsed=$((end_time - start_time))
     
     # Final summary
     local summary_msg="[DONE] total_from_api=$total_comments, total_lines_written=$total_lines_written, file=$OUTPUT_FILE, sec=$elapsed"
     if [ $total_lines_written -gt 1 ]; then  # +1 for header
         printf "%b%s%b\n" "$GREEN" "$summary_msg" "$NC" >&2
     else
         printf "%b%s (no matching comments found)%b\n" "$YELLOW" "$summary_msg" "$NC" >&2
     fi
}

main "$@"
