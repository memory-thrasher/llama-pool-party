#!/usr/bin/env python3
import argparse
import subprocess
import sys

def run_psql(dsn, query):
    """Executes a psql command and returns the output as a list of strings (one per line)."""
    cmd = ["psql", "-d", dsn, "-tA", "-F,", "-c", query]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip().split('\n') if result.stdout.strip() else []
    except subprocess.CalledProcessError as e:
        print(f"Error executing query: {e.stderr.strip()}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Read-only pool state CLI")
    parser.add_argument("--dsn", required=True, help="PostgreSQL DSN (e.g., 'user=sid dbname=NAME')")
    parser.add_argument("--last", type=int, default=10, help="Number of recent records to show (default 10)")
    args = parser.parse_args()

    dsn = args.dsn
    n = args.last

    print("=== Routers ===")
    # id (short), name, software, last_seen age; flag CONNECTED (last_seen within 90s) vs STALE
    router_query = """
    SELECT 
        left(id::text, 8), 
        router_name, 
        software, 
        now() - last_seen_at, 
        CASE WHEN last_seen_at > now() - interval '90 seconds' THEN 'CONNECTED' ELSE 'STALE' END
    FROM routers 
    WHERE deleted_at IS NULL
    ORDER BY last_seen_at DESC;
    """
    routers = run_psql(dsn, router_query)
    if not routers:
        print("No routers found.")
    else:
        print(f"{'ID':<10} {'NAME':<20} {'SOFTWARE':<15} {'LAST_SEEN':<25} {'STATUS':<10}")
        for r in routers:
            parts = r.split(',')
            if len(parts) == 5:
                print(f"{parts[0]:<10} {parts[1]:<20} {parts[2]:<15} {parts[3]:<25} {parts[4]:<10}")

    print("\n=== Models ===")
    # per router: model_id, tags, n_ctx, busy/n_slots, status
    models_query = """
    SELECT 
        r.router_name, 
        rm.model_id, 
        rm.tags, 
        rm.n_ctx, 
        rm.busy_slots || '/' || rm.n_slots, 
        rm.status
    FROM router_models rm
    JOIN routers r ON rm.router_id = r.id
    WHERE r.deleted_at IS NULL
    ORDER BY r.router_name, rm.model_id;
    """
    models = run_psql(dsn, models_query)
    if not models:
        print("No models found.")
    else:
        print(f"{'ROUTER':<20} {'MODEL_ID':<30} {'TAGS':<20} {'N_CTX':<10} {'BUSY/SLOTS':<10} {'STATUS':<10}")
        for m in models:
            parts = m.split(',')
            if len(parts) == 6:
                print(f"{parts[0]:<20} {parts[1]:<30} {parts[2]:<20} {parts[3]:<10} {parts[4]:<10} {parts[5]:<10}")

    print("\n=== Classes ===")
    # per class: member_count (all rows), advertised_ctx (D-C3), size bucket bounds
    classes_query = """
    SELECT 
        c.id, 
        COUNT(cm.router_id) as member_count, 
        c.advertised_ctx, 
        c.min_params, 
        c.max_params
    FROM classes c
    LEFT JOIN class_members cm ON c.id = cm.class_id
    GROUP BY c.id, c.advertised_ctx, c.min_params, c.max_params
    ORDER BY c.id;
    """
    classes = run_psql(dsn, classes_query)
    if not classes:
        print("No classes found.")
    else:
        print(f"{'CLASS_ID':<20} {'MEMBERS':<10} {'ADV_CTX':<10} {'MIN_PARAMS':<15} {'MAX_PARAMS':<15}")
        for cl in classes:
            parts = cl.split(',')
            if len(parts) == 5:
                print(f"{parts[0]:<20} {parts[1]:<10} {parts[2]:<10} {parts[3]:<15} {parts[4]:<15}")

    print("\n=== Queue ===")
    # counts by status (pending/offered/running/done/failed/evicted/expired)
    queue_counts_query = "SELECT status, COUNT(*) FROM queue_jobs GROUP BY status;"
    queue_counts = run_psql(dsn, queue_counts_query)
    if not queue_counts:
        print("Queue is empty.")
    else:
        print("Counts by status:")
        for qc in queue_counts:
            parts = qc.split(',')
            if len(parts) == 2:
                print(f"  {parts[0]:<10}: {parts[1]}")

    # the N oldest pending jobs (id, tag, est_ctx, age - NO content)
    # using class_id as tag
    queue_oldest_query = f"""
    SELECT id, class_id, est_ctx_needed, now() - created_at
    FROM queue_jobs
    WHERE status = 'pending'
    ORDER BY created_at ASC
    LIMIT {n};
    """
    queue_oldest = run_psql(dsn, queue_oldest_query)
    if queue_oldest:
        print(f"\nOldest {len(queue_oldest)} pending jobs:")
        print(f"{'ID':<38} {'TAG (CLASS)':<20} {'EST_CTX':<10} {'AGE':<25}")
        for qo in queue_oldest:
            parts = qo.split(',')
            if len(parts) == 4:
                print(f"{parts[0]:<38} {parts[1]:<20} {parts[2]:<10} {parts[3]:<25}")

    print("\n=== Records ===")
    # last N rows: ok, error_code, model_id, prompt/response tokens, t_queued/t_service ms, router - NEVER any prompt/response text
    records_query = f"""
    SELECT ok, error_code, model_id, prompt_tokens, response_tokens, t_queued_ms, t_service_ms, router_id
    FROM records
    ORDER BY created_at DESC
    LIMIT {n};
    """
    records = run_psql(dsn, records_query)
    if not records:
        print("No records found.")
    else:
        print(f"{'OK':<6} {'ERR':<15} {'MODEL_ID':<30} {'P_TOK':<8} {'R_TOK':<8} {'TQ_MS':<8} {'TS_MS':<8} {'ROUTER_ID':<10}")
        for rec in records:
            parts = rec.split(',')
            if len(parts) == 8:
                print(f"{parts[0]:<6} {parts[1]:<15} {parts[2]:<30} {parts[3]:<8} {parts[4]:<8} {parts[5]:<8} {parts[6]:<8} {parts[7][:10]:<10}")

    print("\n=== Users/Tokens ===")
    users_tokens_query = """
    SELECT (SELECT COUNT(*) FROM users) as user_count, (SELECT COUNT(*) FROM user_tokens) as token_count;
    """
    users_tokens = run_psql(dsn, users_tokens_query)
    if not users_tokens:
        print("No user/token data found.")
    else:
        for ut in users_tokens:
            parts = ut.split(',')
            if len(parts) == 2:
                print(f"Users: {parts[0]}, Tokens: {parts[1]}")

if __name__ == "__main__":
    main()
