# scripts/load_test.py
import argparse, time, random, uuid, json
import urllib.request

def post(url, payload):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=2) as r:
        return r.read().decode("utf-8")

def now_ms():
    return int(time.time() * 1000)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="http://localhost:8081")
    ap.add_argument("--qps", type=float, default=20)
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--sats", type=int, default=5)
    args = ap.parse_args()

    url = args.host.rstrip("/") + "/telemetry"
    sat_ids = [f"SAT-{i:03d}" for i in range(1, args.sats + 1)]

    start = time.time()
    sent = 0
    period = 1.0 / args.qps

    while time.time() - start < args.seconds:
        t = time.time() - start
        sat = random.choice(sat_ids)

        latency = random.uniform(20, 80)
        total = random.randint(80, 200)
        dropped = random.randint(0, max(1, total // 200))
        lq = random.uniform(0.85, 0.99)

        if sat == "SAT-001" and int(t) % 30 < 5:
            latency = random.uniform(300, 800)

        if sat == "SAT-002" and int(t) % 45 < 5:
            dropped = random.randint(total // 5, total // 2)

        if sat == "SAT-003" and int(t) % 60 < 5:
            lq = random.uniform(0.2, 0.6)

        payload = {
            "event_id": str(uuid.uuid4()),
            "sat_id": sat,
            "ts_ms": now_ms(),
            "latency_ms": latency,
            "dropped_packets": dropped,
            "sent_packets": total,
            "link_quality": lq
        }

        try:
            post(url, payload)
            sent += 1
        except Exception:
            pass

        time.sleep(period)

    print(f"sent {sent} events in {args.seconds}s (~{sent/args.seconds:.1f} eps)")

if __name__ == "__main__":
    main()