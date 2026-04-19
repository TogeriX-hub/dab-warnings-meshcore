# asa_watch.py – WarnBridge ASA Monitor
# Zeigt Heartbeat (ews_ensemble) und echte Alerts (active)
import requests, time, datetime, os

WELLE = "http://localhost:7979"

last_active = False
last_ews = False

print("=" * 50)
print("  WarnBridge ASA Watcher – 5C DLF Bundesmux")
print("  Warte auf Heartbeat (ews_ensemble=true)...")
print("  Test-Alert kommt alle 5 Minuten fuer 30s")
print("=" * 50)

while True:
    try:
        mux = requests.get(f"{WELLE}/mux.json", timeout=2).json()
        asa = mux.get("asa", {})

        ews    = asa.get("ews_ensemble", False)
        active = asa.get("active", False)
        is_test = asa.get("is_test", False)
        status = asa.get("status", "?")
        level  = asa.get("level", 0)
        iid    = asa.get("iid", 0)
        ts     = datetime.datetime.now().strftime('%H:%M:%S')

        # Heartbeat erstmals gesehen
        if ews and not last_ews:
            print(f"\n✅ [{ts}] HEARTBEAT – Ensemble ist EWS-faehig (FIG 0/15 empfangen)")
            os.system("say 'Heartbeat empfangen. Ensemble ist E W S faehig.'")

        # Alert startet
        if active and not last_active:
            marker = "[TEST]" if is_test else "[ECHT]"
            print(f"\n🚨 [{ts}] ALERT {marker} aktiv!")
            print(f"   status={status}  level={level}  iid={iid}")
            if is_test:
                os.system(f"say 'Test Alert empfangen. Level {level}.'")
            else:
                os.system("say 'ACHTUNG! Echter A S A Alert empfangen!'")

        # Alert endet
        elif not active and last_active:
            print(f"   [{ts}] Alert beendet")
            os.system("say 'Alert beendet'")

        # Status-Zeile alle 30s wenn kein Alert aktiv
        elif not active and int(time.time()) % 30 == 0:
            ews_str = "EWS-faehig" if ews else "warte auf Heartbeat..."
            print(f"  [{ts}] {ews_str}  active={active}      ", end="\r", flush=True)

        last_active = active
        last_ews    = ews

    except Exception:
        print(".", end="", flush=True)

    time.sleep(1)
