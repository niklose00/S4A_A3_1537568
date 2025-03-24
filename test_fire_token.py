from cli.fire_token import publish_token

import time

print("🚀 Starte erweiterten Test für publish_token()...")

# ✅ Testfall 1: Normale Nachricht an existierendes Topic senden
print("\n🧪 Test 1: Token für R1 auf Track 1 mit 3 Runden")
publish_token(track_id="1", racer_id="R1", laps=3)


