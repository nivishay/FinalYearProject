import subprocess
from pathlib import Path
import time

base_dir = Path(__file__).resolve().parent
scripts = [
    "convert_local.py",
    "add_foreign_keys_to_db.py",
    "test_match.py"
]

print("\n🚀 Starting full database migration + verification...\n")

for script in scripts:
    script_path = base_dir / script
    print(f"🚀 Running {script_path.name}...")
    try:
        result = subprocess.run(
            ["python", str(script_path)],
            capture_output=True,
            text=True,
            encoding='utf-8'  # ✅ Important for emojis and non-ASCII chars
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"⚠️ Error while running {script_path.name}:\n{e.stderr}")
    time.sleep(0.5)

print("✅ All steps completed.")
