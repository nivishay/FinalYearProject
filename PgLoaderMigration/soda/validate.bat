@echo off
echo 🔍 Running Soda scan for MySQL...
soda scan -d mysql_sakila_main scan.yml --configuration configuration.yml

echo.

echo 🔍 Running Soda scan for PostgreSQL...
soda scan -d postgres_sakila scan.yml --configuration configuration.yml

echo.
echo ✅ All scans complete.
pause
