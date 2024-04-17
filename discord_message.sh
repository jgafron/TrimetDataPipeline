WEBHOOK_URL="https://discord.com/api/webhooks/1229176295317835828/pVaHtejPoH0XrDXDYX8Uosu5Lhh-AGpvOxRIc82VSk7BnH-oWmKkJpfSu2Br-Ptxhw0_"

MESSAGE="CRON just ran data.py!"

curl -X POST -H "Content-Type: application/json" -d "{\"content\":\"$MESSAGE\"}" $WEBHOOK_URL
